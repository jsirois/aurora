/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.build;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.lang.model.element.Modifier;

import com.facebook.swift.parser.model.TypeAnnotation;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;

import org.gradle.api.logging.Logger;

class TypeAnnotationVisitor extends BaseVisitor<TypeAnnotation> {
  private static final ClassName ANNOTATION_CLASS =
      ClassName.get(AURORA_THRIFT_PACKAGE_NAME, "Annotation");

  private static final ClassName PARAMETER_CLASS =
      ClassName.get(AURORA_THRIFT_PACKAGE_NAME, "Annotation", "Parameter");

  static AnnotationSpec createAnnotation(List<TypeAnnotation> typeAnnotations) {
    AnnotationSpec.Builder annotationBuilder = AnnotationSpec.builder(ANNOTATION_CLASS);
    for (TypeAnnotation typeAnnotation : typeAnnotations) {
      annotationBuilder.addMember(
          "value",
          "$L",
          AnnotationSpec.builder(PARAMETER_CLASS)
              .addMember("name", "$S", typeAnnotation.getName())
              .addMember("value", "$S", typeAnnotation.getValue())
              .build());
    }
    return annotationBuilder.build();
  }

  TypeAnnotationVisitor(Logger logger, File outdir, SymbolTable symbolTable, String packageName) {
    super(logger, outdir, symbolTable, packageName);
  }

  @Override
  public void finish(ImmutableMap<String, AbstractStructRenderer> structRenderers)
      throws IOException {

    ImmutableList<AnnotationSpec> metaAnnotations =
        ImmutableList.of(
            AnnotationSpec.builder(org.immutables.value.Value.Immutable.class).build(),
            AnnotationSpec.builder(java.lang.annotation.Retention.class)
                .addMember("value", "$T.$L", java.lang.annotation.RetentionPolicy.class,
                    java.lang.annotation.RetentionPolicy.RUNTIME)
                .build());

    writeType(
        ANNOTATION_CLASS.packageName(),
        TypeSpec.annotationBuilder(ANNOTATION_CLASS.simpleName())
            .addAnnotations(metaAnnotations)
            .addModifiers(Modifier.PUBLIC)
            .addType(
                TypeSpec.annotationBuilder(PARAMETER_CLASS.simpleName())
                    .addAnnotations(metaAnnotations)
                    .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                    .addMethod(
                        MethodSpec.methodBuilder("name")
                            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                            .returns(String.class)
                            .build())
                    .addMethod(
                        MethodSpec.methodBuilder("value")
                            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                            .returns(String.class)
                            .build())
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("value")
                    .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                    .returns(ArrayTypeName.of(PARAMETER_CLASS))
                    .build()));
  }
}
