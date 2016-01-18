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
package org.apache.aurora.thrift.build;

import java.io.IOException;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.Objects;

import javax.annotation.concurrent.Immutable;
import javax.lang.model.element.Modifier;

import com.facebook.swift.codec.ThriftUnionId;
import com.facebook.swift.parser.model.ContainerType;
import com.facebook.swift.parser.model.ThriftField;
import com.facebook.swift.parser.model.Union;
import com.google.common.collect.ImmutableSet;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import org.apache.aurora.thrift.ThriftUnion;
import org.slf4j.Logger;

class UnionVisitor extends BaseVisitor<Union> {
  UnionVisitor(
      Logger logger,
      Path outdir,
      SymbolTable symbolTable,
      String packageName) {

    super(logger, outdir, symbolTable, packageName);
  }

  @Override
  public void visit(Union union) throws IOException {
    ClassName localFieldsTypeName = getClassName(union.getName(), "Fields");
    TypeSpec.Builder typeBuilder =
        TypeSpec.classBuilder(union.getName())
            .addAnnotation(
                AnnotationSpec.builder(com.facebook.swift.codec.ThriftUnion.class)
                    .addMember("value", "$S", union.getName())
                    .build())
            .addAnnotation(Immutable.class)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .addSuperinterface(
                ParameterizedTypeName.get(ClassName.get(ThriftUnion.class), localFieldsTypeName));

    if (!union.getAnnotations().isEmpty()) {
      typeBuilder.addAnnotations(BaseVisitor.createAnnotations(union.getAnnotations()));
    }

    FieldSpec valueField =
        FieldSpec.builder(Object.class, "value", Modifier.PRIVATE, Modifier.FINAL).build();
    typeBuilder.addField(valueField);

    FieldSpec idField =
        FieldSpec.builder(short.class, "id", Modifier.PRIVATE, Modifier.FINAL).build();
    typeBuilder.addField(idField);

    MethodSpec fieldsMethod = MethodSpec.methodBuilder("fields")
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
        .returns(
            ParameterizedTypeName.get(ClassName.get(ImmutableSet.class), localFieldsTypeName))
        .addStatement(
            "return $T.copyOf($T.allOf($T.class))",
            ImmutableSet.class,
            EnumSet.class,
            localFieldsTypeName)
        .build();
    typeBuilder.addMethod(fieldsMethod);

    typeBuilder.addMethod(
        MethodSpec.methodBuilder("getFields")
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .returns(fieldsMethod.returnType)
            .addStatement("return $N()", fieldsMethod)
            .build());

    ClassName unionClassName = getClassName(union.getName());
    for (ThriftField field : union.getFields()) {
      TypeName fieldTypeName = typeName(field.getType());
      short id = extractId(field);

      // A convenience factory that eases transition from apache thrift union types.
      typeBuilder.addMethod(
          MethodSpec.methodBuilder(field.getName())
              .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
              .addParameter(fieldTypeName, field.getName())
              .returns(unionClassName)
              .addStatement("return new $T($L)", unionClassName, field.getName())
              .build());

      // The constructor is called by swift codecs when de-serializing unions and it expects the
      // standard java List, Map & Set container types; mutable=true gets us those types.
      TypeName mutableFieldTypeName = typeName(field.getType(), /* mutable */ true);
      CodeBlock.Builder valueAssignment = CodeBlock.builder();
      if (field.getType() instanceof ContainerType) {
        // We have an Immutable{List,Map,Set} that must be populated from a value of the parent
        // java.util mutable interface type passed by the swift codec.
        ParameterizedTypeName immutableType = (ParameterizedTypeName) fieldTypeName;
        valueAssignment.addStatement(
            "this.value = $T.copyOf($L)",
            immutableType.rawType,
            field.getName());
      } else { // We have an immutable value.
        valueAssignment.addStatement(
            "this.value = $T.requireNonNull($L)",
            Objects.class,
            field.getName());
      }
      typeBuilder.addMethod(
          MethodSpec.constructorBuilder()
              .addAnnotation(com.facebook.swift.codec.ThriftConstructor.class)
              .addModifiers(Modifier.PUBLIC)
              .addParameter(
                  ParameterSpec.builder(mutableFieldTypeName, field.getName())
                      .addAnnotation(renderThriftFieldAnnotation(field))
                      .build())
              .addCode(valueAssignment.build())
              .addStatement("this.id = $L", id)
              .build());

      MethodSpec isSetMethod =
          MethodSpec.methodBuilder(isSetName(field))
              .addModifiers(Modifier.PUBLIC)
              .returns(boolean.class)
              .addStatement("return id == $L", id)
              .build();
      typeBuilder.addMethod(isSetMethod);

      typeBuilder.addMethod(
          MethodSpec.methodBuilder(getterName(field))
              .addAnnotation(renderThriftFieldAnnotation(field))
              .addModifiers(Modifier.PUBLIC)
              .returns(fieldTypeName)
              .addCode(
                  CodeBlock.builder()
                      .beginControlFlow("if (!$N())", isSetMethod)
                      .addStatement("throw new $T()", IllegalStateException.class)
                      .endControlFlow()
                      .add("// We checked this cast was valid just above.\n")
                      .add("@$T($S)\n", SuppressWarnings.class, "unchecked")
                      .addStatement("$T typed = ($T) value", fieldTypeName, fieldTypeName)
                      .addStatement("return typed")
                      .build())
              .build());
    }

    MethodSpec getSetIdMethod =
        MethodSpec.methodBuilder("getSetId")
            .addAnnotation(ThriftUnionId.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(short.class)
            .addStatement("return id")
            .build();
    typeBuilder.addMethod(getSetIdMethod);

    ClassName fieldsEnumClass = addFields(typeBuilder, union);
    ParameterSpec fieldParam =
        ParameterSpec.builder(fieldsEnumClass, "field")
            .build();

    MethodSpec getSetFieldMethod =
        MethodSpec.methodBuilder("getSetField")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .returns(fieldsEnumClass)
            .addStatement("return $T.findByThriftId($N())", fieldsEnumClass, getSetIdMethod)
            .build();
    typeBuilder.addMethod(getSetFieldMethod);

    typeBuilder.addMethod(
        MethodSpec.methodBuilder("getFieldValue")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .returns(Object.class)
            .addStatement("return value")
            .build());

    MethodSpec isSetMethod =
        MethodSpec.methodBuilder("isSet")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .addParameter(fieldParam)
            .returns(boolean.class)
            .addStatement("return $N() == $N", getSetFieldMethod, fieldParam)
            .build();
    typeBuilder.addMethod(isSetMethod);

    typeBuilder.addMethod(
        MethodSpec.methodBuilder("getFieldValue")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .addParameter(fieldParam)
            .returns(Object.class)
            .addException(IllegalArgumentException.class)
            .beginControlFlow("if (!$N($N))", isSetMethod, fieldParam)
            .addStatement(
                "throw new $T($T.format($S, $N, $N()))",
                IllegalArgumentException.class,
                String.class,
                "%s is not the set field, %s is.",
                fieldParam,
                getSetFieldMethod)
            .endControlFlow()
            .addStatement("return value")
            .build());

    typeBuilder.addMethod(
        MethodSpec.methodBuilder("equals")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .addParameter(Object.class, "other")
            .returns(boolean.class)
            .beginControlFlow("if (!(other instanceof $T))", unionClassName)
            .addStatement("return false")
            .endControlFlow()
            .addStatement(
                "return $T.equals($N, (($T) other).$N)",
                Objects.class,
                valueField,
                unionClassName,
                valueField)
            .build());

    typeBuilder.addMethod(
        MethodSpec.methodBuilder("hashCode")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .returns(int.class)
            .addStatement("return $T.hash($N, $N)", Objects.class, idField, valueField)
            .build());

    writeType(typeBuilder);
  }
}
