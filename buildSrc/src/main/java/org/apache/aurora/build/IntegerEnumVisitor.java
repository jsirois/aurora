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
import java.util.EnumSet;

import javax.lang.model.element.Modifier;

import com.facebook.swift.parser.model.IntegerEnum;
import com.facebook.swift.parser.model.IntegerEnumField;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import org.gradle.api.logging.Logger;

class IntegerEnumVisitor extends BaseVisitor<IntegerEnum> {
  IntegerEnumVisitor(Logger logger, File outdir, SymbolTable symbolTable, String packageName) {
    super(logger, outdir, symbolTable, packageName);
  }

  @Override
  public void visit(IntegerEnum integerEnum) throws IOException {
    TypeSpec.Builder typeBuilder =
        TypeSpec.enumBuilder(integerEnum.getName())
            .addModifiers(Modifier.PUBLIC)
            .addSuperinterface(org.apache.thrift.TEnum.class);

    ClassName className = getClassName(integerEnum.getName());
    FieldSpec byVal =
        FieldSpec.builder(
            ParameterizedTypeName.get(
                ClassName.get(ImmutableMap.class),
                TypeName.INT.box(),
                className),
            "byVal")
            .addModifiers(Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
            .initializer(
                "$T.uniqueIndex($T.allOf($T.class), $T::getValue)",
                Maps.class,
                EnumSet.class,
                className,
                className)
            .build();
    typeBuilder.addField(byVal);

    typeBuilder.addMethod(
        MethodSpec.methodBuilder("findByValue")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .addParameter(int.class, "val")
            .returns(className)
            .beginControlFlow("if (!$N.containsKey(val))", byVal)
            .addStatement(
                "throw new $T($T.format($S, val))",
                IllegalArgumentException.class,
                String.class,
                "Unknown enum value %d.")
            .endControlFlow()
            .addStatement("return $N.get(val)", byVal)
            .build());

    typeBuilder.addField(int.class, "value", Modifier.PRIVATE, Modifier.FINAL);
    typeBuilder.addMethod(
        MethodSpec.constructorBuilder()
            .addParameter(int.class, "value")
            .addStatement("this.value = value")
            .build());
    typeBuilder.addMethod(
        MethodSpec.methodBuilder("getValue")
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(com.facebook.swift.codec.ThriftEnumValue.class)
            .returns(int.class)
            .addStatement("return value")
            .build());

    for (IntegerEnumField field : integerEnum.getFields()) {
      typeBuilder.addEnumConstant(
          field.getName(),
          TypeSpec.anonymousClassBuilder("$L", field.getValue()).build());
    }

    writeType(typeBuilder);
  }
}
