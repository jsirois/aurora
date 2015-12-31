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
import java.lang.reflect.Type;

import javax.annotation.concurrent.NotThreadSafe;
import javax.lang.model.element.Modifier;

import com.google.common.collect.ImmutableSet;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeVariableName;

import org.apache.thrift.TFieldIdEnum;
import org.gradle.api.logging.Logger;

@NotThreadSafe
class ThriftEntityInterfaceFactory extends BaseEmitter {
  private EntityInterface entityInterface;

  public ThriftEntityInterfaceFactory(Logger logger, File outdir) {
    super(logger, outdir);
  }

  public EntityInterface getEntityInterface() throws IOException {
    if (entityInterface == null) {
      entityInterface = createEntityInterface();
    }
    return entityInterface;
  }

  static class EntityInterface {
    final ClassName typeName;
    final ClassName structTypeName;
    final ClassName unionTypeName;
    final ClassName fieldsTypeName;
    final ClassName noThriftFieldsTypeName;
    final ClassName builderTypeName;

    public EntityInterface(
        ClassName typeName,
        ClassName structTypeName,
        ClassName unionTypeName,
        ClassName fieldsTypeName,
        ClassName noThriftFieldsTypeName,
        ClassName builderTypeName) {

      this.typeName = typeName;
      this.structTypeName = structTypeName;
      this.unionTypeName = unionTypeName;
      this.fieldsTypeName = fieldsTypeName;
      this.noThriftFieldsTypeName = noThriftFieldsTypeName;
      this.builderTypeName = builderTypeName;
    }
  }

  private EntityInterface createEntityInterface() throws IOException {
    String thriftEntitySimpleName = "ThriftEntity";
    String thriftStructSimpleName = "ThriftStruct";
    String thriftUnionSimpleName = "ThriftUnion";
    String builderSimpleName = "Builder";
    String thriftFieldsSimpleName = "ThriftFields";
    String noFieldsSimpleName = "NoFields";

    ClassName thriftEntityClassName =
        ClassName.get(
            AURORA_THRIFT_PACKAGE_NAME,
            thriftEntitySimpleName);

    ClassName thriftStructClassName =
        ClassName.get(
            AURORA_THRIFT_PACKAGE_NAME,
            thriftEntitySimpleName,
            thriftStructSimpleName);

    ClassName thriftUnionClassName =
        ClassName.get(
            AURORA_THRIFT_PACKAGE_NAME,
            thriftEntitySimpleName,
            thriftUnionSimpleName);

    ClassName builderClassName =
        ClassName.get(
            AURORA_THRIFT_PACKAGE_NAME,
            thriftEntitySimpleName,
            thriftStructSimpleName,
            builderSimpleName);

    ClassName thriftFieldsClassName =
        ClassName.get(
            AURORA_THRIFT_PACKAGE_NAME,
            thriftEntitySimpleName,
            thriftFieldsSimpleName);

    ClassName noFieldsClassName =
        ClassName.get(
            AURORA_THRIFT_PACKAGE_NAME,
            thriftEntitySimpleName,
            thriftFieldsSimpleName,
            noFieldsSimpleName);

    TypeSpec thriftFields =
        TypeSpec.interfaceBuilder(thriftFieldsSimpleName)
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .addSuperinterface(TFieldIdEnum.class)
            .addType(
                TypeSpec.classBuilder(noFieldsSimpleName)
                    .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                    .addSuperinterface(thriftFieldsClassName)
                    .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                    .addMethod(
                        MethodSpec.constructorBuilder()
                            .addModifiers(Modifier.PRIVATE)
                            .addCode("// NoFields can never be extended so no fields can \n")
                            .addCode("// ever be added.\n")
                            .build())
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("getFieldType")
                    .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                    .returns(Type.class)
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("getFieldClass")
                    .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                    .returns(Class.class)
                    .build())
            .build();

    TypeVariableName fieldsType = TypeVariableName.get("T", thriftFieldsClassName);

    TypeVariableName thriftFieldsTypeVariable = TypeVariableName.get("F", thriftFieldsClassName);
    TypeVariableName thriftEntityTypeVariable =
        TypeVariableName.get(
            "S",
            ParameterizedTypeName.get(thriftEntityClassName, thriftFieldsTypeVariable));
    TypeVariableName thriftStructTypeVariable =
        TypeVariableName.get(
            "S",
            ParameterizedTypeName.get(thriftStructClassName, thriftFieldsTypeVariable));
    ParameterizedTypeName builderReturnType =
        ParameterizedTypeName.get(
            builderClassName,
            thriftFieldsTypeVariable,
            thriftStructTypeVariable);

    TypeSpec builderInterface =
        TypeSpec.interfaceBuilder(builderSimpleName)
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .addTypeVariable(thriftFieldsTypeVariable)
            .addTypeVariable(thriftStructTypeVariable)
            .addMethod(
                MethodSpec.methodBuilder("set")
                    .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                    .addParameter(thriftFieldsTypeVariable, "field")
                    .addParameter(Object.class, "value")
                    .returns(builderReturnType)
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("build")
                    .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                    .returns(thriftStructTypeVariable)
                    .build())
            .build();

    ParameterizedTypeName structTypeParameterType =
        ParameterizedTypeName.get(
            ClassName.get(Class.class),
            thriftStructTypeVariable);

    ParameterizedTypeName fieldsReturnType =
        ParameterizedTypeName.get(
            ClassName.get(ImmutableSet.class),
            thriftFieldsTypeVariable);

    TypeSpec.Builder structInterfaceBuilder =
        TypeSpec.interfaceBuilder(thriftStructSimpleName)
            .addTypeVariable(fieldsType)
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .addSuperinterface(ParameterizedTypeName.get(thriftEntityClassName, fieldsType))
            .addType(builderInterface)
            .addMethod(
                MethodSpec.methodBuilder("builder")
                    .addTypeVariable(thriftFieldsTypeVariable)
                    .addTypeVariable(thriftStructTypeVariable)
                    .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                    .addParameter(structTypeParameterType, "type")
                    .returns(builderReturnType)
                    .beginControlFlow("try")
                    .addStatement(
                        "return ($T) type.getMethod($S).invoke(null)",
                        builderReturnType,
                        "builder")
                    .nextControlFlow("catch ($T e)", ReflectiveOperationException.class)
                    .addStatement("throw new $T(e)", IllegalStateException.class)
                    .endControlFlow()
                    .build());

    TypeVariableName thriftUnionTypeVariable =
        TypeVariableName.get(
            "U",
            ParameterizedTypeName.get(thriftUnionClassName, thriftFieldsTypeVariable));

    ParameterizedTypeName unionTypeParameterType =
        ParameterizedTypeName.get(
            ClassName.get(Class.class),
            thriftUnionTypeVariable);

    TypeSpec.Builder unionInterfaceBuilder =
        TypeSpec.interfaceBuilder(thriftUnionSimpleName)
            .addTypeVariable(fieldsType)
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .addSuperinterface(ParameterizedTypeName.get(thriftEntityClassName, fieldsType))
            .addMethod(
                MethodSpec.methodBuilder("create")
                    .addTypeVariable(thriftFieldsTypeVariable)
                    .addTypeVariable(thriftUnionTypeVariable)
                    .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                    .addParameter(unionTypeParameterType, "type")
                    .addParameter(thriftFieldsTypeVariable, "field")
                    .addParameter(Object.class, "value")
                    .returns(thriftUnionTypeVariable)
                    .beginControlFlow("try")
                    .addStatement(
                        "return type.getConstructor(field.getFieldClass()).newInstance(value)")
                    .nextControlFlow("catch ($T e)", ReflectiveOperationException.class)
                    .addStatement("throw new $T(e)", IllegalStateException.class)
                    .endControlFlow()
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("getSetField")
                    .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                    .returns(fieldsType)
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("getFieldValue")
                    .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                    .returns(Object.class)
                    .build());

    TypeSpec.Builder entityInterfaceBuilder =
        TypeSpec.interfaceBuilder(thriftEntitySimpleName)
            .addTypeVariable(fieldsType)
            .addModifiers(Modifier.PUBLIC)
            .addType(thriftFields)
            .addType(structInterfaceBuilder.build())
            .addType(unionInterfaceBuilder.build())
            .addMethod(
                MethodSpec.methodBuilder("fields")
                    .addTypeVariable(thriftFieldsTypeVariable)
                    .addTypeVariable(thriftEntityTypeVariable)
                    .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                    .addParameter(structTypeParameterType, "type")
                    .returns(fieldsReturnType)
                    .beginControlFlow("try")
                    .addStatement(
                        "return ($T) type.getMethod($S).invoke(null)",
                        fieldsReturnType,
                        "fields")
                    .nextControlFlow("catch ($T e)", ReflectiveOperationException.class)
                    .addStatement("throw new $T(e)", IllegalStateException.class)
                    .endControlFlow()
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("isSet")
                    .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                    .addParameter(fieldsType, "field")
                    .returns(boolean.class)
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("getFieldValue")
                    .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                    .addParameter(fieldsType, "field")
                    .returns(TypeName.OBJECT)
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("getFields")
                    .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                    .returns(
                        ParameterizedTypeName.get(ClassName.get(ImmutableSet.class), fieldsType))
                    .build());

    writeType(AURORA_THRIFT_PACKAGE_NAME, entityInterfaceBuilder);

    return new EntityInterface(
        thriftEntityClassName,
        thriftStructClassName,
        thriftUnionClassName,
        thriftFieldsClassName,
        noFieldsClassName,
        builderClassName);
  }
}
