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
package org.apache.aurora.build.thrift;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.lang.model.element.Modifier;

import com.facebook.swift.parser.model.AbstractStruct;
import com.facebook.swift.parser.model.ContainerType;
import com.facebook.swift.parser.model.IdentifierType;
import com.facebook.swift.parser.model.ListType;
import com.facebook.swift.parser.model.MapType;
import com.facebook.swift.parser.model.SetType;
import com.facebook.swift.parser.model.Struct;
import com.facebook.swift.parser.model.ThriftField;
import com.facebook.swift.parser.model.ThriftType;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import org.slf4j.Logger;

@NotThreadSafe
class StructVisitor extends BaseVisitor<Struct> {
  private final ImmutableList.Builder<Struct> structs = ImmutableList.builder();
  private final ThriftEntityInterfaceFactory thriftEntityInterfaceFactory;

  StructVisitor(
      ThriftEntityInterfaceFactory thriftEntityInterfaceFactory,
      Logger logger,
      File outdir,
      SymbolTable symbolTable,
      String packageName) {

    super(logger, outdir, symbolTable, packageName);
    this.thriftEntityInterfaceFactory = thriftEntityInterfaceFactory;
  }

  @Override
  public void visit(Struct struct) throws IOException {
    structs.add(struct);
  }

  @Override
  public void finish(ImmutableMap<String, AbstractStructRenderer> structRenderers)
      throws IOException {

    for (Struct struct : structs.build()) {
      writeStruct(structRenderers, struct);
    }
  }

  private void writeStruct(
      ImmutableMap<String, AbstractStructRenderer> structRenderers,
      Struct struct)
      throws IOException {

    TypeSpec.Builder typeBuilder =
        TypeSpec.classBuilder(struct.getName())
            .addAnnotation(
                AnnotationSpec.builder(com.facebook.swift.codec.ThriftStruct.class)
                    .addMember("value", "$S", struct.getName())
                    .addMember("builder", "$L.Builder.class", struct.getName())
                    .build())
            .addAnnotation(com.google.auto.value.AutoValue.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT);

    // TODO(John Sirois): XXX Tame this beast!
    ThriftEntityInterfaceFactory.EntityInterface entityInterface =
        thriftEntityInterfaceFactory.getEntityInterface();
    Optional<ClassName> fieldsEnumClassName =
        maybeAddFieldsEnum(typeBuilder, struct, entityInterface.fieldsTypeName);

    ClassName localFieldsTypeName =
        fieldsEnumClassName.or(entityInterface.noThriftFieldsTypeName);

    ParameterSpec fieldParam = ParameterSpec.builder(localFieldsTypeName, "field").build();

    typeBuilder.addSuperinterface(
        ParameterizedTypeName.get(entityInterface.structTypeName, localFieldsTypeName));


    TypeSpec.Builder builderBuilder =
        TypeSpec.interfaceBuilder("_Builder")
            .addAnnotation(com.google.auto.value.AutoValue.Builder.class);

    // This public nested Builder class with no-arg constructor is needed by ThriftCodec.
    ClassName builderBuilderName = ClassName.get(getPackageName(), struct.getName(), "_Builder");
    ClassName autoValueBuilderName =
        ClassName.get(getPackageName(), "AutoValue_" + struct.getName(), "Builder");

    ClassName wrapperBuilderName = ClassName.get(getPackageName(), struct.getName(), "Builder");

    TypeSpec.Builder wrapperBuilder =
        TypeSpec.classBuilder("Builder")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .addSuperinterface(
                ParameterizedTypeName.get(
                    entityInterface.builderTypeName,
                    localFieldsTypeName,
                    getClassName(struct.getName())))
            .addField(builderBuilderName, "builder", Modifier.PRIVATE, Modifier.FINAL);

    Optional<MethodSpec.Builder> isSetMethod = Optional.absent();
    Optional<CodeBlock.Builder> isSetCode = Optional.absent();

    Optional<MethodSpec.Builder> getFieldValueMethod = Optional.absent();
    Optional<CodeBlock.Builder> getFieldValueCode = Optional.absent();

    Optional<MethodSpec.Builder> builderSetMethod = Optional.absent();
    Optional<CodeBlock.Builder> builderSetCode = Optional.absent();

    MethodSpec fieldsMethod;
    if (fieldsEnumClassName.isPresent()) {
      fieldsMethod =
          MethodSpec.methodBuilder("fields")
              .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
              .returns(
                  ParameterizedTypeName.get(
                      ClassName.get(ImmutableSet.class),
                      localFieldsTypeName))
              .addStatement(
                  "return $T.copyOf($T.allOf($T.class))",
                  ImmutableSet.class,
                  EnumSet.class,
                  localFieldsTypeName)
              .build();

      isSetMethod =
          Optional.of(
              MethodSpec.methodBuilder("isSet")
                  .addAnnotation(Override.class)
                  .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                  .addParameter(fieldParam)
                  .returns(boolean.class));
      isSetCode =
          Optional.of(
              CodeBlock.builder()
                  .beginControlFlow("switch ($N)", fieldParam));

      getFieldValueMethod =
          Optional.of(
              MethodSpec.methodBuilder("getFieldValue")
                  .addAnnotation(Override.class)
                  .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                  .addParameter(fieldParam)
                  .returns(Object.class));
      getFieldValueCode =
          Optional.of(
              CodeBlock.builder()
                  .beginControlFlow("if (!this.isSet($N))", fieldParam)
                  .addStatement(
                      "throw new $T($T.format($S, $N))",
                      IllegalArgumentException.class,
                      String.class,
                      "%s is not set.",
                      fieldParam)
                  .endControlFlow()
                  .beginControlFlow("switch ($N)", fieldParam));

      builderSetMethod =
          Optional.of(
              MethodSpec.methodBuilder("set")
                  .addAnnotation(Override.class)
                  .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                  .addParameter(localFieldsTypeName, "field")
                  .addParameter(
                      ParameterSpec.builder(Object.class, "value")
                          .addAnnotation(Nullable.class)
                          .build())
                  .returns(wrapperBuilderName));
      builderSetCode =
          Optional.of(
              CodeBlock.builder()
                  .beginControlFlow("switch ($N)", fieldParam));
    } else {
      fieldsMethod =
          MethodSpec.methodBuilder("fields")
              .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
              .returns(
                  ParameterizedTypeName.get(
                      ClassName.get(ImmutableSet.class),
                      localFieldsTypeName))
              .addStatement("return $T.of()", ImmutableSet.class)
              .build();

      typeBuilder.addMethod(
          MethodSpec.methodBuilder("isSet")
              .addAnnotation(Override.class)
              .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
              .addParameter(fieldParam)
              .returns(boolean.class)
              .addStatement("throw new $T()", IllegalAccessError.class)
              .build());

      typeBuilder.addMethod(
          MethodSpec.methodBuilder("getFieldValue")
              .addAnnotation(Override.class)
              .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
              .addParameter(fieldParam)
              .returns(Object.class)
              .addStatement("throw new $T()", IllegalAccessError.class)
              .build());

      wrapperBuilder.addMethod(
          MethodSpec.methodBuilder("set")
              .addAnnotation(Override.class)
              .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
              .addParameter(localFieldsTypeName, "field")
              .addParameter(
                  ParameterSpec.builder(Object.class, "value")
                      .addAnnotation(Nullable.class)
                      .build())
              .returns(wrapperBuilderName)
              .addStatement("throw new $T()", IllegalAccessError.class)
              .build());
    }
    typeBuilder.addMethod(fieldsMethod);
    typeBuilder.addMethod(
        MethodSpec.methodBuilder("getFields")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .returns(fieldsMethod.returnType)
            .addStatement("return $N()", fieldsMethod)
            .build());

    wrapperBuilder.addMethod(
        MethodSpec.constructorBuilder()
            .addParameter(builderBuilderName, "builder")
            .addStatement("this.builder = builder")
            .build());

    CodeBlock.Builder wrapperConstructorBuilder =
        CodeBlock.builder()
            .add("$[")
            .add("this(new $T()", autoValueBuilderName);

    // A convenience builder factory method for coding against; the Builder is defined below.
    typeBuilder.addMethod(
        MethodSpec.methodBuilder("builder")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .returns(wrapperBuilderName)
            .addStatement("return new Builder()")
            .build());

    // A convenience builder factory that can be used to produce a modified copy of a struct.
    // NB: AutoValue fills in the implementation.
    MethodSpec toBuilder =
        MethodSpec.methodBuilder("_toBuilder")
            .addModifiers(Modifier.ABSTRACT)
            .returns(builderBuilderName)
            .build();
    typeBuilder.addMethod(toBuilder);

    typeBuilder.addMethod(
        MethodSpec.methodBuilder("toBuilder")
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .returns(wrapperBuilderName)
            .addStatement("return new $T($N())", wrapperBuilderName, toBuilder)
            .build());

    typeBuilder.addMethod(
        MethodSpec.constructorBuilder()
            .addCode("// Package private for access by AutoValue subclass only.\n")
            .build());

    // Setup the psuedo-constructor.
    ClassName structClassName = getClassName(struct.getName());
    ImmutableList.Builder<ParameterSpec> constructorParameters = ImmutableList.builder();
    CodeBlock.Builder constructorCode =
        CodeBlock.builder()
            .add("$[return $T.builder()", structClassName);

    for (ThriftField field : struct.getFields()) {
      ThriftType type = field.getType();
      TypeName rawTypeName = typeName(type);
      boolean isPrimitive = rawTypeName.isPrimitive();
      ThriftField.Requiredness requiredness = field.getRequiredness();

      boolean nullable =
          ((isPrimitive && requiredness == ThriftField.Requiredness.OPTIONAL)
              || (!isPrimitive && requiredness != ThriftField.Requiredness.REQUIRED))
              && !field.getValue().isPresent()
              && !(type instanceof ContainerType);

      Optional<CodeBlock> unsetValue = nullable ? Optional.absent() : renderZero(type);

      TypeName typeName = rawTypeName;
      if (nullable) {
        if (isPrimitive) {
          typeName = typeName.box();
        }
        AnnotationSpec nullableAnnotation = AnnotationSpec.builder(Nullable.class).build();
        typeName = typeName.annotated(nullableAnnotation);
      }

      MethodSpec autoValueAccessor =
          MethodSpec.methodBuilder(field.getName())
              .addAnnotation(renderThriftFieldAnnotation(field))
              .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
              .returns(typeName)
              .build();
      typeBuilder.addMethod(autoValueAccessor);

      MethodSpec.Builder publicAccessor =
          MethodSpec.methodBuilder(getterName(field))
              .addModifiers(Modifier.PUBLIC, Modifier.FINAL);
      if (isPrimitive && nullable) {
        publicAccessor
            .returns(rawTypeName)
            .addStatement("$T value = $N()", autoValueAccessor.returnType, autoValueAccessor)
            .addStatement("return value == null ? $L : value", renderZero(type).get());
      } else {
        publicAccessor.returns(typeName).addStatement("return $N()", autoValueAccessor);
      }
      typeBuilder.addMethod(publicAccessor.build());

      String fieldsValueName = toUpperSnakeCaseName(field);
      if (nullable) {
        MethodSpec isSetFieldMethod =
            MethodSpec.methodBuilder(isSetName(field))
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .returns(TypeName.BOOLEAN)
                .addStatement("return $N() != null", autoValueAccessor)
                .build();
        typeBuilder.addMethod(isSetFieldMethod);

        isSetCode.get().addStatement("case $L: return $N()", fieldsValueName, isSetFieldMethod);
      } else {
        isSetCode.get().addStatement("case $L: return true", fieldsValueName);
      }
      getFieldValueCode.get().addStatement("case $L: return $N()", fieldsValueName, autoValueAccessor);
      builderSetCode.get()
          .add("case $L:\n", fieldsValueName)
          .indent()
          .addStatement(
              "$N(($T) value)",
              setterName(field),
              // TODO(John Sirois): DRY - this is calculated above and re-calculated here.
              nullable && rawTypeName.isPrimitive() ? rawTypeName.box() : rawTypeName)
          .addStatement("break")
          .unindent();

      ParameterSpec parameterSpec = ParameterSpec.builder(typeName, field.getName()).build();

      MethodSpec autoValueBuilderMutator =
          MethodSpec.methodBuilder(field.getName())
              .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
              .addParameter(parameterSpec)
              .returns(builderBuilderName)
              .build();
      builderBuilder.addMethod(autoValueBuilderMutator);

      // TODO(John Sirois): Add collection overloads.
      MethodSpec simpleWither =
          MethodSpec.methodBuilder(witherName(field))
              .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
              .addParameter(rawTypeName, field.getName())
              .returns(structClassName)
              .addStatement("return toBuilder().$N($L).build()", setterName(field), field.getName())
              .build();
      typeBuilder.addMethod(simpleWither);

      typeBuilder.addMethod(
          MethodSpec.methodBuilder(witherName(field))
              .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
              .addParameter(parameterizedTypeName(UnaryOperator.class, type), "mutator")
              .returns(structClassName)
              .addStatement("return $N(mutator.apply($N()))", simpleWither, getterName(field))
              .build());

      ImmutableList.Builder<AnnotationSpec> annotations = ImmutableList.builder();
      if (!(type instanceof ContainerType)) {
        annotations.add(renderThriftFieldAnnotation(field));
      }
      MethodSpec swiftMutator =
          MethodSpec.methodBuilder(field.getName())
              .addAnnotations(annotations.build())
              .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
              .addParameter(parameterSpec)
              .returns(wrapperBuilderName)
              .addStatement("this.builder.$N($N)", autoValueBuilderMutator, parameterSpec)
              .addStatement("return this")
              .build();
      wrapperBuilder.addMethod(swiftMutator);

      String setterName = setterName(field);
      MethodSpec wrapperMethodSpec =
          MethodSpec.methodBuilder(setterName)
              .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
              .addParameter(parameterSpec)
              .returns(wrapperBuilderName)
              .addStatement("return $N($N)", swiftMutator, parameterSpec)
              .build();
      wrapperBuilder.addMethod(wrapperMethodSpec);
      if (rawTypeName.isPrimitive() && !typeName.isPrimitive()) {
        wrapperBuilder.addMethod(
            MethodSpec.methodBuilder(setterName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(parameterSpec.type.unbox(), parameterSpec.name)
                .returns(wrapperBuilderName)
                .addStatement(
                    "return $N($T.valueOf($L))",
                    wrapperMethodSpec,
                    rawTypeName.box(),
                    parameterSpec.name)
                .build());
      }

      // The {List,Set,Map} builder overloads are added specifically for the SwiftCodec, the rest
      // are for convenience.
      if (type instanceof ListType) {
        ThriftType elementType = ((ListType) type).getElementType();
        Iterable<MethodSpec> overloads =
            createCollectionBuilderOverloads(
                field,
                wrapperMethodSpec,
                List.class,
                elementType);
        wrapperBuilder.addMethods(overloads);
      } else if (type instanceof SetType) {
        ThriftType elementType = ((SetType) type).getElementType();
        Iterable<MethodSpec> overloads =
            createCollectionBuilderOverloads(
                field,
                wrapperMethodSpec,
                Set.class,
                elementType);
        wrapperBuilder.addMethods(overloads);
      } else if (type instanceof MapType) {
        MapType mapType = (MapType) type;
        ThriftType keyType = mapType.getKeyType();
        ThriftType valueType = mapType.getValueType();
        ParameterSpec param =
            ParameterSpec.builder(
                parameterizedTypeName(Map.class, keyType, valueType), field.getName())
                .build();
        wrapperBuilder.addMethod(
            createBuilderOverload(
                field, wrapperMethodSpec, param, /* annotate */ true, /* varargs */ false));
      }

      if (field.getValue().isPresent()) {
        CodeBlock defaultValue = renderValue(structRenderers, type, field.getValue().get());
        wrapperConstructorBuilder
            .add("\n.$N(", autoValueBuilderMutator)
            .add(defaultValue)
            .add(")");
      } else if (unsetValue.isPresent()) {
        wrapperConstructorBuilder
            .add("\n.$N(", autoValueBuilderMutator)
            .add(unsetValue.get())
            .add(")");
      }

      // TODO(John Sirois): This signature, skipping OPTIONALs, is tailored to match apache
      // thrift: reconsider convenience factory methods after transition.
      if (requiredness != ThriftField.Requiredness.OPTIONAL) {
        TypeName constructorParamType;
        if (type instanceof ListType) {
          ThriftType elementType = ((ListType) type).getElementType();
          constructorParamType = parameterizedTypeName(List.class, elementType);
        } else if (type instanceof SetType) {
          ThriftType elementType = ((SetType) type).getElementType();
          constructorParamType = parameterizedTypeName(Set.class, elementType);
        } else if (type instanceof MapType) {
          MapType mapType = (MapType) type;
          ThriftType keyType = mapType.getKeyType();
          ThriftType valueType = mapType.getValueType();
          constructorParamType = parameterizedTypeName(Map.class, keyType, valueType);
        } else {
          constructorParamType = rawTypeName;
        }
        ParameterSpec param =
            ParameterSpec.builder(constructorParamType, field.getName()).build();
        constructorParameters.add(param);
        constructorCode.add("\n.$N($N)", wrapperMethodSpec, param);
      }
    }

    if (isSetMethod.isPresent()) {
      typeBuilder.addMethod(
          isSetMethod.get()
              .addCode(
                  isSetCode.get()
                      .addStatement(
                          "default: throw new $T($T.format($S, $N))",
                          IllegalArgumentException.class,
                          String.class,
                          "%s is not a known field",
                          fieldParam)
                      .endControlFlow()
                      .build())
              .build());
    }

    if (getFieldValueMethod.isPresent()) {
      typeBuilder.addMethod(
          getFieldValueMethod.get()
              .addCode(
                  getFieldValueCode.get()
                      .addStatement(
                          "default: throw new $T($T.format($S, $N))",
                          IllegalArgumentException.class,
                          String.class,
                          "%s is not a known field",
                          fieldParam)
                      .endControlFlow()
                      .build())
              .build());
    }

    constructorCode.add("\n.build();\n$]");
    typeBuilder.addMethod(
        MethodSpec.methodBuilder("create")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .returns(structClassName)
            .addParameters(constructorParameters.build())
            .addCode(constructorCode.build())
            .build());

    builderBuilder.addMethod(
        MethodSpec.methodBuilder("build")
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(ClassName.get(getPackageName(), struct.getName()))
            .build());
    typeBuilder.addType(builderBuilder.build());

    wrapperBuilder.addMethod(
        MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PUBLIC)
            .addCode(
                wrapperConstructorBuilder
                    .add(");\n$]")
                    .build())
            .build());

    if (builderSetCode.isPresent()) {
      wrapperBuilder.addMethod(
          builderSetMethod.get()
              .addCode(
                  builderSetCode.get()
                      .addStatement(
                          "default: throw new $T($T.format($S, $N))",
                          IllegalArgumentException.class,
                          String.class,
                          "%s is not a known field",
                          fieldParam)
                      .endControlFlow()
                      .addStatement("return this")
                      .build())
              .build());
    }

    wrapperBuilder.addMethod(
        MethodSpec.methodBuilder("build")
            .addAnnotation(com.facebook.swift.codec.ThriftConstructor.class)
            .returns(ClassName.get(getPackageName(), struct.getName()))
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .addStatement("return this.builder.build()")
            .build());
    typeBuilder.addType(wrapperBuilder.build());

    TypeSpec typeSpec = writeType(typeBuilder);

    Optional<PeerInfo> maybePeerInfo = PeerInfo.from(getPackageName(), struct);
    if (maybePeerInfo.isPresent()) {
      PeerInfo peerInfo = maybePeerInfo.get();
      if (peerInfo.render) {
        MutablePeer mutablePeer =
            new MutablePeer(getLogger(), getOutdir(), getSymbolTable(), getPackageName());
        TypeSpec peerType = mutablePeer.render(struct, typeSpec, peerInfo);
        writeType(peerInfo.packageName, peerType.toBuilder());
      }
    }
  }



  private Iterable<MethodSpec> createCollectionBuilderOverloads(
      ThriftField field,
      MethodSpec primaryMethod,
      Class<? extends Collection> containerType,
      ThriftType elementType) {

    ImmutableList.Builder<MethodSpec> overloads = ImmutableList.builder();
    String fieldName = field.getName();

    ParameterSpec setParam =
        ParameterSpec.builder(parameterizedTypeName(containerType, elementType), fieldName)
            .build();
    overloads.add(
        createBuilderOverload(
            field, primaryMethod, setParam, /* annotate */ true, /* varargs */ false));

    ParameterSpec iterableParam =
        ParameterSpec.builder(parameterizedTypeName(Iterable.class, elementType), fieldName)
            .build();
    overloads.add(
        createBuilderOverload(
            field, primaryMethod, iterableParam, /* annotate */ false, /* varargs */ false));

    ParameterSpec varargsParam =
        ParameterSpec.builder(ArrayTypeName.of(typeName(elementType).box()), fieldName)
            .build();
    overloads.add(
        createBuilderOverload(
            field, primaryMethod, varargsParam, /* annotate */ false, /* varargs */ true));

    return overloads.build();
  }

  private MethodSpec createBuilderOverload(
      ThriftField field,
      MethodSpec primaryMethod,
      ParameterSpec parameterSpec,
      boolean annotate,
      boolean varargs) {

    ParameterizedTypeName primaryType = (ParameterizedTypeName) typeName(field.getType());
    ClassName immutableFactoryType = primaryType.rawType;

    MethodSpec.Builder overloadBuilder = MethodSpec.methodBuilder(primaryMethod.name);
    if (annotate) {
      overloadBuilder.addAnnotation(renderThriftFieldAnnotation(field));
    }
    return overloadBuilder
        .addModifiers(primaryMethod.modifiers)
        .addParameter(parameterSpec)
        .varargs(varargs)
        .returns(primaryMethod.returnType)
        .addStatement(
            "return $N($T.copyOf($N))", primaryMethod, immutableFactoryType, parameterSpec)
        .build();
  }
}
