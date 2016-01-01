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
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import javax.lang.model.element.Modifier;

import com.facebook.swift.parser.model.AbstractStruct;
import com.facebook.swift.parser.model.BaseType;
import com.facebook.swift.parser.model.ConstDouble;
import com.facebook.swift.parser.model.ConstIdentifier;
import com.facebook.swift.parser.model.ConstInteger;
import com.facebook.swift.parser.model.ConstList;
import com.facebook.swift.parser.model.ConstMap;
import com.facebook.swift.parser.model.ConstString;
import com.facebook.swift.parser.model.ConstValue;
import com.facebook.swift.parser.model.IdentifierType;
import com.facebook.swift.parser.model.ListType;
import com.facebook.swift.parser.model.MapType;
import com.facebook.swift.parser.model.SetType;
import com.facebook.swift.parser.model.ThriftField;
import com.facebook.swift.parser.model.ThriftType;
import com.facebook.swift.parser.visitor.Visitable;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import org.slf4j.Logger;

import static java.util.Objects.requireNonNull;

abstract class BaseVisitor<T extends Visitable> extends BaseEmitter implements Visitor<T> {

  protected static short extractId(ThriftField field) {
    Optional<Long> identifier = field.getIdentifier();
    if (!identifier.isPresent()) {
      throw new ParseException("All thrift fields must have an id, given field without id: " + field);
    }
    Long id = identifier.get();
    short value = id.shortValue();
    if (id != value) {
      throw new ParseException("All ids are expected to be shorts, given " + id);
    }
    return value;
  }

  private final SymbolTable symbolTable;
  private final String packageName;

  public BaseVisitor(Logger logger, File outdir, SymbolTable symbolTable, String packageName) {
    super(logger, outdir);
    this.symbolTable = symbolTable;
    this.packageName = requireNonNull(packageName);
  }

  protected final SymbolTable.Symbol lookup(String identifier) {
    return symbolTable.lookup(getPackageName(), identifier);
  }

  protected final SymbolTable.Symbol lookup(IdentifierType identifier) {
    return symbolTable.lookup(getPackageName(), identifier);
  }

  protected final ClassName getClassName(IdentifierType identifierType, String... simpleNames) {
    return getClassName(identifierType.getName(), simpleNames);
  }

  protected final String getPackageName(ConstIdentifier identifierValue) {
    return getClassName(identifierValue.value()).packageName();
  }

  protected final ClassName getClassName(String identifier, String... simpleNames) {
    ClassName className = lookup(identifier).getClassName();
    return ClassName.get(className.packageName(), className.simpleName(), simpleNames);
  }

  protected final String getPackageName() {
    return packageName;
  }

  protected final TypeName typeName(ThriftType thriftType) {
    return typeName(thriftType, /* mutable */ false);
  }

  protected final TypeName typeName(ThriftType thriftType, boolean mutable) {
    if (thriftType instanceof BaseType) {
      BaseType baseType = (BaseType) thriftType;
      switch (baseType.getType()) {
        case BINARY:
          return ClassName.get(ByteBuffer.class);
        case BOOL:
          return TypeName.BOOLEAN;
        case BYTE:
          return TypeName.BYTE;
        case DOUBLE:
          return TypeName.DOUBLE;
        case I16:
          return TypeName.SHORT;
        case I32:
          return TypeName.INT;
        case I64:
          return TypeName.LONG;
        case STRING:
          return ClassName.get(String.class);
      }
    } else if (thriftType instanceof IdentifierType) {
      return getClassName((IdentifierType) thriftType);
    } else if (thriftType instanceof MapType) {
      MapType mapType = (MapType) thriftType;
      ThriftType keyType = mapType.getKeyType();
      ThriftType valueType = mapType.getValueType();
      return parameterizedTypeName(mutable ? Map.class : ImmutableMap.class, keyType, valueType);
    } else if (thriftType instanceof ListType) {
      ThriftType elementType = ((ListType) thriftType).getElementType();
      return parameterizedTypeName(mutable ? List.class : ImmutableList.class, elementType);
    } else if (thriftType instanceof SetType) {
      ThriftType elementType = ((SetType) thriftType).getElementType();
      return parameterizedTypeName(mutable ? Set.class : ImmutableSet.class, elementType);
    }
    throw new UnexpectedTypeException("Unknown thrift type: " + thriftType);
  }

  protected final ParameterizedTypeName parameterizedTypeName(
      Class<?> type,
      ThriftType... parameters) {

    return parameterizedTypeName(type, /* mutable */ false, parameters);
  }

  protected final ParameterizedTypeName parameterizedTypeName(
      Class<?> type,
      boolean mutable,
      ThriftType... parameters) {

    return ParameterizedTypeName.get(ClassName.get(type),
        Stream.of(parameters).map(p -> typeName(p, mutable).box()).toArray(TypeName[]::new));
  }

  protected final CodeBlock renderValue(
      ImmutableMap<String, AbstractStructRenderer> structRenderers,
      ThriftType type,
      ConstValue value) {

    CodeBlock.Builder codeBuilder = CodeBlock.builder();
    if (value instanceof ConstInteger) {
      long val = ((ConstInteger) value).value();
      if (isBoolean(type)) {
        // Thrift uses 0/1 for boolean literals.
        codeBuilder.add("$L", val == 0L);
      } else {
        codeBuilder.add("$L", val);
      }
    } else if (value instanceof ConstDouble) {
      codeBuilder.add("$L", value.value());
    } else if (value instanceof ConstString) {
      codeBuilder.add("\"$L\"", value.value());
    } else if (value instanceof ConstList) {
      renderListValue(structRenderers, type, (ConstList) value, codeBuilder);
    } else if (value instanceof ConstMap) {
      ConstMap map = (ConstMap) value;
      if (type instanceof MapType) {
        renderMapValue(structRenderers, (MapType) type, codeBuilder, map);
      } else if (type instanceof IdentifierType) {
        renderStructValue(structRenderers, (IdentifierType) type, codeBuilder, map);
      } else {
        throw new UnexpectedTypeException(
            String.format(
                "Only maps and structs can be represented as map literals, encountered map " +
                    "literal %s targeted at %s",
                value,
                type));
      }
    } else if (value instanceof ConstIdentifier) {
      renderIdentifierValue(type, (ConstIdentifier) value, codeBuilder);
    } else {
      throw new UnexpectedTypeException("Unknown const value type: " + value);
    }
    return codeBuilder.build();
  }

  protected final CodeBlock renderCode(String literal, Object... args) {
    return CodeBlock.builder().add(literal, args).build();
  }

  /**
   * Renders the zero-value for the given thrift type if there is one different from {@code null}.
   *
   * @param type The type whose zero-value to render.
   * @return An optional code block representing the zero-value.
   */
  protected final Optional<CodeBlock> renderZero(ThriftType type) {
    if (type instanceof BaseType) {
      // NB: $L literal formatting does not work for 0L; so all literals below are rendered
      // directly for consistency.
      switch (((BaseType) type).getType()) {
        case BOOL:
          return Optional.of(renderCode("false"));
        case BYTE:
          return Optional.of(renderCode("0x0"));
        case DOUBLE:
          return Optional.of(renderCode("0.0"));
        case I16:
          return Optional.of(renderCode("(short) 0"));
        case I32:
          return Optional.of(renderCode("0"));
        case I64:
          return Optional.of(renderCode("0L"));
      }
    } else if (type instanceof ListType) {
      return Optional.of(renderCode("$T.of()", ImmutableList.class));
    } else if (type instanceof SetType) {
      return Optional.of(renderCode("$T.of()", ImmutableSet.class));
    } else if (type instanceof MapType) {
      return Optional.of(renderCode("$T.of()", ImmutableMap.class));
    }
    return Optional.absent();
  }

  private void renderListValue(
      ImmutableMap<String, AbstractStructRenderer> structRenderers,
      ThriftType type,
      ConstList value,
      CodeBlock.Builder codeBuilder) {

    Class<?> containerType;
    ThriftType elementType;
    if (type instanceof ListType) {
      containerType = ImmutableList.class;
      elementType = ((ListType) type).getElementType();
    } else if (type instanceof SetType) {
      containerType = ImmutableSet.class;
      elementType = ((SetType) type).getElementType();
    } else {
      throw new UnexpectedTypeException(
          String.format(
              "Only lists and sets can be represented as list literals, encountered list " +
                  "literal %s targeted at %s",
              value,
              type));
    }
    indented(codeBuilder, () -> {
      codeBuilder.add("$T.<$T>builder()", containerType, typeName(elementType).box());
      for (ConstValue elementValue : value.value()) {
        codeBuilder.add("\n.add(");
        codeBuilder.add(renderValue(structRenderers, elementType, elementValue));
        codeBuilder.add(")");
      }
      codeBuilder.add("\n.build()");
    });
  }

  private void renderMapValue(
      ImmutableMap<String, AbstractStructRenderer> structRenderers,
      MapType mapType,
      CodeBlock.Builder codeBuilder,
      ConstMap map) {

    ThriftType keyType = mapType.getKeyType();
    ThriftType valueType = mapType.getValueType();
    indented(codeBuilder, () -> {
      codeBuilder.add("$T.<$T, $T>builder()", ImmutableMap.class, typeName(keyType).box(),
          typeName(valueType).box());
      for (Map.Entry<ConstValue, ConstValue> entry : map.value().entrySet()) {
        codeBuilder.add("\n.put(");
        codeBuilder.add(renderValue(structRenderers, keyType, entry.getKey()));
        codeBuilder.add(", ");
        codeBuilder.add(renderValue(structRenderers, valueType, entry.getValue()));
        codeBuilder.add(")");
      }
      codeBuilder.add("\n.build()");
    });
  }

  private void renderStructValue(
      ImmutableMap<String, AbstractStructRenderer> structRenderers,
      IdentifierType type,
      CodeBlock.Builder codeBuilder,
      ConstMap map) {

    // TODO(John Sirois): XXX structRenderers need to handle type resolution across includes.
    AbstractStructRenderer structRenderer = structRenderers.get(type.getName());
    if (structRenderer == null) {
      throw new ParseException(
          String.format(
              "Cannot create struct literal value using map literal %s, found no struct, " +
                  "union or thrift exception type named '%s'",
              map,
              type.getName()));
    }

    ImmutableMap.Builder<String, ConstValue> parameterMap = ImmutableMap.builder();
    for (Map.Entry<ConstValue, ConstValue> entry : map.value().entrySet()) {
      ConstValue key = entry.getKey();
      if (!(key instanceof ConstString)) {
        throw new UnexpectedTypeException(
            String.format(
                "Cannot create struct literal value using map literal %s, keys must all be " +
                    "strings",
                map));
      }
      parameterMap.put(((ConstString) key).value(), entry.getValue());
    }
    ImmutableMap<String, ConstValue> parameters = parameterMap.build();

    // `renderCode` is almost what we need, but with structRenderers curried.
    LiteralFactory literalFactory = (_1, _2) -> renderValue(structRenderers, _1, _2);

    codeBuilder.add(structRenderer.createLiteral(parameters, literalFactory));
  }

  private void renderIdentifierValue(
      ThriftType type,
      ConstIdentifier value,
      CodeBlock.Builder codeBuilder) {

    String identifier = value.value();
    if (type instanceof IdentifierType) {
      ClassName className = getClassName((IdentifierType) type);
      // Up to 3 components, the rightmost is the name:
      // package local constant: MyEnum.OK
      // via include.thrift: included.MyEnum.OK
      String name = Iterables.getLast(Splitter.on('.').limit(3).splitToList(identifier));
      codeBuilder.add("$T.$L", className, name);
    } else {
      // Or else its a constant value reference:
      // Up to 2 components, the rightmost is the name:
      // package local constant: CONSTANT_VALUE
      // via include.thrift: included.CONSTANT_VALUE
      ClassName className = ClassName.get(getPackageName(value), "Constants");
      String name = Iterables.getLast(Splitter.on('.').limit(2).splitToList(identifier));
      codeBuilder.add("$T.$L", className, name);
    }
  }

  protected final AnnotationSpec renderThriftFieldAnnotation(ThriftField field) {
    return AnnotationSpec.builder(com.facebook.swift.codec.ThriftField.class)
        .addMember("value", "$L", extractId(field))
        .addMember("name", "$S", field.getName())
        .addMember("requiredness", "$T.$L",
            com.facebook.swift.codec.ThriftField.Requiredness.class,
            field.getRequiredness().name())
        .build();
  }

  protected final Optional<ClassName> maybeAddFieldsEnum(
      TypeSpec.Builder typeBuilder,
      AbstractStruct struct,
      TypeName fieldsTypeName) {

    // Enum types must have at least one enum constant, so skip adding the type for empty structs.
    if (struct.getFields().isEmpty()) {
      return Optional.absent();
    }

    ClassName fieldsClassName = getClassName(struct.getName(), "Fields");
    TypeSpec.Builder thriftFieldsEnumBuilder =
        TypeSpec.enumBuilder("Fields")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .addSuperinterface(fieldsTypeName)
            .addField(short.class, "thriftId", Modifier.PRIVATE, Modifier.FINAL)
            .addField(String.class, "fieldName", Modifier.PRIVATE, Modifier.FINAL)
            .addField(Type.class, "fieldType", Modifier.PRIVATE, Modifier.FINAL)
            .addField(Class.class, "fieldClass", Modifier.PRIVATE, Modifier.FINAL)
            .addMethod(
                MethodSpec.constructorBuilder()
                    .addParameter(short.class, "thriftId")
                    .addParameter(String.class, "fieldName")
                    .addParameter(Type.class, "fieldType")
                    .addParameter(Class.class, "fieldClass")
                    .addStatement("this.thriftId = thriftId")
                    .addStatement("this.fieldName = fieldName")
                    .addStatement("this.fieldType = fieldType")
                    .addStatement("this.fieldClass = fieldClass")
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("getThriftFieldId")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(short.class)
                    .addStatement("return thriftId")
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("getFieldName")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(String.class)
                    .addStatement("return fieldName")
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("getFieldType")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(Type.class)
                    .addStatement("return fieldType")
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("getFieldClass")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(Class.class)
                    .addStatement("return fieldClass")
                    .build());

    ParameterSpec fieldIdParam = ParameterSpec.builder(short.class, "fieldId").build();
    MethodSpec.Builder findByThriftIdMethod =
        MethodSpec.methodBuilder("findByThriftId")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .addParameter(fieldIdParam)
            .returns(fieldsClassName);

    CodeBlock.Builder findByThriftIdCode =
        CodeBlock.builder()
            .beginControlFlow("switch ($N)", fieldIdParam);

    for (ThriftField field : struct.getFields()) {
      short fieldId = extractId(field);
      String enumValueName = toUpperSnakeCaseName(field);

      TypeName fieldType = typeName(field.getType());
      if (fieldType instanceof ParameterizedTypeName) {
        ParameterizedTypeName typeToken =
            ParameterizedTypeName.get(ClassName.get(TypeToken.class), fieldType);
        thriftFieldsEnumBuilder.addEnumConstant(
            enumValueName,
            TypeSpec.anonymousClassBuilder(
                "(short) $L, $S, new $T() {}.getType(), $T.class",
                fieldId,
                field.getName(),
                typeToken,
                ((ParameterizedTypeName) fieldType).rawType).build());
      } else {
        thriftFieldsEnumBuilder.addEnumConstant(
            enumValueName,
            TypeSpec.anonymousClassBuilder(
                "(short) $L, $S, $T.class, $T.class",
                fieldId,
                field.getName(),
                fieldType,
                fieldType).build());
      }

      findByThriftIdCode.addStatement(
          "case $L: return $T.$L", fieldId, fieldsClassName, enumValueName);
    }

    findByThriftIdCode
        .addStatement(
            "default: throw new $T($T.format($S, $N))",
            IllegalArgumentException.class,
            String.class,
            "%d is not a known field id.",
            fieldIdParam)
        .endControlFlow();

    thriftFieldsEnumBuilder
        .addMethod(
            findByThriftIdMethod
                .addCode(findByThriftIdCode.build())
                .build());

    typeBuilder.addType(thriftFieldsEnumBuilder.build());

    return Optional.of(fieldsClassName);
  }

  protected final TypeSpec writeType(TypeSpec.Builder typeBuilder) throws IOException {
    return writeType(getPackageName(), typeBuilder);
  }
}
