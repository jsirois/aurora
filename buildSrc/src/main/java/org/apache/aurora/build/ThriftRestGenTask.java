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
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import javax.lang.model.element.Modifier;

import com.facebook.swift.parser.ThriftIdlParser;
import com.facebook.swift.parser.model.BaseType;
import com.facebook.swift.parser.model.Const;
import com.facebook.swift.parser.model.ConstDouble;
import com.facebook.swift.parser.model.ConstIdentifier;
import com.facebook.swift.parser.model.ConstInteger;
import com.facebook.swift.parser.model.ConstList;
import com.facebook.swift.parser.model.ConstMap;
import com.facebook.swift.parser.model.ConstString;
import com.facebook.swift.parser.model.ConstValue;
import com.facebook.swift.parser.model.Document;
import com.facebook.swift.parser.model.IdentifierType;
import com.facebook.swift.parser.model.IntegerEnum;
import com.facebook.swift.parser.model.IntegerEnumField;
import com.facebook.swift.parser.model.ListType;
import com.facebook.swift.parser.model.MapType;
import com.facebook.swift.parser.model.SetType;
import com.facebook.swift.parser.model.Struct;
import com.facebook.swift.parser.model.ThriftField;
import com.facebook.swift.parser.model.ThriftType;
import com.facebook.swift.parser.model.Union;
import com.facebook.swift.parser.visitor.DocumentVisitor;
import com.facebook.swift.parser.visitor.Visitable;
import com.google.common.base.CaseFormat;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.io.CharSource;
import com.google.common.io.Files;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import org.gradle.api.DefaultTask;
import org.gradle.api.logging.Logger;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.TaskAction;
import org.immutables.value.Value;

import static java.util.Objects.requireNonNull;

public class ThriftRestGenTask extends DefaultTask {
  private String packageName;

  @Input
  public void setPackageName(String packageName) {
    this.packageName = packageName;
  }

  interface LiteralFactory {
    CodeBlock create(ThriftType type, ConstValue value);
  }

  abstract static class StructLike {
    static StructLike from(Struct struct) {
      return new StructLike(struct.getName(), struct.getFields()) {
        CodeBlock createLiteral(
            ImmutableMap<String, ConstValue> parameterMap,
            LiteralFactory literalFactory) {

          CodeBlock.Builder codeBuilder =
              CodeBlock.builder()
                  .add("$L.builder()", getName());

          for (ThriftField field : getFields()) {
            String fieldName = field.getName();
            if (parameterMap.containsKey(fieldName)) {
              ConstValue fieldValue = parameterMap.get(fieldName);
              codeBuilder.add("\n.$L(", fieldName);
              codeBuilder.add(literalFactory.create(field.getType(), fieldValue));
              codeBuilder.add(")");
            }
          }

          return codeBuilder
              .add("\n.build()")
              .build();
        }
      };
    }

    static StructLike from(Union union) {
      return new StructLike(union.getName(), union.getFields()) {
        CodeBlock createLiteral(
            ImmutableMap<String, ConstValue> parameterMap,
            LiteralFactory literalFactory) {

          Map.Entry<String, ConstValue> element =
              Iterables.getOnlyElement(parameterMap.entrySet());
          String elementName = element.getKey();
          ThriftField elementField =
              Maps.uniqueIndex(getFields(), ThriftField::getName).get(elementName);
          if (elementField == null) {
            throw new RuntimeException(); // TODO(John Sirois): XXX
          }
          ThriftType elementType = elementField.getType();

          return CodeBlock.builder()
              .add("new $L(", getName())
              .add(literalFactory.create(elementType, element.getValue()))
              .add(")")
              .build();
        }
      };
    }

    private final String name;
    private final ImmutableList<ThriftField> fields;

    private StructLike(String name, Iterable<ThriftField> fields) {
      this.name = name;
      this.fields = ImmutableList.copyOf(fields);
    }

    String getName() {
      return name;
    }

    ImmutableList<ThriftField> getFields() {
      return fields;
    }

    abstract CodeBlock createLiteral(
        ImmutableMap<String, ConstValue> parameterMap,
        LiteralFactory literalFactory);
  }

  @TaskAction
  public void gen() throws IOException {
    // TODO(John Sirois): XXX The parser does not carry over doc comments and we want these for the
    // rest api.
    // TODO(John Sirois): The parser does not carry over annotations in all possible locations -
    // we may want this.

    Set<File> thriftFiles = getInputs().getFiles().getFiles();
    File outdir = getOutputs().getFiles().getSingleFile();

    // const -> //
    // enum -> //
    // from -> //
    // from -> //
    // service -> <<
    // typedef? - not used by aurora yet
    final ImmutableMap<Class<? extends Visitable>, Visitor<? extends Visitable>> visitors =
        ImmutableMap.<Class<? extends Visitable>, Visitor<? extends Visitable>>builder()
            .put(Const.class, new ConstVisitor(getLogger(), outdir, packageName))
            .put(IntegerEnum.class, new IntegerEnumVisitor(getLogger(), outdir, packageName))
            .put(Struct.class, new StructVisitor(getLogger(), outdir, packageName))
            .put(Union.class, new UnionVisitor(getLogger(), outdir, packageName))
            .build();

    ImmutableMap.Builder<String, StructLike> structsByName = ImmutableMap.builder();
    DocumentVisitor visitor = new DocumentVisitor() {
      @Override
      public boolean accept(Visitable visitable) {
        return visitors.containsKey(visitable.getClass())
            || visitable instanceof Struct
            || visitable instanceof Union;
      }

      // We only accept visitables we have a type-matching visitor for above.
      @SuppressWarnings("unchecked")
      @Override
      public void visit(Visitable visitable) throws IOException {
        if (visitable instanceof Struct) {
          Struct struct = (Struct) visitable;
          structsByName.put(struct.getName(), StructLike.from(struct));
        } else if (visitable instanceof Union) {
          Union union = (Union) visitable;
          structsByName.put(union.getName(), StructLike.from(union));
        }
        Visitor visitor = visitors.get(visitable.getClass());
        if (visitor != null) {
          visitor.visit(visitable);
        }
      }

      @Override
      public void finish() throws IOException {
        ImmutableMap<String, StructLike> structs = structsByName.build();
        for (Visitor<?> visitor : visitors.values()) {
          visitor.finish(structs);
        }
      }
    };

    for (File thriftFile : thriftFiles) {
      if (thriftFile.getName().equals("api.thrift")) {
        CharSource thriftIdl = Files.asCharSource(thriftFile, Charsets.UTF_8);
        Document document = ThriftIdlParser.parseThriftIdl(thriftIdl);
        document.visit(visitor);
      } else {
        getLogger().warn("Skipping thrift file {}", thriftFile);
      }
    }
    visitor.finish();
  }

  interface Visitor<T extends Visitable> {
    void visit(T visitable) throws IOException;

    default void finish(ImmutableMap<String, StructLike> structsByName) throws IOException {
      // noop
    }
  }

  static abstract class BaseVisitor<T extends Visitable> implements Visitor<T> {
    // TODO(John Sirois): XXX load this from a resource?
    private static final String APACHE_LICENSE =
        " Licensed under the Apache License, Version 2.0 (the \"License\");\n" +
        " you may not use this file except in compliance with the License.\n" +
        " You may obtain a copy of the License at\n" +
        "\n" +
        "     http://www.apache.org/licenses/LICENSE-2.0\n" +
        "\n" +
        " Unless required by applicable law or agreed to in writing, software\n" +
        " distributed under the License is distributed on an \"AS IS\" BASIS,\n" +
        " WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n" +
        " See the License for the specific language governing permissions and\n" +
        " limitations under the License.";

    private final Logger logger;
    private final File outdir;
    private final String packageName;

    public BaseVisitor(Logger logger, File outdir, String packageName) {
      this.logger = logger;
      this.outdir = requireNonNull(outdir);
      this.packageName = requireNonNull(packageName);
    }

    protected final Logger getLogger() {
      return logger;
    }

    protected final File getOutdir() {
      return outdir;
    }

    protected final String getPackageName() {
      return packageName;
    }

    protected final void writeType(TypeSpec type) throws IOException {
      JavaFile enumeration = JavaFile.builder(getPackageName(), type)
          .addFileComment(APACHE_LICENSE)
          .indent("  ")
          .skipJavaLangImports(true)
          .build();
      getLogger().info("Wrote {} to {}", type.name, getOutdir());
      enumeration.writeTo(getOutdir());
    }

    protected final TypeName typeName(ThriftType thriftType) {
      if (thriftType instanceof BaseType) {
        BaseType baseType = (BaseType) thriftType;
        switch (baseType.getType()) {
          case BINARY: return ClassName.get(ByteBuffer.class);
          case BOOL: return TypeName.BOOLEAN;
          case BYTE: return TypeName.BYTE;
          case DOUBLE: return TypeName.DOUBLE;
          case I16: return TypeName.SHORT;
          case I32: return TypeName.INT;
          case I64: return TypeName.LONG;
          case STRING: return ClassName.get(String.class);
        }
      } else if (thriftType instanceof IdentifierType) {
        return ClassName.get(getPackageName(), ((IdentifierType) thriftType).getName());
      } else if (thriftType instanceof MapType) {
        MapType mapType = (MapType) thriftType;
        ThriftType keyType = mapType.getKeyType();
        ThriftType valueType = mapType.getValueType();
        return ParameterizedTypeName.get(ClassName.get(ImmutableMap.class), typeName(keyType).box(),
            typeName(valueType).box());
      } else if (thriftType instanceof ListType) {
        ThriftType elementType = ((ListType) thriftType).getElementType();
        return ParameterizedTypeName.get(ClassName.get(ImmutableList.class),
            typeName(elementType).box());
      } else if (thriftType instanceof SetType) {
        ThriftType elementType = ((SetType) thriftType).getElementType();
        return ParameterizedTypeName.get(ClassName.get(ImmutableSet.class),
            typeName(elementType).box());
      }
      throw new RuntimeException(); // TODO(John Sirois): XXX
    }

    public CodeBlock createLiteral(
        ImmutableMap<String, StructLike> structsByName,
        ThriftType type,
        ConstValue value) {

      CodeBlock.Builder codeBuilder = CodeBlock.builder();
      if (value instanceof ConstInteger || value instanceof ConstDouble) {
        codeBuilder.add("$L", value.value());
      } else if (value instanceof ConstString) {
        ConstString constString = (ConstString) value;
        codeBuilder.add("$S", constString.value());
      } else if (value instanceof ConstList) {
        Class<?> containerType;
        ThriftType elementType;
        if (type instanceof ListType) {
          containerType = ImmutableList.class;
          elementType = ((ListType) type).getElementType();
        } else if (type instanceof SetType) {
          containerType = ImmutableSet.class;
          elementType = ((SetType) type).getElementType();
        } else {
          throw new RuntimeException(); // TODO(John Sirois): XXX
        }
        codeBuilder.add("$T.<$T>builder()", containerType, typeName(elementType));
        for (ConstValue elementValue : ((ConstList) value).value()) {
          codeBuilder.add("\n.add(");
          codeBuilder.add(createLiteral(structsByName, elementType, elementValue));
          codeBuilder.add(")");
        }
        codeBuilder.add("\n.build()");
      } else if (value instanceof ConstMap) {
        ConstMap map = (ConstMap) value;
        if (type instanceof MapType) {
          getLogger().warn("Skipping literal rendering for map {}", value);
        } else if (type instanceof IdentifierType) {
          StructLike struct = structsByName.get(((IdentifierType) type).getName());
          if (struct == null) {
            throw new RuntimeException(); // TODO(John Sirois): XXX
          }
          ImmutableMap<String, ConstValue> parameterMap =
              ImmutableMap.copyOf(
                  Maps.transformValues(
                      Maps.uniqueIndex(map.value().entrySet(), entry -> {
                        ConstValue key = entry.getKey();
                        if (!(key instanceof ConstString)) {
                          throw new RuntimeException(); // TODO(John Sirois): XXX
                        }
                        return ((ConstString) key).value();
                      }),
                      Map.Entry::getValue));
          CodeBlock literal =
              struct.createLiteral(
                  parameterMap,
                  (t, v) -> createLiteral(structsByName, t, v));
          codeBuilder.add(literal);
        } else {
          throw new RuntimeException(); // TODO(John Sirois): XXX
        }
      } else if (value instanceof ConstIdentifier) {
        codeBuilder.add("$L", value.value());
      } else {
        throw new RuntimeException(); // TODO(John Sirois): XXX
      }
      return codeBuilder.build();
    }
  }

  static class StructVisitor extends BaseVisitor<Struct> {
    private final ImmutableList.Builder<Struct> structs = ImmutableList.builder();

    public StructVisitor(Logger logger, File outdir, String packageName) {
      super(logger, outdir, packageName);
    }

    @Override
    public void visit(Struct struct) throws IOException {
      structs.add(struct);
    }

    @Override
    public void finish(ImmutableMap<String, StructLike> structsByName) throws IOException {
      for (Struct struct: structs.build()) {
        writeStruct(structsByName, struct);
      }
    }

    private void writeStruct(ImmutableMap<String, StructLike> structsByName, Struct struct)
        throws IOException {
      // NB: An abstract class is used here instead of an interface for the sake of a bug in the
      // swift code bytecode compiler.  Swift just relaxed the constraints on from types to not
      // be mandatory final in 1.16.0 - allowing abstract class and interface types, but the
      // bytecode generator expects classes and not interfaces when generating its bytecode.
      TypeSpec.Builder typeBuilder =
          TypeSpec.classBuilder(struct.getName())
              .addAnnotation(
                  AnnotationSpec.builder(com.facebook.swift.codec.ThriftStruct.class)
                      .addMember("value", "$S", struct.getName())
                      .addMember("builder", "$L.Builder.class", struct.getName())
                      .build())
              .addAnnotation(Value.Immutable.class)
              .addAnnotation(
                  AnnotationSpec.builder(Value.Style.class)
                      .addMember("visibility", "$T.PACKAGE",
                          Value.Style.ImplementationVisibility.class)
                      .addMember("build", "$S", "_build")
                      .build())
              .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT);

      // A convenience builder factory method for coding against; the Builder is defined below.
      typeBuilder.addMethod(
          MethodSpec.methodBuilder("builder")
              .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
              .returns(ClassName.get(getPackageName(), struct.getName(), "Builder"))
              .addStatement("return new Builder()")
              .build());

      // Make the constructor package private for the Immutable implementations to access.
      typeBuilder.addMethod(MethodSpec.constructorBuilder().build());

      for (ThriftField field : struct.getFields()) {
        ThriftField.Requiredness requiredness = field.getRequiredness();
        Optional<Long> identifier = field.getIdentifier();

        MethodSpec.Builder accessorBuilder =
            MethodSpec.methodBuilder(field.getName())
                .addAnnotation(
                    AnnotationSpec.builder(com.facebook.swift.codec.ThriftField.class)
                        .addMember("value", "$L", identifier.get()) // TODO(John Sirois): XXX
                        .addMember("name", "$S", field.getName())
                        .addMember("requiredness", "$T.$L",
                            com.facebook.swift.codec.ThriftField.Requiredness.class,
                            requiredness.name())
                        .build())
                .addModifiers(Modifier.PUBLIC)
                .returns(typeName(field.getType()));
        Optional<ConstValue> defaultValue = field.getValue();
        if (defaultValue.isPresent()) {
          CodeBlock literal = createLiteral(structsByName, field.getType(), defaultValue.get());
          accessorBuilder.addStatement("return $L", literal);
        } else {
          accessorBuilder.addModifiers(Modifier.ABSTRACT);
        }
        typeBuilder.addMethod(accessorBuilder.build());
      }

      // This public nested Builder class with no-arg constructor is needed by ThriftCodec.
      ClassName builderName =
          ClassName.get(getPackageName(), String.format("Immutable%s.Builder", struct.getName()));
      typeBuilder.addType(
          TypeSpec.classBuilder("Builder")
              .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
              .superclass(builderName)
              .addMethod(
                  MethodSpec.methodBuilder("build")
                      .addAnnotation(com.facebook.swift.codec.ThriftConstructor.class)
                      .returns(ClassName.get(getPackageName(), struct.getName()))
                      .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                      .addStatement("return _build()")
                      .build())
              .build());
      writeType(typeBuilder.build());
    }
  }

  static class UnionVisitor extends BaseVisitor<Union> {
    public UnionVisitor(Logger logger, File outdir, String packageName) {
      super(logger, outdir, packageName);
    }

    @Override
    public void visit(Union union) throws IOException {
      TypeSpec.Builder typeBuilder =
          TypeSpec.classBuilder(union.getName())
              .addAnnotation(
                  AnnotationSpec.builder(com.facebook.swift.codec.ThriftUnion.class)
                      .addMember("value", "$S", union.getName())
                      .build())
              .addModifiers(Modifier.PUBLIC, Modifier.FINAL);

      typeBuilder.addField(Object.class, "value", Modifier.PRIVATE, Modifier.FINAL);
      typeBuilder.addField(short.class, "id", Modifier.PRIVATE, Modifier.FINAL);

      for (ThriftField field : union.getFields()) {
        short structId = field.getIdentifier().get().shortValue();
        TypeName fieldTypeName = typeName(field.getType());
        String upperCamelCaseFieldName =
            CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, field.getName());

        typeBuilder.addMethod(
            MethodSpec.constructorBuilder()
                .addAnnotation(com.facebook.swift.codec.ThriftConstructor.class)
                .addModifiers(Modifier.PUBLIC)
                .addParameter(fieldTypeName, field.getName())
                .addStatement("this.value = $L", field.getName())
                .addStatement("this.id = $L", structId)
                .build());

        MethodSpec isSetMethod =
            MethodSpec.methodBuilder("isSet" + upperCamelCaseFieldName)
                .addModifiers(Modifier.PUBLIC)
                .returns(boolean.class)
                .addStatement("return id == $L", structId)
                .build();
        typeBuilder.addMethod(isSetMethod);

        typeBuilder.addMethod(
            MethodSpec.methodBuilder("get" + upperCamelCaseFieldName)
                .addAnnotation(
                    AnnotationSpec.builder(com.facebook.swift.codec.ThriftField.class)
                        .addMember("value", "$L", structId)
                        .addMember("name", "$S", field.getName())
                        .addMember("requiredness", "$T.$L",
                            com.facebook.swift.codec.ThriftField.Requiredness.class,
                            field.getRequiredness().name())
                        .build())
                .addModifiers(Modifier.PUBLIC)
                .returns(fieldTypeName)
                .addCode(
                    CodeBlock.builder()
                        .beginControlFlow("if (!$N())", isSetMethod)
                        .addStatement("throw new $T()", IllegalStateException.class)
                        .endControlFlow()
                        .addStatement("return ($T) value", fieldTypeName)
                        .build())
                .build());
      }

      typeBuilder.addMethod(
          MethodSpec.methodBuilder("getSetId")
              .addAnnotation(com.facebook.swift.codec.ThriftUnionId.class)
              .addModifiers(Modifier.PUBLIC)
              .returns(short.class)
              .addStatement("return id")
              .build());

      writeType(typeBuilder.build());
    }
  }

  static class IntegerEnumVisitor extends BaseVisitor<IntegerEnum> {
    public IntegerEnumVisitor(Logger logger, File outdir, String packageName) {
      super(logger, outdir, packageName);
    }

    @Override
    public void visit(IntegerEnum integerEnum) throws IOException {
      TypeSpec.Builder typeBuilder =
          TypeSpec.enumBuilder(integerEnum.getName())
              .addModifiers(Modifier.PUBLIC);

      typeBuilder.addField(int.class, "value", Modifier.PRIVATE, Modifier.FINAL);
      typeBuilder.addMethod(
          MethodSpec.constructorBuilder()
              .addModifiers(Modifier.PRIVATE)
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
      writeType(typeBuilder.build());
    }
  }

  static class ConstVisitor extends BaseVisitor<Const> {
    private final ImmutableList.Builder<Const> consts = ImmutableList.builder();

    public ConstVisitor(Logger logger, File outdir, String packageName) {
      super(logger, outdir, packageName);
    }

    @Override
    public void visit(Const constant) {
      consts.add(constant);
    }

    @Override
    public void finish(ImmutableMap<String, StructLike> structsByName) throws IOException {
      TypeSpec.Builder typeBuilder =
          TypeSpec.classBuilder("Constants")
              .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
              .addMethod(MethodSpec.constructorBuilder().addModifiers(Modifier.PRIVATE).build());

      for (Const constant : consts.build()) {
        ThriftType fieldType = constant.getType();
        typeBuilder.addField(
            FieldSpec.builder(
                typeName(fieldType),
                constant.getName(),
                Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .initializer(createLiteral(structsByName, fieldType, constant.getValue()))
                .build());
      }
      writeType(typeBuilder.build());
    }
  }
}
