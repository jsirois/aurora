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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;
import javax.lang.model.element.Modifier;

import com.facebook.swift.parser.ThriftIdlParser;
import com.facebook.swift.parser.model.AbstractStruct;
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
import com.facebook.swift.parser.model.StringEnum;
import com.facebook.swift.parser.model.Struct;
import com.facebook.swift.parser.model.ThriftException;
import com.facebook.swift.parser.model.ThriftField;
import com.facebook.swift.parser.model.ThriftType;
import com.facebook.swift.parser.model.Typedef;
import com.facebook.swift.parser.model.Union;
import com.facebook.swift.parser.visitor.DocumentVisitor;
import com.facebook.swift.parser.visitor.Visitable;
import com.google.common.base.CaseFormat;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
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

import static java.util.Objects.requireNonNull;

/**
 * Generates thrift stubs for structs and services.
 */
public class ThriftRestGenTask extends DefaultTask {

  /**
   * Indicates an unexpected semantic parsing error.
   *
   * If thrown, the thrift IDL was itself was valid, but it expressed relationships not supported
   * by the thrift spec.
   */
  public static class ParseError extends RuntimeException {
    public ParseError(String message) {
      super(message);
    }
  }

  /**
   * Indicates a combination of parsed thrift IDL value and type that is unexpected according to
   * the thrift spec.
   */
  public static class UnexpectedTypeException extends ParseError {
    public UnexpectedTypeException(String message) {
      super(message);
    }
  }

  private Optional<String> packageSuffix = Optional.absent();

  @Input
  public void setPackageSuffix(String packageSuffix) {
    String normalizedSuffix = packageSuffix.trim();
    if (normalizedSuffix.isEmpty()) {
      throw new IllegalArgumentException("Invalid packageSuffix, cannot be blank.");
    }
    this.packageSuffix = Optional.of(normalizedSuffix);
  }

  @TaskAction
  public void gen() throws IOException {
    // TODO(John Sirois): The parser does not carry over doc comments and we want these for the
    // rest api, investigate a patch to add support before copying/moving comments to annotations.

    // TODO(John Sirois): The parser does not carry over annotations in all possible locations -
    // we may want this - partially depends on TODO above.

    File outdir = getOutputs().getFiles().getSingleFile();
    Map<String, String> packageNameByImportPrefix = new HashMap<>();
    Set<File> thriftFiles =
        getInputs().getFiles().getFiles()
            .stream()
            .map(File::getAbsoluteFile)
            .collect(Collectors.toSet());
    processThriftFiles(packageNameByImportPrefix, thriftFiles, outdir, false);
  }

  private void processThriftFiles(
      Map<String, String> packageNameByImportPrefix,
      Set<File> thriftFiles,
      File outdir,
      boolean required)
      throws IOException {

    Set<File> processed = new HashSet<>();
    for (File thriftFile : thriftFiles) {
      CharSource thriftIdl = Files.asCharSource(thriftFile, Charsets.UTF_8);
      Document document = ThriftIdlParser.parseThriftIdl(thriftIdl);
      String packageName = document.getHeader().getNamespace("java");
      if (packageName == null) {
        if (required) {
          throw new IllegalArgumentException(
              String.format("%s must declare a 'java' namespace", thriftFile.getPath()));
        } else {
          getLogger().warn("Skipping {} - no java namespace", thriftFile);
        }
      } else {
        Set<File> includes =
            document.getHeader().getIncludes()
                .stream()
                .map(inc -> new File(thriftFile.getParentFile(), inc).getAbsoluteFile())
                .filter(f -> !processed.contains(f))
                .collect(Collectors.toSet());
        processThriftFiles(packageNameByImportPrefix, includes, outdir, true);

        if (packageSuffix.isPresent()) {
          packageName = packageName + packageSuffix.get();
        }
        ThriftGenVisitor visitor =
            new ThriftGenVisitor(
                getLogger(),
                outdir,
                ImmutableMap.copyOf(packageNameByImportPrefix),
                packageName);
        document.visit(visitor);
        visitor.finish();
        packageNameByImportPrefix.put(
            Files.getNameWithoutExtension(thriftFile.getName()),
            packageName);
        processed.add(thriftFile);
      }
    }
  }

  @NotThreadSafe
  static class ThriftGenVisitor implements DocumentVisitor {
    private final ImmutableMap.Builder<String, AbstractStructRenderer> structRendererByName =
        ImmutableMap.builder();

    private final ImmutableMap<Class<? extends Visitable>, Visitor<? extends Visitable>> visitors;

    private boolean finished;

    ThriftGenVisitor(
        Logger logger,
        File outdir,
        ImmutableMap<String, String> packageNameByImportPrefix,
        String packageName) {

      // service -> <<
      visitors =
          ImmutableMap.<Class<? extends Visitable>, Visitor<? extends Visitable>>builder()
              .put(Const.class,
                  new ConstVisitor(logger, outdir, packageNameByImportPrefix, packageName))
              .put(IntegerEnum.class,
                  new IntegerEnumVisitor(logger, outdir, packageNameByImportPrefix, packageName))
              // Not needed by Aurora and of questionable value to ever add support for.
              .put(StringEnum.class,
                  Visitor.failing("The Senum type is deprecated and removed in thrift 1.0.0, " +
                      "see: https://issues.apache.org/jira/browse/THRIFT-2003"))
              .put(Struct.class,
                  new StructVisitor(logger, outdir, packageNameByImportPrefix, packageName,
                      Object.class))
              // Currently not used by Aurora, but trivial to support.
              .put(ThriftException.class,
                  new StructVisitor(logger, outdir, packageNameByImportPrefix, packageName,
                      Exception.class))
              // TODO(John Sirois): Implement as the need arises.
              // Currently not needed by Aurora; requires deferring all generation to `finish` and
              // collecting a full symbol table + adding a resolve method to resolve through
              // typedefs.
              .put(Typedef.class, Visitor.failing())
              .put(Union.class,
                  new UnionVisitor(logger, outdir, packageNameByImportPrefix, packageName))
              .build();
    }

    @Override
    public boolean accept(Visitable visitable) {
      return visitors.containsKey(visitable.getClass()) || visitable instanceof AbstractStruct;
    }

    // We only accept visitables we have a type-matching visitor for above; so the raw typed
    // `visitor.visit(visitable);` call below is safe.
    @SuppressWarnings("unchecked")
    @Override
    public void visit(Visitable visitable) throws IOException {
      if (visitable instanceof AbstractStruct) {
        AbstractStruct struct = (AbstractStruct) visitable;
        structRendererByName.put(struct.getName(), AbstractStructRenderer.from(struct));
      }
      Visitor visitor = visitors.get(visitable.getClass());
      if (visitor != null) {
        visitor.visit(visitable);
      }
    }

    @Override
    public void finish() throws IOException {
      if (!finished) {
        ImmutableMap<String, AbstractStructRenderer> structRenderers = structRendererByName.build();
        for (Visitor<?> visitor : visitors.values()) {
          visitor.finish(structRenderers);
        }
        finished = true;
      }
    }
  }

  interface CodeBuilder {
    void build(CodeBlock.Builder builder);
  }

  static CodeBlock indented(CodeBuilder codeBuilder) {
    CodeBlock.Builder codeBlockBuilder = CodeBlock.builder();
    indented(codeBlockBuilder, () -> codeBuilder.build(codeBlockBuilder));
    return codeBlockBuilder.build();
  }

  static void indented(CodeBlock.Builder codeBlockBuilder, Runnable codeBuilder) {
    codeBlockBuilder.add("$>$>");
    codeBuilder.run();
    codeBlockBuilder.add("$<$<");
  }

  interface LiteralFactory {
    CodeBlock create(ThriftType type, ConstValue value);
  }

  abstract static class AbstractStructRenderer {
    private static class StructRenderer extends AbstractStructRenderer {
      private StructRenderer(AbstractStruct struct) {
        super(struct);
      }

      @Override
      CodeBlock createLiteral(
          ImmutableMap<String, ConstValue> parameters,
          LiteralFactory literalFactory) {

        return indented(codeBuilder -> {
          codeBuilder.add("$L.builder()", name);
          for (ThriftField field : fields) {
            String fieldName = field.getName();
            if (parameters.containsKey(fieldName)) {
              ConstValue fieldValue = parameters.get(fieldName);
              codeBuilder.add("\n.$L(", fieldName);
              codeBuilder.add(literalFactory.create(field.getType(), fieldValue));
              codeBuilder.add(")");
            }
          }
          codeBuilder.add("\n.build()");
        });
      }
    }

    private static class UnionRenderer extends AbstractStructRenderer {
      private UnionRenderer(AbstractStruct struct) {
        super(struct);
      }

      @Override
      CodeBlock createLiteral(
          ImmutableMap<String, ConstValue> parameters,
          LiteralFactory literalFactory) {

        Map.Entry<String, ConstValue> element =
            Iterables.getOnlyElement(parameters.entrySet());
        String elementName = element.getKey();
        ThriftField elementField =
            Maps.uniqueIndex(fields, ThriftField::getName).get(elementName);
        if (elementField == null) {
          throw new ParseError(
              String.format(
                  "Encountered a union literal that selects a non-existent member '%s'.\n" +
                      "Only the following members are known:\n\t%s",
                  elementName,
                  Joiner.on("\n\t").join(fields.stream().map(ThriftField::getName).iterator())));
        }
        ThriftType elementType = elementField.getType();

        return CodeBlock.builder()
            .add("new $L(", name)
            .add(literalFactory.create(elementType, element.getValue()))
            .add(")")
            .build();
      }
    }

    static AbstractStructRenderer from(AbstractStruct struct) {
      if (struct instanceof Struct || struct instanceof ThriftException) {
        return new StructRenderer(struct);
      } else if (struct instanceof Union) {
        return new UnionRenderer(struct);
      } else {
        throw new UnexpectedTypeException("Unknown struct type: " + struct);
      }
    }

    protected final String name;
    protected final ImmutableList<ThriftField> fields;

    private AbstractStructRenderer(AbstractStruct struct) {
      this.name = struct.getName();
      this.fields = ImmutableList.copyOf(struct.getFields());
    }

    abstract CodeBlock createLiteral(
        ImmutableMap<String, ConstValue> parameters,
        LiteralFactory literalFactory);
  }

  interface Visitor<T extends Visitable> {
    static Visitor<?> failing() {
      return failing(Optional.absent());
    }

    static Visitor<?> failing(String reason) {
      return failing(Optional.of(reason));
    }

    static Visitor<?> failing(Optional<String> reason) {
      return visitable -> {
        String msg = String.format("Unsupported thrift IDL type: %s", visitable);
        if (reason.isPresent()) {
          msg = String.format("%s%n%s", msg, reason.get());
        }
        throw new IllegalArgumentException(msg);
      };
    }

    void visit(T visitable) throws IOException;

    default void finish(ImmutableMap<String, AbstractStructRenderer> structRenderers)
        throws IOException {
      // noop
    }
  }

  static abstract class BaseVisitor<T extends Visitable> implements Visitor<T> {
    // TODO(John Sirois): Load this from a resource.
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

    protected static short extractId(ThriftField field) {
      Optional<Long> identifier = field.getIdentifier();
      if (!identifier.isPresent()) {
        throw new ParseError("All thrift fields must have an id, given field without id: " + field);
      }
      Long id = identifier.get();
      short value = id.shortValue();
      if (id != value) {
        throw new ParseError("All ids are expected to be shorts, given " + id);
      }
      return value;
    }

    private final Logger logger;
    private final File outdir;
    private final ImmutableMap<String, String> packageNameByImportPrefix;
    private final String packageName;

    public BaseVisitor(
        Logger logger,
        File outdir,
        ImmutableMap<String, String> packageNameByImportPrefix,
        String packageName) {

      this.logger = logger;
      this.outdir = requireNonNull(outdir);
      this.packageNameByImportPrefix = packageNameByImportPrefix;
      this.packageName = requireNonNull(packageName);
    }

    protected final Logger getLogger() {
      return logger;
    }

    protected final File getOutdir() {
      return outdir;
    }

    protected final ClassName getClassName(IdentifierType identifierType) {
      return getClassName(identifierType.getName());
    }

    protected final String getPackageName(ConstIdentifier identifierValue) {
      return getClassName(identifierValue.value()).packageName();
    }

    private final ClassName getClassName(String identifier) {
      List<String> parts = Splitter.on('.').limit(2).splitToList(identifier);
      if (parts.size() == 1) {
        return ClassName.get(getPackageName(), identifier);
      } else {
        String importPrefix = parts.get(0);
        String typeName = parts.get(1);
        String packageName = packageNameByImportPrefix.get(importPrefix);
        if (packageName == null) {
          throw new ParseError(
              String.format("Could not map identifier %s to a parsed type", identifier));
        }
        return ClassName.get(packageName, typeName);
      }
    }

    protected final String getPackageName() {
      return packageName;
    }

    protected final void writeType(TypeSpec type) throws IOException {
      JavaFile javaFile =
          JavaFile.builder(getPackageName(), type)
              .addFileComment(APACHE_LICENSE)
              .indent("  ")
              .skipJavaLangImports(true)
              .build();
      javaFile.writeTo(getOutdir());
      getLogger().info("Wrote {} to {}", type.name, getOutdir());
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
        return getClassName((IdentifierType) thriftType);
      } else if (thriftType instanceof MapType) {
        MapType mapType = (MapType) thriftType;
        ThriftType keyType = mapType.getKeyType();
        ThriftType valueType = mapType.getValueType();
        return ParameterizedTypeName.get(
            ClassName.get(ImmutableMap.class),
            typeName(keyType).box(),
            typeName(valueType).box());
      } else if (thriftType instanceof ListType) {
        ThriftType elementType = ((ListType) thriftType).getElementType();
        return ParameterizedTypeName.get(
            ClassName.get(ImmutableList.class),
            typeName(elementType).box());
      } else if (thriftType instanceof SetType) {
        ThriftType elementType = ((SetType) thriftType).getElementType();
        return ParameterizedTypeName.get(
            ClassName.get(ImmutableSet.class),
            typeName(elementType).box());
      }
      throw new UnexpectedTypeException("Unknown thrift type: " + thriftType);
    }

    public CodeBlock createLiteral(
        ImmutableMap<String, AbstractStructRenderer> structRenderers,
        ThriftType type,
        ConstValue value) {

      CodeBlock.Builder codeBuilder = CodeBlock.builder();
      if (value instanceof ConstInteger || value instanceof ConstDouble) {
        codeBuilder.add("$L", value.value());
      } else if (value instanceof ConstString) {
        codeBuilder.add("$S", value.value());
      } else if (value instanceof ConstList) {
        createListLiteral(structRenderers, type, (ConstList) value, codeBuilder);
      } else if (value instanceof ConstMap) {
        ConstMap map = (ConstMap) value;
        if (type instanceof MapType) {
          createMapLiteral(structRenderers, (MapType) type, codeBuilder, map);
        } else if (type instanceof IdentifierType) {
          createStructLiteral(structRenderers, (IdentifierType) type, codeBuilder, map);
        } else {
          throw new UnexpectedTypeException(
              String.format(
                  "Only maps and structs can be represented as map literals, encountered map " +
                      "literal %s targeted at %s",
                  value,
                  type));
        }
      } else if (value instanceof ConstIdentifier) {
        createIdentifierLiteral(type, (ConstIdentifier) value, codeBuilder);
      } else {
        throw new UnexpectedTypeException("Unknown const value type: " + value);
      }
      return codeBuilder.build();
    }

    private void createListLiteral(
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
          codeBuilder.add(createLiteral(structRenderers, elementType, elementValue));
          codeBuilder.add(")");
        }
        codeBuilder.add("\n.build()");
      });
    }

    private void createMapLiteral(
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
          codeBuilder.add(createLiteral(structRenderers, keyType, entry.getKey()));
          codeBuilder.add(", ");
          codeBuilder.add(createLiteral(structRenderers, valueType, entry.getValue()));
          codeBuilder.add(")");
        }
        codeBuilder.add("\n.build()");
      });
    }

    private void createStructLiteral(
        ImmutableMap<String, AbstractStructRenderer> structRenderers,
        IdentifierType type,
        CodeBlock.Builder codeBuilder,
        ConstMap map) {

      // TODO(John Sirois): XXX structRenderers need to handle type resolution across includes.
      AbstractStructRenderer structRenderer = structRenderers.get(type.getName());
      if (structRenderer == null) {
        throw new ParseError(
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

      // `createLiteral` is almost what we need, but with structRenderers curried.
      LiteralFactory literalFactory = (_1, _2) -> createLiteral(structRenderers, _1, _2);

      codeBuilder.add(structRenderer.createLiteral(parameters, literalFactory));
    }

    private void createIdentifierLiteral(
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
        // Up to 2 components, the rightmost is the name:
        // package local constant: CONSTANT_VALUE
        // via include.thrift: included.CONSTANT_VALUE
        ClassName className = ClassName.get(getPackageName(value), "Constants");
        String name = Iterables.getLast(Splitter.on('.').limit(2).splitToList(identifier));
        codeBuilder.add("$T.$L", className, name);
      }
    }
  }

  @NotThreadSafe
  static class StructVisitor extends BaseVisitor<Struct> {
    private final ImmutableList.Builder<Struct> structs = ImmutableList.builder();
    private final Class<?> superClass;

    public StructVisitor(
        Logger logger,
        File outdir,
        ImmutableMap<String, String> packageNameByImportPrefix,
        String packageName,
        Class<?> superClass) {

      super(logger, outdir, packageNameByImportPrefix, packageName);
      this.superClass = superClass;
    }

    @Override
    public void visit(Struct struct) throws IOException {
      structs.add(struct);
    }

    @Override
    public void finish(ImmutableMap<String, AbstractStructRenderer> structRenderers)
        throws IOException {

      for (Struct struct: structs.build()) {
        writeStruct(structRenderers, struct);
      }
    }

    private void writeStruct(
        ImmutableMap<String, AbstractStructRenderer> structRenderers,
        Struct struct)
        throws IOException {

      // NB: An abstract class is used here instead of an interface for the sake of a bug in the
      // swift code bytecode compiler.  Swift just relaxed the constraints on from types to not
      // be mandatory final in 1.16.0 - allowing abstract class and interface types, but the
      // bytecode generator expects classes and not interfaces when generating its bytecode.
      // Issue filed here: https://github.com/facebook/swift/issues/279
      // PR sent with a fix https://github.com/facebook/swift/pull/280
      TypeSpec.Builder typeBuilder =
          TypeSpec.classBuilder(struct.getName())
              .addAnnotation(
                  AnnotationSpec.builder(com.facebook.swift.codec.ThriftStruct.class)
                      .addMember("value", "$S", struct.getName())
                      .addMember("builder", "$L.Builder.class", struct.getName())
                      .build())
              .addAnnotation(org.immutables.value.Value.Immutable.class)
              .addAnnotation(
                  AnnotationSpec.builder(org.immutables.value.Value.Style.class)
                      .addMember("visibility", "$T.PACKAGE",
                          org.immutables.value.Value.Style.ImplementationVisibility.class)
                      .addMember("build", "$S", "_build")
                      .build())
              .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
              .superclass(superClass);

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
        MethodSpec.Builder accessorBuilder =
            MethodSpec.methodBuilder(field.getName())
                .addAnnotation(
                    AnnotationSpec.builder(com.facebook.swift.codec.ThriftField.class)
                        .addMember("value", "$L", extractId(field))
                        .addMember("name", "$S", field.getName())
                        .addMember("requiredness", "$T.$L",
                            com.facebook.swift.codec.ThriftField.Requiredness.class,
                            field.getRequiredness().name())
                        .build())
                .addModifiers(Modifier.PUBLIC)
                .returns(typeName(field.getType()));
        Optional<ConstValue> defaultValue = field.getValue();
        if (defaultValue.isPresent()) {
          CodeBlock literal = createLiteral(structRenderers, field.getType(), defaultValue.get());
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
    public UnionVisitor(
        Logger logger,
        File outdir,
        ImmutableMap<String, String> packageNameByImportPrefix,
        String packageName) {

      super(logger, outdir, packageNameByImportPrefix, packageName);
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
        TypeName fieldTypeName = typeName(field.getType());
        String upperCamelCaseFieldName =
            CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, field.getName());

        short id = extractId(field);

        typeBuilder.addMethod(
            MethodSpec.constructorBuilder()
                .addAnnotation(com.facebook.swift.codec.ThriftConstructor.class)
                .addModifiers(Modifier.PUBLIC)
                .addParameter(fieldTypeName, field.getName())
                .addStatement("this.value = $L", field.getName())
                .addStatement("this.id = $L", id)
                .build());

        MethodSpec isSetMethod =
            MethodSpec.methodBuilder("isSet" + upperCamelCaseFieldName)
                .addModifiers(Modifier.PUBLIC)
                .returns(boolean.class)
                .addStatement("return id == $L", id)
                .build();
        typeBuilder.addMethod(isSetMethod);

        typeBuilder.addMethod(
            MethodSpec.methodBuilder("get" + upperCamelCaseFieldName)
                .addAnnotation(
                    AnnotationSpec.builder(com.facebook.swift.codec.ThriftField.class)
                        .addMember("value", "$L", id)
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
    public IntegerEnumVisitor(
        Logger logger,
        File outdir,
        ImmutableMap<String, String> packageNameByImportPrefix,
        String packageName) {

      super(logger, outdir, packageNameByImportPrefix, packageName);
    }

    @Override
    public void visit(IntegerEnum integerEnum) throws IOException {
      TypeSpec.Builder typeBuilder =
          TypeSpec.enumBuilder(integerEnum.getName())
              .addModifiers(Modifier.PUBLIC);

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
      writeType(typeBuilder.build());
    }
  }

  @NotThreadSafe
  static class ConstVisitor extends BaseVisitor<Const> {
    private final ImmutableList.Builder<Const> consts = ImmutableList.builder();

    public ConstVisitor(
        Logger logger,
        File outdir,
        ImmutableMap<String, String> packageNameByImportPrefix,
        String packageName) {

      super(logger, outdir, packageNameByImportPrefix, packageName);
    }

    @Override
    public void visit(Const constant) {
      consts.add(constant);
    }

    @Override
    public void finish(ImmutableMap<String, AbstractStructRenderer> structRenderers)
        throws IOException {

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
                .initializer(createLiteral(structRenderers, fieldType, constant.getValue()))
                .build());
      }
      writeType(typeBuilder.build());
    }
  }
}
