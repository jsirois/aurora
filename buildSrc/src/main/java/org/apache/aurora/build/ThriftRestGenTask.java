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
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Generated;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.lang.model.element.Modifier;

import com.facebook.swift.codec.ThriftUnionId;
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
import com.facebook.swift.parser.model.ContainerType;
import com.facebook.swift.parser.model.Definition;
import com.facebook.swift.parser.model.Document;
import com.facebook.swift.parser.model.IdentifierType;
import com.facebook.swift.parser.model.IntegerEnum;
import com.facebook.swift.parser.model.IntegerEnumField;
import com.facebook.swift.parser.model.ListType;
import com.facebook.swift.parser.model.MapType;
import com.facebook.swift.parser.model.Service;
import com.facebook.swift.parser.model.SetType;
import com.facebook.swift.parser.model.StringEnum;
import com.facebook.swift.parser.model.Struct;
import com.facebook.swift.parser.model.ThriftException;
import com.facebook.swift.parser.model.ThriftField;
import com.facebook.swift.parser.model.ThriftMethod;
import com.facebook.swift.parser.model.ThriftType;
import com.facebook.swift.parser.model.TypeAnnotation;
import com.facebook.swift.parser.model.Typedef;
import com.facebook.swift.parser.model.Union;
import com.facebook.swift.parser.visitor.DocumentVisitor;
import com.facebook.swift.parser.visitor.Visitable;
import com.google.common.base.CaseFormat;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.io.CharSource;
import com.google.common.io.Files;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.ListenableFuture;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeVariableName;

import org.apache.thrift.TFieldIdEnum;
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
   * Indicates a thrift IDL feature was encountered that is not supported.
   */
  public static class UnsupportedFeatureException extends RuntimeException {
    public UnsupportedFeatureException(String message) {
      super(message);
    }
  }

  /**
   * Indicates an unexpected semantic parsing error.
   *
   * If thrown, the thrift IDL was itself was valid, but it expressed relationships not supported
   * by the thrift spec.
   */
  public static class ParseException extends RuntimeException {
    public ParseException(String message) {
      super(message);
    }
  }

  /**
   * Indicates a combination of parsed thrift IDL value and type that is unexpected according to
   * the thrift spec.
   */
  public static class UnexpectedTypeException extends ParseException {
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
    SymbolTable symbolTable = new SymbolTable();
    Set<File> thriftFiles =
        getInputs().getFiles().getFiles()
            .stream()
            .map(File::getAbsoluteFile)
            .collect(Collectors.toSet());
    processThriftFiles(symbolTable, thriftFiles, outdir, false);
  }

  static class SymbolTable {
    static class Symbol {
      private final String packageName;
      private final Definition symbol;

      Symbol(String packageName, Definition symbol) {
        this.packageName = packageName;
        this.symbol = symbol;
      }

      ClassName getClassName() {
        return ClassName.get(packageName, symbol.getName());
      }

      Definition getSymbol() {
        return symbol;
      }
    }

    private final ImmutableBiMap<File, String> importPrefixByFile;
    private final ImmutableMap<String, String> packageNameByImportPrefix;
    private final ImmutableMap<String, ImmutableMap<String, Symbol>> symbolsByPackageName;

    SymbolTable() {
      this(
          ImmutableBiMap.<File, String>of(),
          ImmutableMap.<String, String>of(),
          ImmutableMap.<String, ImmutableMap<String, Symbol>>of());
    }

    private SymbolTable(
        ImmutableBiMap<File, String> importPrefixByFile,
        ImmutableMap<String, String> packageNameByImportPrefix,
        ImmutableMap<String, ImmutableMap<String, Symbol>> symbolsByPackageName) {

      this.importPrefixByFile = importPrefixByFile;
      this.packageNameByImportPrefix = packageNameByImportPrefix;
      this.symbolsByPackageName = symbolsByPackageName;
    }

    Symbol lookup(String packageName, IdentifierType identifier) {
      return lookup(packageName, identifier.getName());
    }

    Symbol lookup(String packageName, String identifierName) {
      List<String> parts = Splitter.on('.').limit(2).splitToList(identifierName);
      if (parts.size() == 2) {
        String importPrefix = parts.get(0);
        packageName = packageNameByImportPrefix.get(importPrefix);
        if (packageName == null) {
          throw new ParseException(
              String.format(
                  "Could not map identifier %s to a parsed type: %s", identifierName, this));
        }
        identifierName = parts.get(1);
      }
      return symbolsByPackageName.get(packageName).get(identifierName);
    }

    SymbolTable updated(File file, String packageName, Iterable<Definition> definitions) {
      if (importPrefixByFile.containsKey(file)) {
        return this;
      }

      String importPrefix = Files.getNameWithoutExtension(file.getName());
      String existingPackageName = packageNameByImportPrefix.get(importPrefix);
      if (existingPackageName != null) {
        throw new ParseException(
            String.format(
                "Invalid include, already have an include with prefix of %s containing " +
                    "definitions for package %s in file %s.",
                importPrefix, existingPackageName, importPrefixByFile.inverse().get(importPrefix)));
      }

      ImmutableBiMap<File, String> prefixByFile =
          ImmutableBiMap.<File, String>builder()
              .putAll(importPrefixByFile)
              .put(file, importPrefix)
              .build();

      ImmutableMap<String, String> packageByPrefix =
          ImmutableMap.<String, String>builder()
              .putAll(packageNameByImportPrefix)
              .put(importPrefix, packageName)
              .build();

      ImmutableMap<String, ImmutableMap<String, Symbol>> symbolsByPackage =
          ImmutableMap.<String, ImmutableMap<String, Symbol>>builder()
              .putAll(symbolsByPackageName)
              .put(
                  packageName,
                  Maps.uniqueIndex(
                      Iterables.transform(definitions, d -> new Symbol(packageName, d)),
                      s -> s.getClassName().simpleName()))
              .build();

      return new SymbolTable(prefixByFile, packageByPrefix, symbolsByPackage);
    }

    @Override
    public String toString() {
      return "SymbolTable{" +
          "packageNameByImportPrefix=" + packageNameByImportPrefix +
          ", symbolsByPackageName=" + symbolsByPackageName +
          '}';
    }
  }

  private SymbolTable processThriftFiles(
      SymbolTable symbolTable,
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
        symbolTable = symbolTable.updated(thriftFile, packageName, document.getDefinitions());
        Set<File> includes =
            document.getHeader().getIncludes()
                .stream()
                .map(inc -> new File(thriftFile.getParentFile(), inc).getAbsoluteFile())
                .filter(f -> !processed.contains(f))
                .collect(Collectors.toSet());
        symbolTable = processThriftFiles(symbolTable, includes, outdir, true);

        if (packageSuffix.isPresent()) {
          packageName = packageName + packageSuffix.get();
        }

        ThriftGenVisitor visitor =
            new ThriftGenVisitor(
                getLogger(),
                outdir,
                symbolTable,
                packageName);
        document.visit(visitor);
        visitor.finish();
        processed.add(thriftFile);
      }
    }
    return symbolTable;
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
        SymbolTable symbolTable,
        String packageName) {

      ThriftEntityInterfaceFactory thriftEntityInterfaceFactory =
          new ThriftEntityInterfaceFactory(logger, outdir);
      visitors =
          ImmutableMap.<Class<? extends Visitable>, Visitor<? extends Visitable>>builder()
              .put(Const.class,
                  new ConstVisitor(logger, outdir, symbolTable, packageName))
              .put(IntegerEnum.class,
                  new IntegerEnumVisitor(logger, outdir, symbolTable, packageName))
              .put(Service.class,
                  new ServiceVisior(logger, outdir, symbolTable, packageName))
              // Not needed by Aurora and of questionable value to ever add support for.
              .put(StringEnum.class,
                  Visitor.failing("The Senum type is deprecated and removed in thrift 1.0.0, " +
                      "see: https://issues.apache.org/jira/browse/THRIFT-2003"))
              .put(Struct.class,
                  new StructVisitor(
                      thriftEntityInterfaceFactory,
                      logger,
                      outdir,
                      symbolTable,
                      packageName))
              // Currently not used by Aurora, but trivial to support.
              .put(ThriftException.class, Visitor.failing())
              .put(TypeAnnotation.class,
                  new TypeAnnotationVisitor(logger, outdir, symbolTable, packageName))
              // TODO(John Sirois): Implement as the need arises.
              // Currently not needed by Aurora; requires deferring all generation to `finish` and
              // collecting a full symbol table + adding a resolve method to resolve through
              // typedefs.
              .put(Typedef.class, Visitor.failing())
              .put(Union.class,
                  new UnionVisitor(
                      thriftEntityInterfaceFactory,
                      logger,
                      outdir,
                      symbolTable,
                      packageName))
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
    codeBlockBuilder.indent();
    codeBuilder.run();
    codeBlockBuilder.unindent();
  }

  interface LiteralFactory {
    CodeBlock create(ThriftType type, ConstValue value);
  }

  private static String getterName(ThriftField field) {
    String upperCamelCaseFieldName = toUpperCamelCaseName(field);
    ThriftType type = field.getType();
    if (type instanceof BaseType && ((BaseType) type).getType() == BaseType.Type.BOOL) {
      return "is" + upperCamelCaseFieldName;
    } else {
      return "get" + upperCamelCaseFieldName;
    }
  }

  private static String setterName(ThriftField field) {
    return "set" + toUpperCamelCaseName(field);
  }

  private static String isSetName(ThriftField field) {
    return "isSet" + toUpperCamelCaseName(field);
  }

  private static String toUpperCamelCaseName(ThriftField field) {
    return CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, field.getName());
  }

  private static String toUpperSnakeCaseName(ThriftField field) {
    return CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, field.getName());
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
              codeBuilder.add("\n.$L(", setterName(field));
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
          throw new ParseException(
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
      return new Visitor<Visitable>() {
        @Override public void visit(Visitable visitable) throws IOException {
          String msg = String.format("Unsupported thrift IDL type: %s", visitable);
          if (reason.isPresent()) {
            msg = String.format("%s%n%s", msg, reason.get());
          }
          throw new UnsupportedFeatureException(msg);
        }
      };
    }

    default void visit(T visitable) throws IOException {
      // noop
    }

    default void finish(ImmutableMap<String, AbstractStructRenderer> structRenderers)
        throws IOException {
      // noop
    }
  }

  static class BaseEmitter {
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

    private static final String AURORA_THRIFT_PACKAGE_NAME = "org.apache.aurora.thrift";

    private final Logger logger;
    private final File outdir;

    public BaseEmitter(Logger logger, File outdir) {
      this.logger = logger;
      this.outdir = requireNonNull(outdir);
    }

    protected final Logger getLogger() {
      return logger;
    }

    protected final File getOutdir() {
      return outdir;
    }

    protected final TypeSpec writeType(String packageName, TypeSpec.Builder typeBuilder)
        throws IOException {

      TypeSpec type =
          typeBuilder.addAnnotation(
              AnnotationSpec.builder(Generated.class)
                  .addMember("value", "$S", getClass().getName())
                  .build())
              .build();

      JavaFile javaFile =
          JavaFile.builder(packageName, type)
              .addFileComment(APACHE_LICENSE)
              .indent("  ")
              .skipJavaLangImports(true)
              .build();
      javaFile.writeTo(getOutdir());
      getLogger().info("Wrote {} to {}", type.name, getOutdir());
      return type;
    }
  }

  static abstract class BaseVisitor<T extends Visitable> extends BaseEmitter implements Visitor<T> {

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
        return parameterizedTypeName(ImmutableMap.class, keyType, valueType);
      } else if (thriftType instanceof ListType) {
        ThriftType elementType = ((ListType) thriftType).getElementType();
        return parameterizedTypeName(ImmutableList.class, elementType);
      } else if (thriftType instanceof SetType) {
        ThriftType elementType = ((SetType) thriftType).getElementType();
        return parameterizedTypeName(ImmutableSet.class, elementType);
      }
      throw new UnexpectedTypeException("Unknown thrift type: " + thriftType);
    }

    protected final ParameterizedTypeName parameterizedTypeName(
        Class<?> type,
        ThriftType... parameters) {

      return ParameterizedTypeName.get(ClassName.get(type),
          Stream.of(parameters).map(p -> typeName(p).box()).toArray(TypeName[]::new));
    }

    protected final CodeBlock renderValue(
        ImmutableMap<String, AbstractStructRenderer> structRenderers,
        ThriftType type,
        ConstValue value) {

      CodeBlock.Builder codeBuilder = CodeBlock.builder();
      if (value instanceof ConstInteger || value instanceof ConstDouble) {
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

      ClassName fieldsClassName = getClassName(struct.getName(), "_Fields");
      TypeSpec.Builder thriftFieldsEnumBuilder =
          // TODO(John Sirois): Rename this (striking _) after transitioning to new thrift gen.
          TypeSpec.enumBuilder("_Fields")
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

  @NotThreadSafe
  static class ThriftEntityInterfaceFactory extends BaseEmitter {
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
              BaseEmitter.AURORA_THRIFT_PACKAGE_NAME,
              thriftEntitySimpleName);

      ClassName thriftStructClassName =
          ClassName.get(
              BaseEmitter.AURORA_THRIFT_PACKAGE_NAME,
              thriftEntitySimpleName,
              thriftStructSimpleName);

      ClassName thriftUnionClassName =
          ClassName.get(
              BaseEmitter.AURORA_THRIFT_PACKAGE_NAME,
              thriftEntitySimpleName,
              thriftUnionSimpleName);

      ClassName builderClassName =
          ClassName.get(
              BaseEmitter.AURORA_THRIFT_PACKAGE_NAME,
              thriftEntitySimpleName,
              thriftStructSimpleName,
              builderSimpleName);

      ClassName thriftFieldsClassName =
          ClassName.get(
              BaseEmitter.AURORA_THRIFT_PACKAGE_NAME,
              thriftEntitySimpleName,
              thriftFieldsSimpleName);

      ClassName noFieldsClassName =
          ClassName.get(
              BaseEmitter.AURORA_THRIFT_PACKAGE_NAME,
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

      writeType(BaseEmitter.AURORA_THRIFT_PACKAGE_NAME, entityInterfaceBuilder);

      return new EntityInterface(
          thriftEntityClassName,
          thriftStructClassName,
          thriftUnionClassName,
          thriftFieldsClassName,
          noFieldsClassName,
          builderClassName);
    }
  }

  @NotThreadSafe
  static class ConstVisitor extends BaseVisitor<Const> {
    private final ImmutableList.Builder<Const> consts = ImmutableList.builder();

    ConstVisitor(Logger logger, File outdir, SymbolTable symbolTable, String packageName) {
      super(logger, outdir, symbolTable, packageName);
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
                .initializer(renderValue(structRenderers, fieldType, constant.getValue()))
                .build());
      }
      writeType(typeBuilder);
    }
  }

  static class IntegerEnumVisitor extends BaseVisitor<IntegerEnum> {
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

  @NotThreadSafe
  static class ServiceVisior extends BaseVisitor<Service> {
    private static final ParameterizedTypeName METHODS_MAP_TYPE =
        ParameterizedTypeName.get(
        ClassName.get(ImmutableMap.class),
        ClassName.get(String.class),
        ParameterizedTypeName.get(
            ClassName.get(ImmutableMap.class),
            ClassName.get(String.class),
            ClassName.get(Type.class)));

    private TypeName thriftServiceInterface;

    ServiceVisior(Logger logger, File outdir, SymbolTable symbolTable, String packageName) {
      super(logger, outdir, symbolTable, packageName);
    }

    @Override
    public void visit(Service service) throws IOException {
      TypeSpec.Builder serviceContainerBuilder =
          TypeSpec.interfaceBuilder(service.getName())
              .addModifiers(Modifier.PUBLIC);

      CodeBlock.Builder methodsMapInitializerCode =
          CodeBlock.builder()
            .add(
                "$[$T.<$T, $T<$T, $T>>builder()",
                ImmutableMap.class,
                String.class,
                ImmutableMap.class,
                String.class,
                Type.class);

      TypeSpec.Builder asyncServiceBuilder = createServiceBuilder(service, "Async");
      TypeSpec.Builder syncServiceBuilder = createServiceBuilder(service, "Sync");

      Optional<String> parent = service.getParent();
      if (parent.isPresent()) {
        methodsMapInitializerCode.add("\n.putAll($T._METHODS)", getClassName(parent.get()));
        asyncServiceBuilder.addSuperinterface(getClassName(parent.get(), "Async"));
        syncServiceBuilder.addSuperinterface(getClassName(parent.get(), "Sync"));
      }

      for (ThriftMethod method : service.getMethods()) {
        methodsMapInitializerCode
            .add("\n.put($S,\n", method.getName())
            .indent()
            .indent()
            .add(renderParameterMapInitializer(method))
            .unindent()
            .unindent()
            .add(")");

        asyncServiceBuilder.addMethod(
            renderMethod(
                method, parameterizedTypeName(ListenableFuture.class, method.getReturnType())));
        syncServiceBuilder.addMethod(renderMethod(method, typeName(method.getReturnType())));
      }

      CodeBlock methodMapInitializer =
          methodsMapInitializerCode
              .add("\n.build()$]")
              .build();

      FieldSpec methodsField =
          FieldSpec.builder(METHODS_MAP_TYPE, "_METHODS")
              .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
              .initializer(methodMapInitializer)
              .build();
      serviceContainerBuilder.addField(methodsField);

      MethodSpec thriftMethods =
          MethodSpec.methodBuilder("thriftMethods")
              .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
              .returns(METHODS_MAP_TYPE)
              .addStatement("return $N", methodsField)
              .build();

      MethodSpec getThriftMethods =
          MethodSpec.methodBuilder("getThriftMethods")
              .addAnnotation(Override.class)
              .addModifiers(Modifier.PUBLIC, Modifier.DEFAULT)
              .returns(METHODS_MAP_TYPE)
              .addStatement("return $N()", thriftMethods)
              .build();

      asyncServiceBuilder.addMethod(thriftMethods);
      asyncServiceBuilder.addMethod(getThriftMethods);
      serviceContainerBuilder.addType(asyncServiceBuilder.build());

      syncServiceBuilder.addMethod(thriftMethods);
      syncServiceBuilder.addMethod(getThriftMethods);
      serviceContainerBuilder.addType(syncServiceBuilder.build());

      writeType(serviceContainerBuilder);
    }

    private CodeBlock renderParameterMapInitializer(ThriftMethod method) {
      if (method.getArguments().isEmpty()) {
        return CodeBlock.builder().add("$T.of()", ImmutableMap.class).build();
      }

      CodeBlock.Builder parameterMapInitializerCode =
          CodeBlock.builder()
              .add("$T.<$T, $T>builder()", ImmutableMap.class, String.class, Type.class)
              .indent()
              .indent();

      for (ThriftField field : method.getArguments()) {
        TypeName fieldType = typeName(field.getType());
        if (fieldType instanceof ParameterizedTypeName) {
          ParameterizedTypeName typeToken =
              ParameterizedTypeName.get(ClassName.get(TypeToken.class), fieldType);
          parameterMapInitializerCode.add(
              "\n.put($S, new $T() {}.getType())", field.getName(), typeToken);
        } else {
          parameterMapInitializerCode.add("\n.put($S, $T.class)", field.getName(), fieldType);
        }
      }

      return parameterMapInitializerCode
          .add("\n.build()")
          .unindent()
          .unindent()
          .build();
    }

    private TypeSpec.Builder createServiceBuilder(Service service, String typeName)
        throws IOException {

      return TypeSpec.interfaceBuilder(typeName)
          .addAnnotation(
              AnnotationSpec.builder(com.facebook.swift.service.ThriftService.class)
                  .addMember("value", "$S", service.getName())
                  .build())
          .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
          .addSuperinterface(getThriftServiceInterface())
          .addMethod(MethodSpec.methodBuilder("close")
              .addAnnotation(Override.class)
              .addModifiers(Modifier.PUBLIC, Modifier.DEFAULT)
              .build());
    }


    private TypeName getThriftServiceInterface() throws IOException {
      if (thriftServiceInterface == null) {
        thriftServiceInterface = createThriftServiceInterface();
      }
      return thriftServiceInterface;
    }

    private TypeName createThriftServiceInterface() throws IOException {
      TypeSpec.Builder thriftService =
          TypeSpec.interfaceBuilder("ThriftService")
              .addModifiers(Modifier.PUBLIC)
              .addSuperinterface(AutoCloseable.class)
              .addMethod(
                  MethodSpec.methodBuilder("getThriftMethods")
                      .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                      .returns(METHODS_MAP_TYPE)
                      .build());
      writeType(BaseEmitter.AURORA_THRIFT_PACKAGE_NAME, thriftService);
      return ClassName.get(BaseEmitter.AURORA_THRIFT_PACKAGE_NAME, "ThriftService");
    }

    private MethodSpec renderMethod(ThriftMethod method, TypeName returnType) {
      if (!method.getThrowsFields().isEmpty()) {
        throw new UnsupportedFeatureException("Service methods that declare exceptions are not " +
            "supported, given " + method);
      }

      MethodSpec.Builder methodBuilder =
          MethodSpec.methodBuilder(method.getName())
              .addAnnotation(
                  AnnotationSpec.builder(com.facebook.swift.service.ThriftMethod.class)
                      .addMember("value", "$S", method.getName())
                      .addMember("oneway", "$L", method.isOneway())
                      .build())
              .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
              .returns(returnType);

      for (ThriftField field : method.getArguments()) {
        ParameterSpec.Builder paramBuilder =
            ParameterSpec.builder(typeName(field.getType()), field.getName())
                .addAnnotation(renderThriftFieldAnnotation(field));
        if (!field.getAnnotations().isEmpty()) {
          paramBuilder.addAnnotation(
              TypeAnnotationVisitor.createAnnotation(field.getAnnotations()));
        }
        if (field.getRequiredness() != ThriftField.Requiredness.REQUIRED) {
          paramBuilder.addAnnotation(javax.annotation.Nullable.class);
        }
        methodBuilder.addParameter(paramBuilder.build());
      }

      return methodBuilder.build();
    }
  }

  @NotThreadSafe
  static class StructVisitor extends BaseVisitor<Struct> {
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
              .addSuperinterface(builderBuilderName)
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
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(fieldParam)
                .returns(boolean.class)
                .addStatement("throw new $T()", IllegalAccessError.class)
                .build());

        typeBuilder.addMethod(
            MethodSpec.methodBuilder("getFieldValue")
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
        Optional<CodeBlock> unsetValue = renderZero(type);
        boolean nullable =
            (field.getRequiredness() != ThriftField.Requiredness.REQUIRED)
            && !field.getValue().isPresent()
            && !(type instanceof ContainerType);

        MethodSpec.Builder accessorBuilder =
            MethodSpec.methodBuilder(getterName(field))
                .addAnnotation(renderThriftFieldAnnotation(field))
                .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                .returns(typeName(type));
        if (nullable && !unsetValue.isPresent()) {
          accessorBuilder.addAnnotation(javax.annotation.Nullable.class);
        }
        MethodSpec accessor = accessorBuilder.build();
        typeBuilder.addMethod(accessor);

        String fieldsValueName = toUpperSnakeCaseName(field);
        if (nullable && !unsetValue.isPresent()) {
          MethodSpec isSetFieldMethod =
              MethodSpec.methodBuilder(isSetName(field))
                  .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                  .returns(TypeName.BOOLEAN)
                  .addStatement("return $N() != null", accessor)
                  .build();
          typeBuilder.addMethod(isSetFieldMethod);

          isSetCode.get().addStatement("case $L: return $N()", fieldsValueName, isSetFieldMethod);
        } else {
          isSetCode.get().addStatement("case $L: return true", fieldsValueName);
        }
        getFieldValueCode.get().addStatement("case $L: return $N()", fieldsValueName, accessor);
        builderSetCode.get()
            .add("case $L:\n", fieldsValueName)
            .indent()
            .addStatement("$N(($T) value)", setterName(field), typeName(type))
            .addStatement("break")
            .unindent();

        ParameterSpec.Builder paramBuilder = ParameterSpec.builder(typeName(type), field.getName());
        if (nullable && !unsetValue.isPresent()) {
          paramBuilder.addAnnotation(javax.annotation.Nullable.class);
        }
        ParameterSpec parameterSpec = paramBuilder.build();

        String setterName = setterName(field);
        MethodSpec methodSpec =
            MethodSpec.methodBuilder(setterName)
                .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                .addParameter(parameterSpec)
                .returns(builderBuilderName)
                .build();
        builderBuilder.addMethod(methodSpec);

        ImmutableList.Builder<AnnotationSpec> annotations = ImmutableList.builder();
        annotations.add(AnnotationSpec.builder(Override.class).build());
        if (!(type instanceof ContainerType)) {
          annotations.add(renderThriftFieldAnnotation(field));
        }
        MethodSpec wrapperMethodSpec =
            MethodSpec.methodBuilder(setterName)
                .addAnnotations(annotations.build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(parameterSpec)
                .returns(wrapperBuilderName)
                .addStatement("this.builder.$N($N)", methodSpec, parameterSpec)
                .addStatement("return this")
                .build();
        wrapperBuilder.addMethod(wrapperMethodSpec);

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
              .add("\n.$N(", wrapperMethodSpec)
              .add(defaultValue)
              .add(")");
        } else if (unsetValue.isPresent()) {
          wrapperConstructorBuilder
              .add("\n.$N(", wrapperMethodSpec)
              .add(unsetValue.get())
              .add(")");
        }

        // TODO(John Sirois): This signature, skipping OPTIONALs, is tailored to match apache
        // thrift: reconsider convenience factory methods after transition.
        if (field.getRequiredness() != ThriftField.Requiredness.OPTIONAL) {
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
            constructorParamType = typeName(type);
          }
          ParameterSpec.Builder constructorParam =
              ParameterSpec.builder(constructorParamType, field.getName());
          if (nullable && !unsetValue.isPresent()) {
            constructorParam.addAnnotation(javax.annotation.Nullable.class);
          }
          ParameterSpec param = constructorParam.build();
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

      Optional<PeerInfo> peerInfo = PeerInfo.from(getPackageName(), struct);
      if (peerInfo.isPresent()) {
        PeerInfo mutablePeer = peerInfo.get();
        if (mutablePeer.render) {
          TypeSpec.Builder peerType =
              createMutablePeer(struct, typeSpec, mutablePeer);
          writeType(mutablePeer.packageName, peerType);
        }
      }
    }

    static class PeerInfo {
      static Optional<PeerInfo> from(String packageName, AbstractStruct struct) {
        return FluentIterable.from(struct.getAnnotations())
            .filter(a -> "mutablePeer".equals(a.getName()))
            .transform(TypeAnnotation::getValue)
            .first()
            .transform(value -> new PeerInfo(packageName, struct, value));
      }

      final boolean render;
      final String packageName;
      final String className;

      private PeerInfo(String structPackageName, AbstractStruct struct, String mutablePeerValue) {
        render = Boolean.parseBoolean(mutablePeerValue);
        if (render) {
          packageName = structPackageName + ".peer";
          className = "Mutable" + struct.getName();
        } else {
          int i = mutablePeerValue.lastIndexOf('.');
          if (i == -1) {
            packageName = "";
            className = mutablePeerValue;
          } else {
            packageName = mutablePeerValue.substring(0, i);
            className = mutablePeerValue.substring(i + 1);
          }
        }
      }
    }

    private TypeSpec.Builder createMutablePeer(Struct struct, TypeSpec typeSpec, PeerInfo peerInfo) {
      TypeSpec.Builder typeBuilder =
          TypeSpec.classBuilder(peerInfo.className)
              .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
              .addMethod(MethodSpec.constructorBuilder().addModifiers(Modifier.PRIVATE).build());

      CodeBlock.Builder toThriftCode =
          CodeBlock.builder()
              .add("$[return $N.builder()", typeSpec);

      for (ThriftField field : struct.getFields()) {
        // TODO(John Sirois): XXX here we only want the mutable peer if the identifier type does not
        // resolve to an enum ...
        // + this implies the need to operate off of a symbol table - type identifier to ThriftType
        // + this further implies just labeling storage roots and then walking and creating peers
        // + OR just creating peers for everything (minus enums and structs).

        // TODO(John Sirois): XXX the structTable needs to map identifier to AbstractStruct
        // I need to be able to say:
        // getIdentifiedType(
        //     callerPackage=getPackageName(),
        //     identifier=((Identifier) field.getType()))
        ThriftType fieldType = field.getType();
        FieldSpec fieldSpec =
            FieldSpec.builder(typeName(fieldType), field.getName())
                .addModifiers(Modifier.PRIVATE)
                .build();
        CodeBlock code = CodeBlock.builder().add("$N", fieldSpec).build();

        toThriftCode.add("\n.$L(", setterName(field));
        if (fieldType instanceof IdentifierType) {
          SymbolTable.Symbol symbol = lookup(((IdentifierType) fieldType));
          if (symbol.getSymbol() instanceof AbstractStruct) {
            Optional<PeerInfo> peer =
                PeerInfo.from(symbol.packageName, (AbstractStruct) symbol.getSymbol());
            if (peer.isPresent()) {
              ClassName peerType = ClassName.get(peer.get().packageName, peer.get().className);
              fieldSpec =
                  FieldSpec.builder(peerType, field.getName())
                      .addModifiers(Modifier.PRIVATE)
                      .build();
              code =
                  CodeBlock.builder()
                      .add("$N == null ? null : $N.toThrift()", fieldSpec, fieldSpec)
                      .build();
            }
          }
        } else if (fieldType instanceof ListType) {
          ThriftType elementType = ((ListType) fieldType).getElementType();
          if (elementType instanceof IdentifierType) {
            SymbolTable.Symbol symbol = lookup(((IdentifierType) elementType));
            if (symbol.getSymbol() instanceof AbstractStruct) {
              Optional<PeerInfo> peer =
                  PeerInfo.from(symbol.packageName, (AbstractStruct) symbol.getSymbol());
              if (peer.isPresent()) {
                ClassName peerType = ClassName.get(peer.get().packageName, peer.get().className);
                ParameterizedTypeName listType =
                    ParameterizedTypeName.get(ClassName.get(List.class), peerType);
                fieldSpec =
                    FieldSpec.builder(listType, field.getName())
                        .addModifiers(Modifier.PRIVATE)
                        .build();
                code =
                    CodeBlock.builder()
                        .add(
                            "$N.stream().map($T::toThrift).collect($T.toList())",
                            fieldSpec,
                            peerType,
                            Collectors.class)
                        .build();
              }
            }
          } else {
            ParameterizedTypeName listType =
                ParameterizedTypeName.get(ClassName.get(List.class), typeName(elementType));
            fieldSpec =
                FieldSpec.builder(listType, field.getName())
                    .addModifiers(Modifier.PRIVATE)
                    .build();
          }
        } else if (fieldType instanceof SetType) {
          ThriftType elementType = ((SetType) fieldType).getElementType();
          if (elementType instanceof IdentifierType) {
            SymbolTable.Symbol symbol = lookup(((IdentifierType) elementType));
            if (symbol.getSymbol() instanceof AbstractStruct) {
              Optional<PeerInfo> peer =
                  PeerInfo.from(symbol.packageName, (AbstractStruct) symbol.getSymbol());
              if (peer.isPresent()) {
                ClassName peerType = ClassName.get(peer.get().packageName, peer.get().className);
                ParameterizedTypeName setType =
                    ParameterizedTypeName.get(ClassName.get(Set.class), peerType);
                fieldSpec =
                    FieldSpec.builder(setType, field.getName())
                        .addModifiers(Modifier.PRIVATE)
                        .build();
                code =
                    CodeBlock.builder()
                        .add(
                            "$N.stream().map($T::toThrift).collect($T.toSet())",
                            fieldSpec,
                            peerType,
                            Collectors.class)
                        .build();
              }
            }
          } else {
            ParameterizedTypeName listType =
                ParameterizedTypeName.get(ClassName.get(Set.class), typeName(elementType));
            fieldSpec =
                FieldSpec.builder(listType, field.getName())
                    .addModifiers(Modifier.PRIVATE)
                    .build();
          }
        } // TODO(John Sirois): XXX Handle maps

        typeBuilder.addField(fieldSpec);
        toThriftCode.add(code);
        toThriftCode.add(")");
      }

      typeBuilder.addMethod(
          MethodSpec.methodBuilder("toThrift")
              .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
              .returns(getClassName(typeSpec.name))
              .addCode(
                  toThriftCode
                      .add("\n.build();\n$]")
                      .build())
              .build());

      return typeBuilder;
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

  static class TypeAnnotationVisitor extends BaseVisitor<TypeAnnotation> {
    private static final ClassName ANNOTATION_CLASS =
        ClassName.get(BaseEmitter.AURORA_THRIFT_PACKAGE_NAME, "Annotation");

    private static final ClassName PARAMETER_CLASS =
        ClassName.get(BaseEmitter.AURORA_THRIFT_PACKAGE_NAME, "Annotation", "Parameter");

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

  static class UnionVisitor extends BaseVisitor<Union> {
    private final ThriftEntityInterfaceFactory thriftEntityInterfaceFactory;

    UnionVisitor(
        ThriftEntityInterfaceFactory thriftEntityInterfaceFactory,
        Logger logger,
        File outdir,
        SymbolTable symbolTable,
        String packageName) {

      super(logger, outdir, symbolTable, packageName);
      this.thriftEntityInterfaceFactory = thriftEntityInterfaceFactory;
    }

    @Override
    public void visit(Union union) throws IOException {
      ThriftEntityInterfaceFactory.EntityInterface entityInterface =
          thriftEntityInterfaceFactory.getEntityInterface();

      ClassName localFieldsTypeName = getClassName(union.getName(), "_Fields");
      TypeSpec.Builder typeBuilder =
          TypeSpec.classBuilder(union.getName())
              .addAnnotation(
                  AnnotationSpec.builder(com.facebook.swift.codec.ThriftUnion.class)
                      .addMember("value", "$S", union.getName())
                      .build())
              .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
              .addSuperinterface(
                  ParameterizedTypeName.get(entityInterface.unionTypeName, localFieldsTypeName));

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

        typeBuilder.addMethod(
            MethodSpec.constructorBuilder()
                .addAnnotation(com.facebook.swift.codec.ThriftConstructor.class)
                .addModifiers(Modifier.PUBLIC)
                .addParameter(fieldTypeName, field.getName())
                .addStatement("this.value = $T.requireNonNull($L)", Objects.class, field.getName())
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
                        .addStatement("return ($T) value", fieldTypeName)
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

      Optional<ClassName> fieldsEnumClassName =
          maybeAddFieldsEnum(typeBuilder, union, entityInterface.fieldsTypeName);
      if (fieldsEnumClassName.isPresent()) {
        ClassName fieldsEnumClass = fieldsEnumClassName.get();
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
      }

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
}
