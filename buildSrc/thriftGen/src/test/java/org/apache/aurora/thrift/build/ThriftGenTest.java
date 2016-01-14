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
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;

import com.facebook.swift.codec.ThriftCodecManager;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.jimfs.Jimfs;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.ListenableFuture;
import com.sun.tools.javac.nio.JavacPathFileManager;
import com.sun.tools.javac.util.Context;

import org.apache.aurora.thrift.ImmutableParameter;
import org.apache.aurora.thrift.ImmutableThriftAnnotation;
import org.apache.aurora.thrift.ThriftAnnotation;
import org.apache.aurora.thrift.ThriftEntity;
import org.apache.aurora.thrift.ThriftFields;
import org.apache.aurora.thrift.ThriftService;
import org.apache.aurora.thrift.ThriftStruct;
import org.apache.aurora.thrift.ThriftUnion;
import org.apache.thrift.TEnum;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TTransport;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ThriftGenTest {
  private FileSystem fileSystem;
  private Path outdir;
  private ThriftGen thriftGen;

  private Path classes;
  private ClassLoader classLoader;
  private JavaFileManager fileManager;

  private ThriftCodecManager codecManager;

  @Before
  public void setUp() throws IOException {
    fileSystem = Jimfs.newFileSystem();
    outdir = Files.createDirectory(fileSystem.getPath("out"));
    thriftGen = new ThriftGen(outdir, LoggerFactory.getLogger(getClass()));

    classes = Files.createDirectory(fileSystem.getPath("classes"));
    classLoader = new ClassLoader() {
      @Override protected Class<?> findClass(String name) throws ClassNotFoundException {
        try {
          byte[] bytes = Files.readAllBytes(classes.resolve(name.replace('.', '/') + ".class"));
          return defineClass(name, bytes, 0, bytes.length);
        } catch (IOException e) {
          throw new ClassNotFoundException("Problem loading class " + name, e);
        }
      }
    };

    JavacPathFileManager fileManager =
        new JavacPathFileManager(new Context(), false, Charsets.UTF_8);
    fileManager.setDefaultFileSystem(fileSystem);
    fileManager.setLocation(StandardLocation.SOURCE_PATH, ImmutableList.of(outdir));
    fileManager.setLocation(StandardLocation.CLASS_OUTPUT, ImmutableList.of(classes));
    this.fileManager = fileManager;

    codecManager = new ThriftCodecManager(classLoader);
  }

  private Path outdirPath(String... pathComponents) {
    Path current = outdir;
    for (String pathComponent : pathComponents) {
      current = current.resolve(pathComponent);
    }
    return current;
  }

  private static String textBlock(String... lines) {
    return Joiner.on(System.lineSeparator()).join(lines);
  }

  private static void write(Path file, String... lines) throws IOException {
    if (file.getParent() != null) {
      Files.createDirectories(file.getParent());
    }
    Files.write(
        file,
        textBlock(lines).getBytes(Charsets.UTF_8),
        StandardOpenOption.CREATE_NEW,
        StandardOpenOption.WRITE);
  }

  private void generateThrift(Path...thriftFiles) throws IOException {
    thriftGen.generate(ImmutableSet.copyOf(thriftFiles));
  }

  private void generateThrift(String... lines) throws IOException {
    Path thriftFile = fileSystem.getPath("test.thrift");
    write(thriftFile, lines);
    generateThrift(thriftFile);
  }

  private void assertOutdirFiles(Path... paths) throws IOException {
    assertEquals(
        ImmutableSet.copyOf(paths),
        Files.walk(outdir).filter(Files::isRegularFile).collect(Collectors.toSet()));
  }

  @Test
  public void testNoJavaNamespace() throws Exception {
    generateThrift("namespace py test");

    assertOutdirFiles();
  }

  private Class<?> loadClass(String className) throws ClassNotFoundException {
    return Class.forName(className, true /* initialize */, classLoader);
  }

  private JavaFileObject getSourceCode(String className) {
    try {
      return fileManager.getJavaFileForInput(
          StandardLocation.SOURCE_PATH,
          className,
          JavaFileObject.Kind.SOURCE);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Class<?> compileClass(String className, String... additionalClasses)
      throws IOException, ClassNotFoundException {

    Iterable<JavaFileObject> javaFiles =
        FluentIterable.from(Lists.asList(className, additionalClasses))
            .transform(this::getSourceCode);

    JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler();
    JavaCompiler.CompilationTask task =
        javaCompiler.getTask(
            new PrintWriter(System.err), /* out: diagnostic stream for javac */
            fileManager,
            null /* DiagnosticListener: default (prints to writer above) */,
            // We suppress warnings about un-processed annotations but otherwise fail any warnings.
            ImmutableList.of("-implicit:class", "-Xlint:all,-processing", "-Werror"),
            null /* apt classes: use META-INF discovery mechanism */,
            javaFiles);
    boolean success = task.call();
    assertTrue(success);
    return loadClass(className);
  }

  private static <T> Class<? extends T> assertAssignableFrom(Class<T> target, Class<?> clazz) {
    assertTrue(target.isAssignableFrom(clazz));
    // We tested this was assignable to above.
    @SuppressWarnings("unchecked")
    Class<T> targeted = (Class<T>) clazz;
    return targeted;
  }

  private static Class<? extends Enum> assertEnumClass(Class<?> clazz) {
    return assertAssignableFrom(Enum.class, clazz);
  }

  private static Enum assertEnum(Class<? extends Enum> enumClass, String name, int value) {
    Enum enumInstance = Enum.valueOf(enumClass, name);
    assertTrue(enumInstance instanceof TEnum);
    assertEquals(value, ((TEnum) enumInstance).getValue());
    return enumInstance;
  }

  @Test
  public void testEnum() throws Exception {
    generateThrift(
        "namespace java test",
        "",
        "enum ResponseCode {",
        "  OK = 0,",
        "  ERROR = 2",
        "}");
    assertOutdirFiles(outdirPath("test", "ResponseCode.java"));

    Class<? extends Enum> enumClass = assertEnumClass(compileClass("test.ResponseCode"));
    Enum ok = assertEnum(enumClass, "OK", 0);
    Enum error = assertEnum(enumClass, "ERROR", 2);
    assertEquals(EnumSet.of(ok, error), EnumSet.allOf(enumClass));
  }

  private static void assertConstantValue(Field field, Class<?> type, Object value)
      throws IllegalAccessException {

    assertEquals(type, field.getType());
    assertTrue(Modifier.isStatic(field.getModifiers()));
    assertEquals(value, field.get(null));
  }

  @Test
  public void testConstant() throws Exception {
    generateThrift(
        "namespace java test",
        "",
        "const i32 MEANING_OF_LIFE = 42",
        "const string REGEX = '[Jj]ake'",
        "const set<string> TAGS = ['A', 'B']",
        "const list<bool> BITS = [0, 1]",
        "const map<string, bool> COLORS = {'reddish': 1, 'bluish': 0}");
    assertOutdirFiles(outdirPath("test", "Constants.java"));

    Class<?> clazz = compileClass("test.Constants");

    Field meaningOfLife = clazz.getField("MEANING_OF_LIFE");
    assertConstantValue(meaningOfLife, int.class, 42);

    Field regex = clazz.getField("REGEX");
    assertConstantValue(regex, String.class, "[Jj]ake");

    Field tags = clazz.getField("TAGS");
    assertConstantValue(tags, ImmutableSet.class, ImmutableSet.of("A", "B"));

    Field bits = clazz.getField("BITS");
    assertConstantValue(bits, ImmutableList.class, ImmutableList.of(Boolean.TRUE, Boolean.FALSE));

    Field colors = clazz.getField("COLORS");
    assertConstantValue(
        colors,
        ImmutableMap.class, ImmutableMap.of("reddish", Boolean.FALSE, "bluish", Boolean.TRUE));

    assertEquals(
        ImmutableSet.of(meaningOfLife, regex, tags, bits, colors),
        ImmutableSet.copyOf(clazz.getFields()));
  }

  private static Class<? extends ThriftStruct> assertStructClass(Class<?> clazz) {
    return assertAssignableFrom(ThriftStruct.class, clazz);
  }

  private Class<? extends ThriftStruct> compileStructClass(
      String className,
      String... additionalClasses)
      throws IOException, ClassNotFoundException {

    Class<?> clazz = compileClass(className, additionalClasses);
    return assertStructClass(clazz);
  }

  private <T extends ThriftEntity<?>> void assertSerializationRoundTrip(
      Class<? extends T> entityType,
      T entity)
      throws Exception {

    // Needed to work around need for an exact type in the underlying CodecManager.getCodec.
    @SuppressWarnings("unchecked")
    Class<T> deWildcarded = (Class<T>) entityType;

    TTransport trans = new TMemoryBuffer(1024 /* initial size, the buffer grows as needed */);
    TProtocol protocol = new TBinaryProtocol(trans);

    codecManager.write(deWildcarded, entity, protocol);
    T reconstituted = codecManager.read(entityType, protocol);

    assertEquals(entity, reconstituted);
  }

  @Test
  public void testStructNoFields() throws Exception {
    generateThrift(
        "namespace java test",
        "",
        "struct NoFields {}");
    assertOutdirFiles(outdirPath("test", "NoFields.java"));
    Class<? extends ThriftStruct> structClass = compileStructClass("test.NoFields");

    ImmutableSet<ThriftFields> fields = ThriftEntity.fields(structClass);
    assertEquals(ImmutableSet.of(), fields);

    ThriftStruct<?> structInstance = ThriftStruct.builder(structClass).build();
    assertTrue(structClass.isInstance(structInstance));
    assertSerializationRoundTrip(structClass, structInstance);
  }

  private static ThriftStruct.Builder<ThriftFields, ? extends ThriftStruct> buildStruct(
      Class<? extends ThriftStruct> structClass,
      Map<ThriftFields, Object> fields) {

    ThriftStruct.Builder<ThriftFields, ? extends ThriftStruct> builder =
        ThriftStruct.builder(structClass);

    for (Map.Entry<ThriftFields, Object> entry : fields.entrySet()) {
      builder.set(entry.getKey(), entry.getValue());
    }
    return builder;
  }

  private ThriftStruct createStruct(
      Class<? extends ThriftStruct> structClass,
      Map<ThriftFields, Object> fields)
      throws Exception {

    ThriftStruct struct = buildStruct(structClass, fields).build();
    assertSerializationRoundTrip(structClass, struct);
    return struct;
  }

  private static void assertMissingFields(
      Class<? extends ThriftStruct> structClass,
      Map<ThriftFields, Object> fields) {

    ThriftStruct.Builder<ThriftFields, ? extends ThriftStruct> builder =
        buildStruct(structClass, fields);
    try {
      builder.build();
      fail();
    } catch (IllegalStateException e) {
      // expected
    }
  }

  private static void assertField(ThriftFields field, short id, Class<?> clazz, Type type) {
    assertEquals(clazz, field.getFieldClass());
    assertEquals(type, field.getFieldType());
    assertEquals(id, field.getThriftFieldId());
  }

  @SuppressWarnings({"raw", "unchecked"}) // Needed to to extract fields.
  private static void assertRaisesUnset(ThriftStruct struct, ThriftFields field) {
    try {
      struct.getFieldValue(field);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  @SuppressWarnings({"raw", "unchecked"}) // Needed to to extract fields.
  public void testStructStringField() throws Exception {
    generateThrift(
        "namespace java test",
        "",
        "struct Struct {",
        "  1: required string name",
        "  3: string address",
        "  5: optional string description = 'None'",
        "}");
    assertOutdirFiles(outdirPath("test", "Struct.java"));
    Class<? extends ThriftStruct> structClass = compileStructClass("test.Struct");

    assertMissingFields(structClass, ImmutableMap.of());

    ImmutableMap<String, ThriftFields> fieldsByName =
        indexFields(structClass, "name", "address", "description");

    ThriftFields nameField = fieldsByName.get("name");
    assertField(nameField, (short) 1, String.class, String.class);

    ThriftFields addressField = fieldsByName.get("address");
    assertField(addressField, (short) 3, String.class, String.class);

    ThriftFields descriptionField = fieldsByName.get("description");
    assertField(descriptionField, (short) 5, String.class, String.class);

    assertMissingFields(structClass, ImmutableMap.of(addressField, "a"));
    assertMissingFields(structClass, ImmutableMap.of(addressField, "a", descriptionField, "b"));

    ThriftStruct struct = createStruct(structClass, ImmutableMap.of(nameField, "Fred"));

    assertTrue(struct.isSet(nameField));
    assertEquals("Fred", struct.getFieldValue(nameField));
    assertFalse(struct.isSet(addressField));
    assertTrue(struct.isSet(descriptionField));
    assertEquals("None", struct.getFieldValue(descriptionField));

    struct =
        createStruct(
            structClass,
            ImmutableMap.of(nameField, "Joe", addressField, "Flatland", descriptionField, "fit"));

    assertTrue(struct.isSet(nameField));
    assertEquals("Joe", struct.getFieldValue(nameField));
    assertTrue(struct.isSet(addressField));
    assertEquals("Flatland", struct.getFieldValue(addressField));
    assertTrue(struct.isSet(descriptionField));
    assertEquals("fit", struct.getFieldValue(descriptionField));

    HashMap<ThriftFields, Object> fields = new HashMap<>();
    fields.put(nameField, "Bill");
    fields.put(addressField, null);
    struct = createStruct(structClass, fields);

    assertTrue(struct.isSet(nameField));
    assertEquals("Bill", struct.getFieldValue(nameField));
    assertFalse(struct.isSet(addressField));
    assertRaisesUnset(struct, addressField);
    assertTrue(struct.isSet(descriptionField));
    assertEquals("None", struct.getFieldValue(descriptionField));
  }

  private static ImmutableMap<String, ThriftFields> indexFields(
      Class<? extends ThriftEntity> structClass, String... expectedFieldNames) {

    ImmutableMap<String, ThriftFields> fieldsByName =
        Maps.uniqueIndex(ThriftEntity.fields(structClass), ThriftFields::getFieldName);
    assertEquals(ImmutableSet.copyOf(expectedFieldNames), fieldsByName.keySet());
    return fieldsByName;
  }

  @Test
  @SuppressWarnings({"raw", "unchecked"}) // Needed to to extract fields.
  public void testIncludes() throws Exception {
    Path includedFile = fileSystem.getPath("subdir", "included.thrift");
    write(
        includedFile,
        "namespace java test.subpackage",
        "",
        "const string NAME = 'George'",
        "",
        "enum States {",
        "  ON = 1",
        "  OFF = 2",
        "}");

    Path thriftFile = fileSystem.getPath("test.thrift");
    write(
        thriftFile,
        "namespace java test",
        "",
        "include 'subdir/included.thrift'",
        "",
        "struct Struct {",
        "  1: string name = included.NAME",
        "  2: included.States state = included.States.ON",
        "}");

    generateThrift(thriftFile);
    assertOutdirFiles(
        outdirPath("test", "Struct.java"),
        outdirPath("test", "subpackage", "Constants.java"),
        outdirPath("test", "subpackage", "States.java"));

    Class<? extends ThriftStruct> structClass =
        compileStructClass("test.Struct", "test.subpackage.States");
    ThriftStruct struct = ThriftStruct.builder(structClass).build();

    ImmutableMap<String, ThriftFields> fieldsByName = indexFields(structClass, "name", "state");
    assertEquals("George", struct.getFieldValue(fieldsByName.get("name")));
    Class<? extends Enum> enumClass = assertEnumClass(loadClass("test.subpackage.States"));
    Enum on = assertEnum(enumClass, "ON", 1);
    assertEquals(on, struct.getFieldValue(fieldsByName.get("state")));
  }

  @Test
  public void testStructAnnotations() throws Exception {
    generateThrift(
        "namespace java test",
        "",
        "struct AnnotatedStruct {",
        "} (",
        "  age=1,", // Bare ints should go to strings.
        "  doc='",
        "Multiline strings should work",
        "for annotation values making them ~natural for doc",
        "'",
        ")");
    assertOutdirFiles(outdirPath("test", "AnnotatedStruct.java"));
    Class<? extends ThriftStruct> structClass = compileStructClass("test.AnnotatedStruct");
    ThriftAnnotation annotation = structClass.getAnnotation(ThriftAnnotation.class);
    assertNotNull(annotation);

    assertEquals(
        ImmutableThriftAnnotation.of(new ImmutableParameter[] {
            ImmutableParameter.of("age", "1"),
            ImmutableParameter.of(
                "doc",
                textBlock(
                    "",
                    "Multiline strings should work",
                    "for annotation values making them ~natural for doc",
                    ""))}),
        annotation);
  }

  @Test
  @SuppressWarnings({"raw", "unchecked"}) // Needed to to extract fields.
  public void testStructLiteral() throws Exception {
    generateThrift(
        "namespace java test",
        "",
        "enum States {",
        "  NH = 1",
        "  MA = 2",
        "  NC = 3",
        "  WV = 4",
        "  MT = 5",
        "}",
        "",
        "struct A {",
        "  1: string street",
        "  2: optional States state",
        "  3: required string zip",
        "}",
        "",
        "struct B {",
        "  1: string name",
        "  2: set<A> addresses",
        "}",
        "",
        "struct C {",
        "  1: B b = {",
        "    'name': 'bob', ",
        "    'addresses': [",
        "      {",
        "        'street': '123 Main St.',",
        "        'state': State.MT,",
        "        'zip': '59715'",
        "      }",
        "      {",
        "        'zip': '02134'",
        "      }",
        "    ]",
        "  }",
        "}");
    assertOutdirFiles(
        outdirPath("test", "C.java"),
        outdirPath("test", "B.java"),
        outdirPath("test", "A.java"),
        outdirPath("test", "States.java"));
    Class<? extends ThriftStruct> cClass =
        compileStructClass("test.C", "test.B", "test.A", "test.States");

    ThriftStruct defaultC = ThriftStruct.builder(cClass).build();

    ImmutableMap<String, ThriftFields> cFields = indexFields(cClass, "b");

    Class<? extends Enum> statesClass = assertEnumClass(loadClass("test.States"));
    Enum montana = assertEnum(statesClass, "MT", 5);

    Class<? extends ThriftStruct> aClass = assertStructClass(loadClass("test.A"));
    ImmutableMap<String, ThriftFields> aFields = indexFields(aClass, "street", "state", "zip");
    Object address1 =
        ThriftStruct.builder(aClass)
            .set(aFields.get("street"), "123 Main St.")
            .set(aFields.get("state"), montana)
            .set(aFields.get("zip"), "59715")
            .build();
    Object address2 = ThriftStruct.builder(aClass).set(aFields.get("zip"), "02134").build();

    Class<? extends ThriftStruct> bClass = assertStructClass(loadClass("test.B"));
    ImmutableMap<String, ThriftFields> bFields = indexFields(bClass, "name", "addresses");
    ThriftStruct expectedDefaultB =
        ThriftStruct.builder(bClass)
            .set(bFields.get("name"), "bob")
            .set(bFields.get("addresses"), ImmutableSet.of(address1, address2))
            .build();

    Object actualDefaultB = defaultC.getFieldValue(cFields.get("b"));
    assertEquals(expectedDefaultB, actualDefaultB);
  }

  private static Class<? extends ThriftUnion> assertUnionClass(Class<?> clazz) {
    return assertAssignableFrom(ThriftUnion.class, clazz);
  }

  private Class<? extends ThriftUnion> compileUnionClass(
      String className,
      String... additionalClasses)
      throws IOException, ClassNotFoundException {

    Class<?> clazz = compileClass(className, additionalClasses);
    return assertUnionClass(clazz);
  }

  private static <T> Type immutableListType(Class<T> containedType) {
    return new TypeToken<ImmutableList<T>>() {}
        .where(new TypeParameter<T>() {}, containedType)
        .getType();
  }

  @Test
  public void testUnion() throws Exception {
    generateThrift(
        "namespace java test",
        "",
        "struct Error {}",
        "",
        "union Response {",
        "  2: Error error",
        "  4: list<Error> errors",
        "  6: bool noop",
        "}");
    assertOutdirFiles(outdirPath("test", "Error.java"), outdirPath("test", "Response.java"));
    Class<? extends ThriftUnion> unionClass = compileUnionClass("test.Response", "test.Error");

    ImmutableMap<String, ThriftFields> fieldsByName =
        indexFields(unionClass, "error", "errors", "noop");

    Class<? extends ThriftStruct> errorStructClass = assertStructClass(loadClass("test.Error"));
    ThriftFields errorField = fieldsByName.get("error");
    assertField(errorField, (short) 2, errorStructClass, errorStructClass);

    ThriftFields errorsField = fieldsByName.get("errors");
    assertField(errorsField, (short) 4, ImmutableList.class, immutableListType(errorStructClass));

    ThriftFields noopField = fieldsByName.get("noop");
    assertField(noopField, (short) 6, boolean.class, boolean.class);

    ThriftStruct errorStruct = ThriftStruct.builder(errorStructClass).build();
    assertSerializationRoundTrip(errorStructClass, errorStruct);

    ThriftUnion errorResponse = ThriftUnion.create(unionClass, errorField, errorStruct);
    assertSame(errorField, errorResponse.getSetField());
    assertSame(errorStruct, errorResponse.getFieldValue());

    ThriftUnion errorsResponse =
        ThriftUnion.create(unionClass, errorsField, ImmutableList.of(errorStruct));
    assertSame(errorsField, errorsResponse.getSetField());
    assertEquals(ImmutableList.of(errorStruct), errorsResponse.getFieldValue());

    ThriftUnion noopResponse = ThriftUnion.create(unionClass, noopField, true);
    assertSame(noopField, noopResponse.getSetField());
    assertEquals(true, noopResponse.getFieldValue());
  }

  private Class<? extends ThriftService> assertServiceInterface(Class<?> clazz) {
    assertTrue(clazz.isInterface());
    return assertAssignableFrom(ThriftService.class, clazz);
  }

  private Class<? extends ThriftService> loadThriftService(
      Class<?> expectedEnclosingClass,
      String className)
      throws ClassNotFoundException {

    Class<?> clazz = loadClass(className);
    assertSame(expectedEnclosingClass, clazz.getEnclosingClass());
    return assertServiceInterface(clazz);
  }

  private static void assertSignature(Method method, Type returnType, Type... parameterTypes) {
    assertEquals(returnType, method.getGenericReturnType());
    assertEquals(
        ImmutableList.copyOf(parameterTypes),
        ImmutableList.copyOf(method.getGenericParameterTypes()));
  }

  @Test
  public void testService() throws Exception {
    generateThrift(
        "namespace java test",
        "",
        "service Base {",
        "  bool isAlive()",
        "}",
        "",
        "service Sub extends Base {",
        "  string getMessageOfTheDay(1: bool extendedVersion)",
        "}");
    Class<?> subClass = compileClass("test.Sub");
    Class<?> baseClass = loadClass("test.Base");
    assertTrue(subClass.isInterface());

    ImmutableSet<String> expectedMethodNames = ImmutableSet.of("isAlive", "getMessageOfTheDay");

    Class<? extends ThriftService> asyncBaseClass = loadThriftService(baseClass, "test.Base$Async");
    Class<? extends ThriftService> asyncSubClass = loadThriftService(subClass, "test.Sub$Async");
    assertEquals(
        ImmutableSet.of(ThriftService.class, asyncBaseClass),
        ImmutableSet.copyOf(asyncSubClass.getInterfaces()));

    ImmutableMap<String, Method> asyncMethods = ThriftService.getThriftMethods(asyncSubClass);
    assertEquals(expectedMethodNames, asyncMethods.keySet());
    assertSignature(
        asyncMethods.get("isAlive"),
        new TypeToken<ListenableFuture<Boolean>>() {}.getType());
    assertSignature(
        asyncMethods.get("getMessageOfTheDay"),
        new TypeToken<ListenableFuture<String>>() {}.getType(),
        boolean.class);

    Class<? extends ThriftService> syncBaseClass = loadThriftService(baseClass, "test.Base$Sync");
    Class<? extends ThriftService> syncSubClass = loadThriftService(subClass, "test.Sub$Sync");
    assertEquals(
        ImmutableSet.of(ThriftService.class, syncBaseClass),
        ImmutableSet.copyOf(syncSubClass.getInterfaces()));
    ImmutableMap<String, Method> syncMethods = ThriftService.getThriftMethods(syncSubClass);
    assertEquals(expectedMethodNames, syncMethods.keySet());
    assertSignature(syncMethods.get("isAlive"), boolean.class);
    assertSignature(syncMethods.get("getMessageOfTheDay"), String.class, boolean.class);
  }

  @Test
  public void testServiceAnnotations() throws Exception {
    generateThrift(
        "namespace java test",
        "",
        "service UserInfo {",
        "  string getStatus(1: string userName (secured='true'), 2: bool verbose)",
        "}");
    Class<? extends ThriftService> userInfoSyncServiceClass =
        loadThriftService(compileClass("test.UserInfo"), "test.UserInfo$Sync");

    Method setStatusMethod =
        Iterables.getOnlyElement(ThriftService.getThriftMethods(userInfoSyncServiceClass).values());
    Parameter[] parameters = setStatusMethod.getParameters();
    assertEquals(2, parameters.length);
    ThriftAnnotation annotation = parameters[0].getAnnotation(ThriftAnnotation.class);
    assertEquals(
        ImmutableThriftAnnotation.of(new ThriftAnnotation.Parameter[] {
            ImmutableParameter.of("secured", "true")}),
        annotation);
    assertNull(parameters[1].getAnnotation(ThriftAnnotation.class));
  }
}
