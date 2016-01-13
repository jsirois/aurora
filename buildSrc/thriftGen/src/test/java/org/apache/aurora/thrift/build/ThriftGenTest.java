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

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.jimfs.Jimfs;
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
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import autovalue.shaded.com.google.common.common.collect.FluentIterable;
import autovalue.shaded.com.google.common.common.collect.Iterables;
import autovalue.shaded.com.google.common.common.collect.Lists;

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
          throw new ClassNotFoundException(e.toString());
        }
      }
    };

    JavacPathFileManager fileManager =
        new JavacPathFileManager(new Context(), false, Charsets.UTF_8);
    fileManager.setDefaultFileSystem(fileSystem);
    fileManager.setLocation(StandardLocation.SOURCE_PATH, ImmutableList.of(outdir));
    fileManager.setLocation(StandardLocation.CLASS_OUTPUT, ImmutableList.of(classes));
    this.fileManager = fileManager;
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
            ImmutableList.of("-implicit:class", "-Werror") /* javac options */,
            null /* apt classes: use META-INF discovery mechanism */,
            javaFiles);
    boolean success = task.call();
    assertTrue(success);
    return loadClass(className);
  }

  private static Class<? extends Enum> assertEnumClass(Class<?> clazz) {
    assertTrue(Enum.class.isAssignableFrom(clazz));
    // We tested this was assignable to Enum above and needs to be raw to extract an enum value.
    @SuppressWarnings({"raw", "unchecked"})
    Class<? extends Enum> enumClass = (Class<? extends Enum>) clazz;
    return enumClass;
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
        "const string REGEX = \"[Jj]ake\"",
        "const set<string> TAGS = [\"A\", \"B\"]",
        "const list<bool> BITS = [0, 1]",
        "const map<string, bool> COLORS = {\"reddish\": 1, \"bluish\": 0}");
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

  private Class<? extends ThriftStruct> compileStructClass(
      String className,
      String... additionalClasses)
      throws IOException, ClassNotFoundException {

    Class<?> clazz = compileClass(className, additionalClasses);

    assertTrue(ThriftStruct.class.isAssignableFrom(clazz));
    // We tested this was assignable to ThriftStruct above and needs to be raw so the user can
    // extract fields.
    @SuppressWarnings({"raw", "unchecked"})
    Class<? extends ThriftStruct> structClass = (Class<? extends ThriftStruct>) clazz;
    return structClass;
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

  private static ThriftStruct createStruct(
      Class<? extends ThriftStruct> structClass,
      Map<ThriftFields, Object> fields) {

    return buildStruct(structClass, fields).build();
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
        "  5: optional string description = \"None\"",
        "}");
    assertOutdirFiles(outdirPath("test", "Struct.java"));
    Class<? extends ThriftStruct> structClass = compileStructClass("test.Struct");

    assertMissingFields(structClass, ImmutableMap.of());

    ImmutableMap<String, ThriftFields> fieldsByName = indexFields(structClass);
    assertEquals(ImmutableSet.of("name", "address", "description"), fieldsByName.keySet());

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
      Class<? extends ThriftEntity> structClass) {

    return Maps.uniqueIndex(ThriftEntity.fields(structClass), ThriftFields::getFieldName);
  }

  @Test
  @SuppressWarnings({"raw", "unchecked"}) // Needed to to extract fields.
  public void testIncludes() throws Exception {
    Path includedFile = fileSystem.getPath("subdir", "included.thrift");
    write(
        includedFile,
        "namespace java test.subpackage",
        "",
        "const string NAME = \"George\"",
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
        "include \"subdir/included.thrift\"",
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

    ImmutableMap<String, ThriftFields> fieldsByName = indexFields(structClass);
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
        "  doc=\"",
        "Multiline strings should work",
        "for annotation values making them ~natural for doc",
        "\"",
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

  private Class<? extends ThriftUnion> compileUnionClass(
      String className,
      String... additionalClasses)
      throws IOException, ClassNotFoundException {

    Class<?> clazz = compileClass(className, additionalClasses);
    assertTrue(ThriftUnion.class.isAssignableFrom(clazz));
    // We tested this was assignable to ThriftUnion above and needs to be raw so the user can
    // extract fields.
    @SuppressWarnings({"raw", "unchecked"})
    Class<? extends ThriftUnion> unionClass = (Class<? extends ThriftUnion>) clazz;
    return unionClass;
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

    ImmutableMap<String, ThriftFields> fieldsByName = indexFields(unionClass);
    assertEquals(ImmutableSet.of("error", "errors", "noop"), fieldsByName.keySet());

    // We know test.Error is a struct from reading the thrift above.
    @SuppressWarnings("unchecked")
    Class<? extends ThriftStruct> clazz = (Class<? extends ThriftStruct>) loadClass("test.Error");
    ThriftStruct errorStruct = ThriftStruct.builder(clazz).build();
    ThriftFields errorField = fieldsByName.get("error");
    ThriftUnion errorResponse = ThriftUnion.create(unionClass, errorField, errorStruct);
    assertSame(errorField, errorResponse.getSetField());
    assertSame(errorStruct, errorResponse.getFieldValue());

    ThriftFields errorsField = fieldsByName.get("errors");
    ThriftUnion errorsResponse =
        ThriftUnion.create(unionClass, errorsField, ImmutableList.of(errorStruct));
    assertSame(errorsField, errorsResponse.getSetField());
    assertEquals(ImmutableList.of(errorStruct), errorsResponse.getFieldValue());

    ThriftFields noopField = fieldsByName.get("noop");
    ThriftUnion noopResponse = ThriftUnion.create(unionClass, noopField, true);
    assertSame(noopField, noopResponse.getSetField());
    assertEquals(true, noopResponse.getFieldValue());
  }

  private Class<? extends ThriftService> loadThriftService(
      Class<?> expectedEnclosingClass,
      String className)
      throws ClassNotFoundException {

    Class<?> clazz = loadClass(className);
    assertSame(expectedEnclosingClass, clazz.getEnclosingClass());
    assertTrue(ThriftService.class.isAssignableFrom(clazz));
    @SuppressWarnings("unchecked") // We checked this was afe just above.
    Class<? extends ThriftService> thriftServiceClass = (Class<? extends ThriftService>) clazz;
    return thriftServiceClass;
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
        "  string getStatus(1: string userName (secured=\"true\"), 2: bool verbose)",
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
