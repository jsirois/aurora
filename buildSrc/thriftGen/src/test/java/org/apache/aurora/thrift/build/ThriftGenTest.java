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
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.jimfs.Jimfs;
import com.sun.tools.javac.nio.JavacPathFileManager;
import com.sun.tools.javac.util.Context;

import org.apache.thrift.TEnum;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    outdir = Files.createDirectory(fileSystem.getPath("/out"));
    thriftGen = new ThriftGen(outdir, LoggerFactory.getLogger(getClass()));

    classes = Files.createDirectory(fileSystem.getPath("/classes"));
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

  private void write(Path file, String contents) throws IOException {
    Files.write(
        file,
        contents.getBytes(Charsets.UTF_8),
        StandardOpenOption.CREATE_NEW,
        StandardOpenOption.WRITE);
  }

  private void assertOutdirFiles(Path... paths) throws IOException {
    assertEquals(
        ImmutableSet.copyOf(paths),
        ImmutableSet.copyOf(Files.walk(outdir).filter(Files::isRegularFile).iterator()));
  }

  @Test
  public void testNoJavaNamespace() throws IOException {
    Path thriftFile = fileSystem.getPath("test.thrift");
    write(thriftFile, "namespace py test");
    thriftGen.generate(ImmutableSet.of(thriftFile));

    assertOutdirFiles();
  }

  private Class<?> compileClass(String className) throws IOException, ClassNotFoundException {
    JavaFileObject enumSource =
        fileManager.getJavaFileForInput(
            StandardLocation.SOURCE_PATH,
            className,
            JavaFileObject.Kind.SOURCE);

    JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler();
    JavaCompiler.CompilationTask task =
        javaCompiler.getTask(
            new PrintWriter(System.err),
            fileManager,
            null /* DiagnosticListener: default */,
            ImmutableList.of() /* javac options */,
            null /* apt classes: no apt */,
            ImmutableList.of(enumSource));
    boolean success = task.call();
    assertTrue(success);

    return Class.forName(className, true /* initialize */, classLoader);
  }

  @Test
  public void testEnum() throws IOException, ClassNotFoundException {
    Path thriftFile = fileSystem.getPath("test.thrift");
    write(
        thriftFile,
        "namespace java test\n" +
        "enum ResponseCode {\n" +
        "  OK = 0,\n" +
        "  ERROR = 2\n" +
        "}");
    thriftGen.generate(ImmutableSet.of(thriftFile));

    Path enumCode = outdirPath("test", "ResponseCode.java");
    assertOutdirFiles(enumCode);

    @SuppressWarnings("raw")
    Class clazz = compileClass("test.ResponseCode");
    assertTrue(Enum.class.isAssignableFrom(clazz));

    @SuppressWarnings({"raw", "unchecked"})
    Class<? extends Enum> enumClass = (Class<? extends Enum>) clazz;

    Enum ok = Enum.valueOf(enumClass, "OK");
    assertTrue(ok instanceof TEnum);
    assertEquals(0, ((TEnum) ok).getValue());

    Enum error = Enum.valueOf(enumClass, "ERROR");
    assertTrue(error instanceof TEnum);
    assertEquals(2, ((TEnum) error).getValue());
  }
}
