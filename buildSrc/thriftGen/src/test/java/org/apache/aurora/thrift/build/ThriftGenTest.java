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
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.jimfs.Jimfs;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class ThriftGenTest {
  private FileSystem fileSystem;
  private Path outdir;
  private ThriftGen thriftGen;

  @Before
  public void setUp() throws IOException {
    fileSystem = Jimfs.newFileSystem();
    outdir = Files.createDirectory(fileSystem.getPath("/out"));
    thriftGen = new ThriftGen(outdir, LoggerFactory.getLogger(getClass()));
  }

  private void write(Path file, String contents) throws IOException {
    Files.write(
        file,
        contents.getBytes(Charsets.UTF_8),
        StandardOpenOption.CREATE_NEW,
        StandardOpenOption.WRITE);
  }

  @Test
  public void testNotJavaNamespace() throws IOException {
    Path thriftFile = fileSystem.getPath("test.thrift");
    write(thriftFile, "namespace py test");
    thriftGen.generate(ImmutableSet.of(thriftFile));

    assertEquals(ImmutableList.of(outdir), ImmutableList.copyOf(Files.walk(outdir).iterator()));
  }
}
