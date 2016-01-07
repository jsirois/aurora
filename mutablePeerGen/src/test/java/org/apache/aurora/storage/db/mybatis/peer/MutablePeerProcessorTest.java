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
package org.apache.aurora.storage.db.mybatis.peer;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;

import javax.tools.JavaFileObject;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.google.testing.compile.JavaFileObjects;

import org.junit.Test;

import static com.google.common.truth.Truth.assert_;
import static com.google.testing.compile.JavaSourceSubjectFactory.javaSource;

public class MutablePeerProcessorTest {

  private static final String PACKAGE = "org.apache.aurora.test";
  private static final Joiner PACKAGE_JOINER = Joiner.on('.');

  private static String fqcn(String... simpleNames) {
    return PACKAGE_JOINER.join(
        ImmutableList.builder().add(PACKAGE).addAll(Arrays.asList(simpleNames)).build());
  }

  private static JavaFileObject javaFile(String fullyQualifiedClassName) throws IOException {
    // NB: We load java sources from files w/o java extension to prevent a host of issues that can
    // crop up when resource files are `.java` files.
    URL resource = Resources.getResource(fullyQualifiedClassName.replace('.', '/'));
    String code = Resources.toString(resource, Charsets.UTF_8);
    return JavaFileObjects.forSourceString(fullyQualifiedClassName, code);
  }

  private static void assertGenerated(String className) throws IOException {
    String input = fqcn(className);
    String mutableInput = fqcn("peer", "Mutable" + className);

    assert_().about(javaSource())
        .that(javaFile(input))
        .processedWith(new MutablePeerProcessor())
        .compilesWithoutError()
        .and()
        .generatesSources(javaFile(mutableInput));
  }

  @Test
  public void testPrimitiveField() throws IOException {
    assertGenerated("Primitive");
  }
}
