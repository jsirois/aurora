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
import java.util.stream.Collectors;

import javax.tools.JavaFileObject;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.google.testing.compile.CompileTester;
import com.google.testing.compile.JavaFileObjects;

import org.junit.Test;

import static com.google.common.truth.Truth.assert_;
import static com.google.testing.compile.JavaSourcesSubjectFactory.javaSources;

public class MutablePeerProcessorTest {

  private static final String PACKAGE = "org.apache.aurora.test";
  private static final Joiner PACKAGE_JOINER = Joiner.on('.');

  private static String fqcn(String... simpleNames) {
    return PACKAGE_JOINER.join(
        ImmutableList.builder().add(PACKAGE).addAll(Arrays.asList(simpleNames)).build());
  }

  private static String loadCode(URL resource) {
    try {
      return Resources.toString(resource, Charsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static JavaFileObject javaFile(String fullyQualifiedClassName) {
    // NB: We load java sources from files w/o java extension to prevent a host of build issues
    // that can crop up when resource files are `.java` files.
    URL resource = Resources.getResource(fullyQualifiedClassName.replace('.', '/'));
    String code = loadCode(resource);
    return JavaFileObjects.forSourceString(fullyQualifiedClassName, code);
  }

  private static JavaFileObject javaFileForClassName(String simpleClassName) {
    return javaFile(fqcn(simpleClassName));
  }

  private static JavaFileObject javaFileForPeer(String simpleClassName) {
    return javaFile(fqcn("peer", "Mutable" + simpleClassName));
  }

  private static CompileTester assertAboutSources(String primary, String... rest) {
    return assert_().about(javaSources())
        .that(Lists.asList(primary, rest)
            .stream()
            .map(MutablePeerProcessorTest::javaFileForClassName)
            .collect(Collectors.toList()))
        // We suppress warnings about un-processed annotations but otherwise fail on any warnings.
        .withCompilerOptions("-Xlint:all,-processing", "-Werror")
        .processedWith(new MutablePeerProcessor());
  }

  private void assertGenerated(String primary, String... rest) {
    assertAboutSources(primary, rest)
        .compilesWithoutError()
        .and()
        .generatesSources(
            javaFileForPeer(primary),
            Arrays.asList(rest).stream()
                .map(MutablePeerProcessorTest::javaFileForPeer)
                .toArray(JavaFileObject[]::new));
  }

  @Test
  public void testPrimitiveField() {
    assertGenerated("PrimitiveField");
  }

  @Test
  public void testMultipleThriftAnnotations() {
    assertGenerated("MultipleThriftAnnotations");
  }

  @Test
  public void testMultipleThriftAnnotationsContainer() {
    assertGenerated("MultipleThriftAnnotationsContainer");
  }

  @Test
  public void testPrimitiveListField() {
    assertGenerated("PrimitiveListField");
  }

  @Test
  public void testPrimitiveSetField() {
    assertGenerated("PrimitiveSetField");
  }

  @Test
  public void testPrimitiveMapField() {
    assertGenerated("PrimitiveMapField");
  }

  @Test
  public void testThriftField() {
    assertGenerated("ThriftField", "PrimitiveField");
  }

  @Test
  public void testThriftListField() {
    assertGenerated("ThriftListField", "PrimitiveField");
  }

  @Test
  public void testThriftSetField() {
    assertGenerated("ThriftSetField", "PrimitiveField");
  }

  @Test
  public void testThriftMapField() {
    JavaFileObject thriftMapField = javaFileForClassName("ThriftMapField");
    assertAboutSources("ThriftMapField", "PrimitiveField")
        .failsToCompile()
        .withErrorCount(1)
        .withErrorContaining(MutablePeerProcessor.MUTABLE_PEER_MAPS_NOT_SUPPORTED_MSG)
        .in(thriftMapField)
        .onLine(48);
  }

  @Test
  public void testPreExistingPeerThriftField() {
    assertAboutSources("PreExistingPeerThriftField", "PreExistingPeer", "peer.artisinal.Peer")
        .compilesWithoutError()
        .and()
        .generatesSources(javaFileForPeer("PreExistingPeerThriftField"));
  }
}
