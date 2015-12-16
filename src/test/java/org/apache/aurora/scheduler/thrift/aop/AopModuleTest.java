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
package org.apache.aurora.scheduler.thrift.aop;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.CreationException;
import com.google.inject.Guice;
import com.google.inject.Injector;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.ResponseCode;
import org.apache.aurora.gen.ServerInfo;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AopModuleTest extends EasyMockTest {
  private AnnotatedAuroraAdmin mockThrift;
  private ServerInfo serverInfo;

  @Before
  public void setUp() throws Exception {
    mockThrift = createMock(AnnotatedAuroraAdmin.class);
    serverInfo = ServerInfo.builder().build();
  }

  private AuroraAdmin.Sync getIface(Map<String, Boolean> toggledMethods) {
    Injector injector = Guice.createInjector(
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(ServerInfo.class).toInstance(serverInfo);
            MockDecoratedThrift.bindForwardedMock(binder(), mockThrift);
          }
        },
        new AopModule(toggledMethods));
    return injector.getInstance(AnnotatedAuroraAdmin.class);
  }

  @Test
  public void testNonFlaggedMethod() throws Exception {
    assertCreateAllowed(ImmutableMap.of("acquireLock", false));
  }

  @Test
  public void testNoFlaggedMethods() throws Exception {
    assertCreateAllowed(ImmutableMap.of());
  }

  @Test
  public void testFlaggedMethodEnabled() throws Exception {
    assertCreateAllowed(ImmutableMap.of("createJob", true));
  }

  @Test
  public void testFlaggedMethodDisabled() throws Exception {
    JobConfiguration job = JobConfiguration.builder().build();

    control.replay();

    AuroraAdmin.Sync thrift = getIface(ImmutableMap.of("createJob", false));
    assertEquals(ResponseCode.ERROR, thrift.createJob(job, null).getResponseCode());
  }

  @Test(expected = CreationException.class)
  public void testMissingMethod() {
    control.replay();
    getIface(ImmutableMap.of("notamethod", true));
  }

  private void assertCreateAllowed(Map<String, Boolean> toggledMethods) throws Exception {
    JobConfiguration job = JobConfiguration.builder().build();
    Response response = Response.builder().build();
    expect(mockThrift.createJob(job, null)).andReturn(response);

    control.replay();

    // The ServerInfoInterceptor is expected to add in serverInfo.
    Response expected = response.toBuilder().setServerInfo(serverInfo).build();

    AuroraAdmin.Sync thrift = getIface(toggledMethods);
    assertEquals(expected, thrift.createJob(job, null));
  }

  @Test
  public void assertToStringNotIntercepted() {
    control.replay();

    AuroraAdmin.Sync thrift = getIface(ImmutableMap.of());
    assertNotNull(thrift.toString());
  }
}
