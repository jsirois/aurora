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
import org.apache.aurora.gen.AuroraAdmin.Sync;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.ResponseCode;
import org.apache.aurora.gen.ServerInfo;
import org.apache.aurora.gen.ServerInfo;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

public class AopModuleTest extends EasyMockTest {
  private AnnotatedAuroraAdmin mockThrift;

  @Before
  public void setUp() throws Exception {
    mockThrift = createMock(AnnotatedAuroraAdmin.class);
  }

  private Iface getIface(Map<String, Boolean> toggledMethods) {
    Injector injector = Guice.createInjector(
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(ServerInfo.class).toInstance(ServerInfo.build(new ServerInfo()));
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
    JobConfiguration job = new JobConfiguration();

    control.replay();

    Iface thrift = getIface(ImmutableMap.of("createJob", false));
    assertEquals(ResponseCode.ERROR, thrift.createJob(job, null).getResponseCode());
  }

  @Test(expected = CreationException.class)
  public void testMissingMethod() {
    control.replay();
    getIface(ImmutableMap.of("notamethod", true));
  }

  private void assertCreateAllowed(Map<String, Boolean> toggledMethods) throws Exception {
    JobConfiguration job = new JobConfiguration();
    Response response = new Response();
    expect(mockThrift.createJob(job, null)).andReturn(response);

    control.replay();

    Iface thrift = getIface(toggledMethods);
    assertSame(response, thrift.createJob(job, null));
  }

  @Test
  public void assertToStringNotIntercepted() {
    control.replay();

    Iface thrift = getIface(ImmutableMap.of());
    assertNotNull(thrift.toString());
  }
}
