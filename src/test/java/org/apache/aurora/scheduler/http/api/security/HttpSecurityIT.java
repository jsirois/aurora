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
package org.apache.aurora.scheduler.http.api.security;

import java.net.URI;
import java.util.Set;

import com.facebook.nifty.client.HttpClientChannel;
import com.facebook.nifty.client.HttpClientConnector;
import com.facebook.nifty.client.NettyClientConfig;
import com.facebook.nifty.duplex.TDuplexProtocolFactory;
import com.facebook.swift.service.ThriftClientManager;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import com.sun.jersey.api.client.ClientResponse;

import org.apache.aurora.common.base.Closure;
import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.ResponseCode;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.http.H2ConsoleModule;
import org.apache.aurora.scheduler.http.JettyServerModuleTest;
import org.apache.aurora.scheduler.http.api.ApiModule;
import org.apache.aurora.scheduler.thrift.aop.AnnotatedAuroraAdmin;
import org.apache.aurora.scheduler.thrift.aop.MockDecoratedThrift;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.shiro.authz.UnauthenticatedException;
import org.apache.shiro.config.Ini;
import org.apache.shiro.realm.text.IniRealm;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TJSONProtocol;
import org.jboss.netty.channel.Channel;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.http.H2ConsoleModule.H2_PATH;
import static org.apache.aurora.scheduler.http.H2ConsoleModule.H2_PERM;
import static org.apache.aurora.scheduler.http.api.ApiModule.API_PATH;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class HttpSecurityIT extends JettyServerModuleTest {
  private static final Response OK = Response.builder().setResponseCode(ResponseCode.OK).build();

  private static final UsernamePasswordCredentials ROOT =
      new UsernamePasswordCredentials("root", "secret");
  private static final UsernamePasswordCredentials WFARNER =
      new UsernamePasswordCredentials("wfarner", "password");
  private static final UsernamePasswordCredentials UNPRIVILEGED =
      new UsernamePasswordCredentials("ksweeney", "12345");
  private static final UsernamePasswordCredentials BACKUP_SERVICE =
      new UsernamePasswordCredentials("backupsvc", "s3cret!!1");
  private static final UsernamePasswordCredentials DEPLOY_SERVICE =
      new UsernamePasswordCredentials("deploysvc", "0_0-x_0");
  private static final UsernamePasswordCredentials H2_USER =
      new UsernamePasswordCredentials("dbuser", "pwd");

  private static final UsernamePasswordCredentials INCORRECT =
      new UsernamePasswordCredentials("root", "wrong");
  private static final UsernamePasswordCredentials NONEXISTENT =
      new UsernamePasswordCredentials("nobody", "12345");

  private static final Set<Credentials> INVALID_CREDENTIALS =
      ImmutableSet.of(INCORRECT, NONEXISTENT);

  private static final Set<Credentials> VALID_CREDENTIALS =
      ImmutableSet.of(ROOT, WFARNER, UNPRIVILEGED, BACKUP_SERVICE);

  private static final JobKey ADS_STAGING_JOB = JobKeys.from("ads", "staging", "job");

  private Ini ini;
  private AnnotatedAuroraAdmin auroraAdmin;

  private static final Joiner COMMA_JOINER = Joiner.on(", ");
  private static final String ADMIN_ROLE = "admin";
  private static final String ENG_ROLE = "eng";
  private static final String BACKUP_ROLE = "backup";
  private static final String DEPLOY_ROLE = "deploy";
  private static final String H2_ROLE = "h2access";

  @Before
  public void setUp() {
    ini = new Ini();

    Ini.Section users = ini.addSection(IniRealm.USERS_SECTION_NAME);
    users.put(ROOT.getUserName(), COMMA_JOINER.join(ROOT.getPassword(), ADMIN_ROLE));
    users.put(WFARNER.getUserName(), COMMA_JOINER.join(WFARNER.getPassword(), ENG_ROLE));
    users.put(UNPRIVILEGED.getUserName(), UNPRIVILEGED.getPassword());
    users.put(
        BACKUP_SERVICE.getUserName(),
        COMMA_JOINER.join(BACKUP_SERVICE.getPassword(), BACKUP_ROLE));
    users.put(
        DEPLOY_SERVICE.getUserName(),
        COMMA_JOINER.join(DEPLOY_SERVICE.getPassword(), DEPLOY_ROLE));
    users.put(H2_USER.getUserName(), COMMA_JOINER.join(H2_USER.getPassword(), H2_ROLE));

    Ini.Section roles = ini.addSection(IniRealm.ROLES_SECTION_NAME);
    roles.put(ADMIN_ROLE, "*");
    roles.put(ENG_ROLE, "thrift.AuroraSchedulerManager:*");
    roles.put(BACKUP_ROLE, "thrift.AuroraAdmin:listBackups");
    roles.put(
        DEPLOY_ROLE,
        "thrift.AuroraSchedulerManager:killTasks:"
            + ADS_STAGING_JOB.getRole()
            + ":"
            + ADS_STAGING_JOB.getEnvironment()
            + ":"
            + ADS_STAGING_JOB.getName());
    roles.put(H2_ROLE, H2_PERM);

    auroraAdmin = createMock(AnnotatedAuroraAdmin.class);
  }

  @Override
  protected Module getChildServletModule() {
    return Modules.combine(
        new ApiModule(),
        new H2ConsoleModule(true),
        new HttpSecurityModule(new IniShiroRealmModule(ini)),
        new AbstractModule() {
          @Override
          protected void configure() {
            MockDecoratedThrift.bindForwardedMock(binder(), auroraAdmin);
          }
        });
  }

  private AuroraAdmin.Sync getUnauthenticatedClient() throws Exception {
    return getClient(channel -> {});
  }

  private String formatUrl(String endpoint) {
    return "http://" + httpServer.getHostText() + ":" + httpServer.getPort() + endpoint;
  }

  private AuroraAdmin.Sync getClient(Closure<HttpClientChannel> channelModifier) throws Exception {
    URI uri = URI.create(formatUrl(API_PATH));
    TDuplexProtocolFactory protocolFactory =
        TDuplexProtocolFactory.fromSingleFactory(new TJSONProtocol.Factory());

    HttpClientConnector httpClientConnector = new HttpClientConnector(uri, protocolFactory) {
      @Override
      public HttpClientChannel newThriftClientChannel(Channel channel, NettyClientConfig config) {
        HttpClientChannel httpChannel = super.newThriftClientChannel(channel, config);
        channelModifier.execute(httpChannel);
        return httpChannel;
      }
    };

    ThriftClientManager thriftClientManager = new ThriftClientManager();
    addTearDown(thriftClientManager::close);

    ListenableFuture<AuroraAdmin.Sync> connectingClient =
        thriftClientManager.createClient(httpClientConnector, AuroraAdmin.Sync.class);
    AuroraAdmin.Sync sync = connectingClient.get();
    addTearDown(sync::close);
    return sync;
  }

  private AuroraAdmin.Sync getAuthenticatedClient(Credentials credentials) throws Exception {
    return getClient(channel -> {
      Header authenticate =
          BasicScheme.authenticate(credentials, Charsets.UTF_8.displayName(), /* proxy */ false);
      channel.setHeaders(ImmutableMap.of(authenticate.getName(), authenticate.getValue()));
    });
  }

  @Test
  public void testReadOnlyScheduler() throws Exception {
    expect(auroraAdmin.getRoleSummary()).andReturn(OK).times(3);

    replayAndStart();

    assertEquals(OK, getUnauthenticatedClient().getRoleSummary());
    assertEquals(OK, getAuthenticatedClient(ROOT).getRoleSummary());
    // Incorrect works because the server doesn't challenge for credentials to execute read-only
    // methods.
    assertEquals(OK, getAuthenticatedClient(INCORRECT).getRoleSummary());
  }

  private void assertKillTasksFails(AuroraAdmin.Sync client) throws TException {
    try {
      client.killTasks(null, null);
      fail("killTasks should fail.");
    } catch (UnauthenticatedException e) {
      // Expected.
    }
  }

  @Test
  public void testAuroraSchedulerManager() throws Exception {
    expect(auroraAdmin.killTasks(null, Lock.builder().setMessage("1").build())).andReturn(OK);
    expect(auroraAdmin.killTasks(null, Lock.builder().setMessage("2").build())).andReturn(OK);

    TaskQuery jobScopedQuery = Query.jobScoped(JobKeys.from("role", "env", "name")).get();
    TaskQuery adsScopedQuery = Query.jobScoped(ADS_STAGING_JOB).get();
    expect(auroraAdmin.killTasks(adsScopedQuery, null)).andReturn(OK);

    replayAndStart();

    assertEquals(OK,
        getAuthenticatedClient(WFARNER).killTasks(null, Lock.builder().setMessage("1").build()));
    assertEquals(OK,
        getAuthenticatedClient(ROOT).killTasks(null, Lock.builder().setMessage("2").build()));
    assertEquals(
        ResponseCode.INVALID_REQUEST,
        getAuthenticatedClient(UNPRIVILEGED).killTasks(null, null).getResponseCode());
    assertEquals(
        ResponseCode.AUTH_FAILED,
        getAuthenticatedClient(UNPRIVILEGED)
            .killTasks(jobScopedQuery, null)
            .getResponseCode());
    assertEquals(
        ResponseCode.INVALID_REQUEST,
        getAuthenticatedClient(BACKUP_SERVICE).killTasks(null, null).getResponseCode());
    assertEquals(
        ResponseCode.AUTH_FAILED,
        getAuthenticatedClient(BACKUP_SERVICE)
            .killTasks(jobScopedQuery, null)
            .getResponseCode());
    assertEquals(
        ResponseCode.AUTH_FAILED,
        getAuthenticatedClient(DEPLOY_SERVICE)
            .killTasks(jobScopedQuery, null)
            .getResponseCode());
    assertEquals(
        OK,
        getAuthenticatedClient(DEPLOY_SERVICE).killTasks(adsScopedQuery, null));

    assertKillTasksFails(getUnauthenticatedClient());
    assertKillTasksFails(getAuthenticatedClient(INCORRECT));
    assertKillTasksFails(getAuthenticatedClient(NONEXISTENT));
  }

  private void assertSnapshotFails(AuroraAdmin.Sync client) throws TException {
    try {
      client.snapshot();
      fail("snapshot should fail");
    } catch (UnauthenticatedException e) {
      // Expected.
    }
  }

  @Test
  public void testAuroraAdmin() throws Exception {
    expect(auroraAdmin.snapshot()).andReturn(OK);
    expect(auroraAdmin.listBackups()).andReturn(OK);

    replayAndStart();

    assertEquals(OK, getAuthenticatedClient(ROOT).snapshot());

    for (Credentials credentials : INVALID_CREDENTIALS) {
      assertSnapshotFails(getAuthenticatedClient(credentials));
    }

    for (Credentials credentials : Sets.difference(VALID_CREDENTIALS, ImmutableSet.of(ROOT))) {
      assertEquals(
          ResponseCode.AUTH_FAILED,
          getAuthenticatedClient(credentials).snapshot().getResponseCode());
    }

    assertEquals(OK, getAuthenticatedClient(BACKUP_SERVICE).listBackups());
  }

  private HttpResponse callH2Console(Credentials credentials) throws Exception {
    DefaultHttpClient defaultHttpClient = new DefaultHttpClient();

    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY, credentials);
    defaultHttpClient.setCredentialsProvider(credentialsProvider);
    return defaultHttpClient.execute(new HttpPost(formatUrl(H2_PATH + "/")));
  }

  @Test
  public void testH2ConsoleUser() throws Exception {
    replayAndStart();

    assertEquals(
        ClientResponse.Status.OK.getStatusCode(),
        callH2Console(H2_USER).getStatusLine().getStatusCode());
  }

  @Test
  public void testH2ConsoleAdmin() throws Exception {
    replayAndStart();

    assertEquals(
        ClientResponse.Status.OK.getStatusCode(),
        callH2Console(ROOT).getStatusLine().getStatusCode());
  }

  @Test
  public void testH2ConsoleUnauthorized() throws Exception {
    replayAndStart();

    assertEquals(
        ClientResponse.Status.UNAUTHORIZED.getStatusCode(),
        callH2Console(UNPRIVILEGED).getStatusLine().getStatusCode());
  }
}
