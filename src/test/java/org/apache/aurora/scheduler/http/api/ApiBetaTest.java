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
package org.apache.aurora.scheduler.http.api;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.CronCollisionPolicy;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobSummary;
import org.apache.aurora.gen.JobSummaryResult;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.Result;
import org.apache.aurora.gen.RoleSummary;
import org.apache.aurora.gen.RoleSummaryResult;
import org.apache.aurora.gen.ScheduleStatusResult;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.http.AbstractJettyTest;
import org.apache.aurora.scheduler.thrift.Responses;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ResponseCode.OK;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class ApiBetaTest extends AbstractJettyTest {
  private AuroraAdmin.Sync thrift;

  @Before
  public void setUp() {
    thrift = createMock(AuroraAdmin.Sync.class);
  }

  @Override
  protected Module getChildServletModule() {
    return Modules.combine(
        new ApiModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(AuroraAdmin.Sync.class).toInstance(thrift);
          }
        }
    );
  }

  private static final TaskConfig TASK_CONFIG = TaskTestUtil.makeConfig(TaskTestUtil.JOB);
  private static final JobConfiguration JOB_CONFIG = JobConfiguration.builder()
      .setCronCollisionPolicy(CronCollisionPolicy.CANCEL_NEW)
      .setKey(JobKey.create("role", "env", "name"))
      .setTaskConfig(TASK_CONFIG)
      .build();

  @Test
  public void testCreateJob() throws Exception {
    Lock lock = Lock.builder()
        .setKey(LockKey.job(JobKey.create("role", "env", "name")))
        .setToken("token")
        .build();
    Response response = Responses.ok();

    expect(thrift.createJob(anyObject(), eq(lock))).andReturn(response);

    replayAndStart();

    Response actualResponse = getRequestBuilder("/apibeta/createJob")
        .entity(
            ImmutableMap.of("description", JOB_CONFIG, "lock", lock),
            MediaType.APPLICATION_JSON)
        .post(Response.class);
    assertEquals(response, actualResponse);
  }

  @Test
  public void testGetRoleSummary() throws Exception {
    Response response = Response.builder()
        .setResponseCode(OK)
        .setResult(Result.roleSummaryResult(RoleSummaryResult.create(
            ImmutableSet.of(RoleSummary.builder()
                .setCronJobCount(1)
                .setJobCount(2)
                .setRole("role")
                .build()))))
        .build();

    expect(thrift.getRoleSummary()).andReturn(response);

    replayAndStart();

    Response actualResponse = getRequestBuilder("/apibeta/getRoleSummary")
        .post(Response.class);
    assertEquals(response, actualResponse);
  }

  @Test
  public void testGetJobSummary() throws Exception {
    Response response = Response.builder()
        .setResponseCode(OK)
        .setResult(Result.jobSummaryResult(JobSummaryResult.create(
            ImmutableSet.of(JobSummary.builder()
                .setJob(JOB_CONFIG)
                .build()))))
        .build();

    expect(thrift.getJobSummary("roleA")).andReturn(response);

    replayAndStart();

    Response actualResponse = getRequestBuilder("/apibeta/getJobSummary")
        .entity(ImmutableMap.of("role", "roleA"), MediaType.APPLICATION_JSON)
        .post(Response.class);
    assertEquals(response, actualResponse);
  }

  @Test
  public void testGetTasks() throws Exception {
    ScheduledTask task = ScheduledTask.builder()
        .setStatus(RUNNING)
        .setAssignedTask(
            AssignedTask.builder()
                .setTask(TASK_CONFIG)
                .build())
        .build();
    Response response = Response.builder()
        .setResponseCode(OK)
        .setResult(Result.scheduleStatusResult(ScheduleStatusResult.create(
            ImmutableList.of(task))))
        .build();

    TaskQuery query = TaskQuery.builder()
        .setStatuses(ImmutableSet.of(RUNNING))
        .setTaskIds(ImmutableSet.of("a"))
        .build();

    expect(thrift.getTasksStatus(query)).andReturn(response);

    replayAndStart();

    Response actualResponse = getRequestBuilder("/apibeta/getTasksStatus")
        .entity(ImmutableMap.of("query", query), MediaType.APPLICATION_JSON)
        .post(Response.class);
    assertEquals(response, actualResponse);
  }

  @Test
  public void testGetHelp() throws Exception {
    replayAndStart();

    ClientResponse response = getRequestBuilder("/apibeta")
        .accept(MediaType.TEXT_HTML)
        .get(ClientResponse.class);
    assertEquals(Status.SEE_OTHER.getStatusCode(), response.getStatus());
  }

  @Test
  public void testPostInvalidStructure() throws Exception {
    replayAndStart();

    ClientResponse badRequest = getRequestBuilder("/apibeta/createJob")
        .entity("not an object", MediaType.APPLICATION_JSON)
        .post(ClientResponse.class);
    assertEquals(Status.BAD_REQUEST.getStatusCode(), badRequest.getStatus());

    ClientResponse badParameter = getRequestBuilder("/apibeta/createJob")
        .entity(ImmutableMap.of("description", "not a job description"), MediaType.APPLICATION_JSON)
        .post(ClientResponse.class);
    assertEquals(Status.BAD_REQUEST.getStatusCode(), badParameter.getStatus());
  }

  @Test
  public void testInvalidApiMethod() throws Exception {
    replayAndStart();

    ClientResponse response = getRequestBuilder("/apibeta/notAMethod")
        .post(ClientResponse.class);
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void testPostInvalidJson() throws Exception {
    replayAndStart();

    ClientConfig config = new DefaultClientConfig();
    Client client = Client.create(config);
    ClientResponse response = client.resource(makeUrl("/apibeta/createJob"))
        .accept(MediaType.APPLICATION_JSON)
        .entity("{this is bad json}", MediaType.APPLICATION_JSON)
        .post(ClientResponse.class);
    assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }
}
