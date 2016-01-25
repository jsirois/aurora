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
package org.apache.aurora.scheduler.thrift;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.AddInstancesConfig;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.ConfigRewrite;
import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.HostStatus;
import org.apache.aurora.gen.Hosts;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.InstanceConfigRewrite;
import org.apache.aurora.gen.InstanceKey;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobConfigRewrite;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdatePulseStatus;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.JobUpdateRequest;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.LimitConstraint;
import org.apache.aurora.gen.ListBackupsResult;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.MesosContainer;
import org.apache.aurora.gen.PulseJobUpdateResult;
import org.apache.aurora.gen.QueryRecoveryResult;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.ReadOnlyScheduler;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.Result;
import org.apache.aurora.gen.RewriteConfigsRequest;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.StartJobUpdateResult;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.gen.ValueConstraint;
import org.apache.aurora.scheduler.TaskIdGenerator;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import org.apache.aurora.scheduler.configuration.SanitizedConfiguration;
import org.apache.aurora.scheduler.cron.CronException;
import org.apache.aurora.scheduler.cron.CronJobManager;
import org.apache.aurora.scheduler.cron.SanitizedCronJob;
import org.apache.aurora.scheduler.quota.QuotaCheckResult;
import org.apache.aurora.scheduler.quota.QuotaManager;
import org.apache.aurora.scheduler.state.LockManager;
import org.apache.aurora.scheduler.state.LockManager.LockException;
import org.apache.aurora.scheduler.state.MaintenanceController;
import org.apache.aurora.scheduler.state.StateChangeResult;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.state.UUIDGenerator;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.backup.Recovery;
import org.apache.aurora.scheduler.storage.backup.StorageBackup;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.updater.JobUpdateController;
import org.apache.aurora.scheduler.updater.JobUpdateController.AuditData;
import org.apache.aurora.scheduler.updater.UpdateStateException;
import org.apache.thrift.TException;
import org.easymock.EasyMock;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.LockValidation.CHECKED;
import static org.apache.aurora.gen.LockValidation.UNCHECKED;
import static org.apache.aurora.gen.MaintenanceMode.DRAINING;
import static org.apache.aurora.gen.MaintenanceMode.NONE;
import static org.apache.aurora.gen.MaintenanceMode.SCHEDULED;
import static org.apache.aurora.gen.ResponseCode.INVALID_REQUEST;
import static org.apache.aurora.gen.ResponseCode.LOCK_ERROR;
import static org.apache.aurora.gen.ResponseCode.OK;
import static org.apache.aurora.gen.ResponseCode.WARNING;
import static org.apache.aurora.scheduler.configuration.ConfigurationManager.DEDICATED_ATTRIBUTE;
import static org.apache.aurora.scheduler.storage.backup.Recovery.RecoveryException;
import static org.apache.aurora.scheduler.thrift.Fixtures.CRON_JOB;
import static org.apache.aurora.scheduler.thrift.Fixtures.ENOUGH_QUOTA;
import static org.apache.aurora.scheduler.thrift.Fixtures.JOB_KEY;
import static org.apache.aurora.scheduler.thrift.Fixtures.JOB_NAME;
import static org.apache.aurora.scheduler.thrift.Fixtures.LOCK;
import static org.apache.aurora.scheduler.thrift.Fixtures.LOCK_KEY;
import static org.apache.aurora.scheduler.thrift.Fixtures.NOT_ENOUGH_QUOTA;
import static org.apache.aurora.scheduler.thrift.Fixtures.ROLE;
import static org.apache.aurora.scheduler.thrift.Fixtures.ROLE_IDENTITY;
import static org.apache.aurora.scheduler.thrift.Fixtures.TASK_ID;
import static org.apache.aurora.scheduler.thrift.Fixtures.UPDATE_KEY;
import static org.apache.aurora.scheduler.thrift.Fixtures.USER;
import static org.apache.aurora.scheduler.thrift.Fixtures.UU_ID;
import static org.apache.aurora.scheduler.thrift.Fixtures.assertOkResponse;
import static org.apache.aurora.scheduler.thrift.Fixtures.assertResponse;
import static org.apache.aurora.scheduler.thrift.Fixtures.defaultTask;
import static org.apache.aurora.scheduler.thrift.Fixtures.makeJob;
import static org.apache.aurora.scheduler.thrift.Fixtures.makeProdJob;
import static org.apache.aurora.scheduler.thrift.Fixtures.nonProductionTask;
import static org.apache.aurora.scheduler.thrift.Fixtures.okResponse;
import static org.apache.aurora.scheduler.thrift.Fixtures.productionTask;
import static org.apache.aurora.scheduler.thrift.Fixtures.response;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.MAX_TASK_ID_LENGTH;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.NOOP_JOB_UPDATE_MESSAGE;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.NO_CRON;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.jobAlreadyExistsMessage;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.noCronScheduleMessage;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.notScheduledCronMessage;
import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SchedulerThriftInterfaceTest extends EasyMockTest {

  private static final String AUDIT_MESSAGE = "message";
  private static final AuditData AUDIT = new AuditData(USER, Optional.of(AUDIT_MESSAGE));
  private static final Thresholds THRESHOLDS = new Thresholds(1000, 2000);

  private StorageTestUtil storageUtil;
  private LockManager lockManager;
  private StorageBackup backup;
  private Recovery recovery;
  private MaintenanceController maintenance;
  private AuroraAdmin.Sync thrift;
  private CronJobManager cronJobManager;
  private QuotaManager quotaManager;
  private StateManager stateManager;
  private TaskIdGenerator taskIdGenerator;
  private UUIDGenerator uuidGenerator;
  private JobUpdateController jobUpdateController;
  private ReadOnlyScheduler.Sync readOnlyScheduler;
  private AuditMessages auditMessages;

  @Before
  public void setUp() throws Exception {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    lockManager = createMock(LockManager.class);
    backup = createMock(StorageBackup.class);
    recovery = createMock(Recovery.class);
    maintenance = createMock(MaintenanceController.class);
    cronJobManager = createMock(CronJobManager.class);
    quotaManager = createMock(QuotaManager.class);
    stateManager = createMock(StateManager.class);
    taskIdGenerator = createMock(TaskIdGenerator.class);
    uuidGenerator = createMock(UUIDGenerator.class);
    jobUpdateController = createMock(JobUpdateController.class);
    readOnlyScheduler = createMock(ReadOnlyScheduler.Sync.class);
    auditMessages = createMock(AuditMessages.class);

    thrift = getResponseProxy(
        new SchedulerThriftInterface(
            TaskTestUtil.CONFIGURATION_MANAGER,
            THRESHOLDS,
            storageUtil.storage,
            lockManager,
            backup,
            recovery,
            cronJobManager,
            maintenance,
            quotaManager,
            stateManager,
            taskIdGenerator,
            uuidGenerator,
            jobUpdateController,
            readOnlyScheduler,
            auditMessages));
  }

  private static AuroraAdmin.Sync getResponseProxy(AuroraAdmin.Sync realThrift) {
    // Capture all API method calls to validate response objects.
    Class<AuroraAdmin.Sync> thriftClass = AuroraAdmin.Sync.class;
    return (AuroraAdmin.Sync) Proxy.newProxyInstance(
        thriftClass.getClassLoader(),
        new Class<?>[] {thriftClass},
        (o, method, args) -> {
          Response response;
          try {
            response = (Response) method.invoke(realThrift, args);
          } catch (InvocationTargetException e) {
            Throwables.propagateIfPossible(e.getTargetException(), TException.class);
            throw e;
          }
          assertTrue(response.isSetResponseCode());
          assertNotNull(response.getDetails());
          return response;
        });
  }

  private static SanitizedConfiguration fromUnsanitized(JobConfiguration job)
      throws TaskDescriptionException {

    return SanitizedConfiguration.fromUnsanitized(TaskTestUtil.CONFIGURATION_MANAGER, job);
  }

  private static JobConfiguration validateAndPopulate(JobConfiguration job)
      throws TaskDescriptionException {

    return TaskTestUtil.CONFIGURATION_MANAGER.validateAndPopulate(job);
  }

  @Test
  public void testCreateJobNoLock() throws Exception {
    // Validate key is populated during sanitizing.
    JobConfiguration jobConfig = makeProdJob().withTaskConfig(tc -> tc.withJob((JobKey) null));

    JobConfiguration job = makeProdJob();
    SanitizedConfiguration sanitized = fromUnsanitized(job);
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.empty());
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expectNoCronJob();
    expect(taskIdGenerator.generate(sanitized.getJobConfig().getTaskConfig(), 1))
        .andReturn(TASK_ID);
    expectInstanceQuotaCheck(sanitized, ENOUGH_QUOTA);

    stateManager.insertPendingTasks(
        storageUtil.mutableStoreProvider,
        sanitized.getJobConfig().getTaskConfig(),
        sanitized.getInstanceIds());

    control.replay();

    assertOkResponse(thrift.createJob(jobConfig, null));
  }

  @Test
  public void testCreateJobWithLock() throws Exception {
    JobConfiguration job = makeProdJob();
    SanitizedConfiguration sanitized = fromUnsanitized(job);
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.of(LOCK));
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expectNoCronJob();
    expect(taskIdGenerator.generate(sanitized.getJobConfig().getTaskConfig(), 1))
        .andReturn(TASK_ID);
    expectInstanceQuotaCheck(sanitized, ENOUGH_QUOTA);

    stateManager.insertPendingTasks(
        storageUtil.mutableStoreProvider,
        sanitized.getJobConfig().getTaskConfig(),
        sanitized.getInstanceIds());

    control.replay();

    assertOkResponse(thrift.createJob(job, LOCK));
  }

  @Test
  public void testCreateJobFailsForCron() throws Exception {
    JobConfiguration job = makeProdJob().withCronSchedule("");

    control.replay();

    assertEquals(
        invalidResponse(NO_CRON),
        thrift.createJob(job, LOCK));
  }

  @Test
  public void testCreateJobFailsConfigCheck() throws Exception {
    JobConfiguration job = makeJob(null);
    control.replay();

    assertResponse(
        INVALID_REQUEST,
        thrift.createJob(job, null));
  }

  @Test
  public void testCreateJobFailsLockCheck() throws Exception {
    JobConfiguration job = makeJob();
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.of(LOCK));
    expectLastCall().andThrow(new LockException("Invalid lock"));

    control.replay();

    assertResponse(LOCK_ERROR, thrift.createJob(job, LOCK));
  }

  @Test
  public void testCreateJobFailsJobExists() throws Exception {
    JobConfiguration job = makeJob();
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.empty());
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active(), buildScheduledTask());

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.createJob(job, null));
  }

  @Test
  public void testCreateJobFailsCronJobExists() throws Exception {
    JobConfiguration job = makeJob();
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.empty());
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expectCronJob();

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.createJob(job, null));
  }

  @Test
  public void testCreateJobFailsInstanceCheck() throws Exception {
    JobConfiguration job = makeJob(defaultTask(true), THRESHOLDS.getMaxTasksPerJob() + 1);

    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.empty());
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expectNoCronJob();
    expect(quotaManager.checkInstanceAddition(
        anyObject(TaskConfig.class),
        anyInt(),
        eq(storageUtil.mutableStoreProvider))).andReturn(ENOUGH_QUOTA);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.createJob(job, null));
  }

  @Test
  public void testCreateJobFailsTaskIdLength() throws Exception {
    JobConfiguration job = makeJob();
    SanitizedConfiguration sanitized = fromUnsanitized(job);
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.empty());
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expectNoCronJob();
    expect(quotaManager.checkInstanceAddition(
        anyObject(TaskConfig.class),
        anyInt(),
        eq(storageUtil.mutableStoreProvider))).andReturn(ENOUGH_QUOTA);

    expect(taskIdGenerator.generate(sanitized.getJobConfig().getTaskConfig(), 1))
        .andReturn(Strings.repeat("a", MAX_TASK_ID_LENGTH + 1));

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.createJob(job, null));
  }

  @Test
  public void testCreateJobFailsQuotaCheck() throws Exception {
    JobConfiguration job = makeProdJob();
    SanitizedConfiguration sanitized = fromUnsanitized(job);
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.empty());
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expectNoCronJob();
    expect(taskIdGenerator.generate(sanitized.getJobConfig().getTaskConfig(), 1))
        .andReturn(TASK_ID);
    expectInstanceQuotaCheck(sanitized, NOT_ENOUGH_QUOTA);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.createJob(job, null));
  }

  private void assertMessageMatches(Response response, String string) {
    // TODO(wfarner): This test coverage could be much better.  Circle back to apply more thorough
    // response contents testing throughout.
    assertTrue(Iterables.any(response.getDetails(), detail -> detail.getMessage().equals(string)));
  }

  @Test
  public void testCreateEmptyJob() throws Exception {
    control.replay();

    JobConfiguration job =
        JobConfiguration.builder().setKey(JOB_KEY).setOwner(ROLE_IDENTITY).build();
    assertResponse(INVALID_REQUEST, thrift.createJob(job, null));
  }

  @Test
  public void testCreateJobFailsNoExecutorConfig() throws Exception {
    JobConfiguration job = makeJob().withTaskConfig(
        tc -> tc.withExecutorConfig((ExecutorConfig) null));

    control.replay();

    Response response = thrift.createJob(job, LOCK);
    assertResponse(INVALID_REQUEST, response);
    // TODO(wfarner): Don't rely on a magic string here, reference a constant from the source.
    assertMessageMatches(response, "Configuration may not be null");
  }

  @Test
  public void testCreateHomogeneousJobNoInstances() throws Exception {
    JobConfiguration job = makeJob().withInstanceCount(0);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.createJob(job, null));
  }

  @Test
  public void testCreateJobNegativeInstanceCount() throws Exception {
    JobConfiguration job = makeJob().withInstanceCount(-1);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.createJob(job, null));
  }

  @Test
  public void testCreateJobNoResources() throws Exception {
    control.replay();

    TaskConfig task = productionTask().toBuilder()
        .setNumCpus(0)
        .setRamMb(0)
        .setDiskMb(0)
        .build();
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task), null));
  }

  @Test
  public void testCreateJobBadCpu() throws Exception {
    control.replay();

    TaskConfig task = productionTask().withNumCpus(0.0);
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task), null));
  }

  @Test
  public void testCreateJobBadRam() throws Exception {
    control.replay();

    TaskConfig task = productionTask().withRamMb(-123);
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task), null));
  }

  @Test
  public void testCreateJobBadDisk() throws Exception {
    control.replay();

    TaskConfig task = productionTask().withDiskMb(0);
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task), null));
  }

  @Test
  public void testCreateJobPopulateDefaults() throws Exception {
    TaskConfig task = TaskConfig.builder()
        .setContactEmail("testing@twitter.com")
        .setExecutorConfig(ExecutorConfig.create("aurora", "config"))  // Arbitrary opaque data.
        .setNumCpus(1.0)
        .setRamMb(1024)
        .setDiskMb(1024)
        .setIsService(true)
        .setProduction(true)
        .setOwner(ROLE_IDENTITY)
        .setEnvironment("devel")
        .setContainer(Container.mesos(MesosContainer.create()))
        .setJobName(JOB_NAME)
        .build();
    JobConfiguration job = makeJob(task);

    JobConfiguration sanitized = job.withTaskConfig(tc -> tc.toBuilder()
        .setJob(JOB_KEY)
        .setNumCpus(1.0)
        .setPriority(0)
        .setRamMb(1024)
        .setDiskMb(1024)
        .setIsService(true)
        .setProduction(true)
        .setRequestedPorts()
        .setTaskLinks(ImmutableMap.of())
        .setConstraints()
        .setMaxTaskFailures(0)
        .setEnvironment("devel")
        .build());

    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.empty());
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expectNoCronJob();
    expect(taskIdGenerator.generate(sanitized.getTaskConfig(), 1)).andReturn(TASK_ID);
    expectInstanceQuotaCheck(sanitized.getTaskConfig(), ENOUGH_QUOTA);
    stateManager.insertPendingTasks(
        storageUtil.mutableStoreProvider,
        sanitized.getTaskConfig(),
        ImmutableSet.of(0));

    control.replay();

    assertOkResponse(thrift.createJob(job, null));
  }

  @Test
  public void testCreateUnauthorizedDedicatedJob() throws Exception {
    control.replay();

    TaskConfig task = nonProductionTask()
        .withConstraints(ImmutableSet.of(dedicatedConstraint(ImmutableSet.of("mesos"))));
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task), null));
  }

  @Test
  public void testLimitConstraintForDedicatedJob() throws Exception {
    control.replay();

    TaskConfig task = nonProductionTask().withConstraints(ImmutableSet.of(dedicatedConstraint(1)));
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task), null));
  }

  @Test
  public void testMultipleValueConstraintForDedicatedJob() throws Exception {
    control.replay();

    TaskConfig task = nonProductionTask()
        .withConstraints(ImmutableSet.of(dedicatedConstraint(ImmutableSet.of("mesos", "test"))));
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task), null));
  }

  private ScheduledTask buildTaskForJobUpdate(int instanceId) {
    return buildTaskForJobUpdate(instanceId, "data");
  }

  private ScheduledTask buildTaskForJobUpdate(int instanceId, String executorData) {
    return ScheduledTask.builder()
        .setAssignedTask(AssignedTask.builder()
            .setInstanceId(instanceId)
            .setTask(populatedTask().toBuilder()
                .setRamMb(5)
                .setIsService(true)
                .setExecutorConfig(ExecutorConfig.builder().setData(executorData).build())
                .build())
            .build())
        .build();
  }

  private ScheduledTask buildScheduledTask() {
    return buildScheduledTask(JOB_NAME, TASK_ID);
  }

  private static ScheduledTask buildScheduledTask(String jobName, String taskId) {
    return ScheduledTask.builder()
        .setAssignedTask(AssignedTask.builder()
            .setTaskId(taskId)
            .setTask(TaskConfig.builder()
                .setJob(JOB_KEY.withName(jobName))
                .setOwner(ROLE_IDENTITY)
                .setEnvironment("devel")
                .setJobName(jobName)
                .build())
            .build())
        .build();
  }

  private void expectTransitionsToKilling() {
    expect(auditMessages.killedByRemoteUser()).andReturn(Optional.of("test"));
    expect(stateManager.changeState(
        storageUtil.mutableStoreProvider,
        TASK_ID,
        Optional.absent(),
        ScheduleStatus.KILLING,
        Optional.of("test"))).andReturn(StateChangeResult.SUCCESS);
  }

  @Test
  public void testKillByJobName() throws Exception {
    TaskQuery query = TaskQuery.builder().setJobName("job").build();
    storageUtil.expectTaskFetch(Query.arbitrary(query).active(), buildScheduledTask());
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.empty());
    expectTransitionsToKilling();

    control.replay();

    Response response = thrift.killTasks(query, null, null, null);
    assertOkResponse(response);
    assertMessageMatches(response, "The TaskQuery field is deprecated.");
  }

  @Test
  public void testJobScopedKillsActive() throws Exception {
    Query.Builder query = Query.unscoped().byJob(JOB_KEY);
    storageUtil.expectTaskFetch(query.active(), buildScheduledTask());
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.empty());
    expectTransitionsToKilling();

    control.replay();

    assertOkResponse(thrift.killTasks(null, null, JOB_KEY, null));
  }

  @Test
  public void testInstanceScoped() throws Exception {
    Query.Builder query = Query.instanceScoped(JOB_KEY, ImmutableSet.of(1)).active();
    storageUtil.expectTaskFetch(query, buildScheduledTask());
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.empty());
    expectTransitionsToKilling();

    control.replay();

    assertOkResponse(thrift.killTasks(null, null, JOB_KEY, ImmutableSet.of(1)));
  }

  @Test
  public void testKillTasksLockCheckFailed() throws Exception {
    Query.Builder query = Query.unscoped().byJob(JOB_KEY).active();
    ScheduledTask task2 = buildScheduledTask("job_bar", TASK_ID);
    LockKey key2 = LockKey.job(JobKeys.from(ROLE, "devel", "job_bar"));
    storageUtil.expectTaskFetch(query, buildScheduledTask(), task2);
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.of(LOCK));
    lockManager.validateIfLocked(key2, java.util.Optional.of(LOCK));
    expectLastCall().andThrow(new LockException("Failed lock check."));

    control.replay();

    assertResponse(
        LOCK_ERROR,
        thrift.killTasks(null, LOCK, JOB_KEY, null));
  }

  @Test
  public void testKillByTaskId() throws Exception {
    // A non-admin user may kill their own tasks when specified by task IDs.
    Query.Builder query = Query.taskScoped("taskid");
    // This query happens twice - once for authentication (without consistency) and once again
    // to perform the state change (within a write transaction).
    storageUtil.expectTaskFetch(query.active(), buildScheduledTask());
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.empty());
    expectTransitionsToKilling();

    control.replay();

    assertOkResponse(thrift.killTasks(query.get(), null, null, null));
  }

  @Test
  public void testKillTasksInvalidJobName() throws Exception {
    TaskQuery query = TaskQuery.builder()
        .setOwner(ROLE_IDENTITY)
        .setJobName("")
        .build();

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.killTasks(query, null, null, null));
  }

  @Test
  public void testKillNonExistentTasks() throws Exception {
    Query.Builder query = Query.unscoped().byJob(JOB_KEY).active();
    storageUtil.expectTaskFetch(query);

    control.replay();

    Response response = thrift.killTasks(null, null, JOB_KEY, null);
    assertOkResponse(response);
    assertMessageMatches(response, SchedulerThriftInterface.NO_TASKS_TO_KILL_MESSAGE);
  }

  @Test
  public void testSetQuota() throws Exception {
    ResourceAggregate resourceAggregate = ResourceAggregate.builder()
        .setNumCpus(10)
        .setDiskMb(100)
        .setRamMb(200)
        .build();
    quotaManager.saveQuota(
        ROLE,
        resourceAggregate,
        storageUtil.mutableStoreProvider);

    control.replay();

    assertOkResponse(thrift.setQuota(ROLE, resourceAggregate));
  }

  @Test
  public void testSetQuotaFails() throws Exception {
    ResourceAggregate resourceAggregate = ResourceAggregate.builder()
        .setNumCpus(10)
        .setDiskMb(100)
        .setRamMb(200)
        .build();
    quotaManager.saveQuota(
        ROLE,
        resourceAggregate,
        storageUtil.mutableStoreProvider);

    expectLastCall().andThrow(new QuotaManager.QuotaException("fail"));

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.setQuota(ROLE, resourceAggregate));
  }

  @Test
  public void testForceTaskState() throws Exception {
    ScheduleStatus status = ScheduleStatus.FAILED;

    expect(auditMessages.transitionedBy()).andReturn(Optional.of("test"));
    expect(stateManager.changeState(
        storageUtil.mutableStoreProvider,
        TASK_ID,
        Optional.absent(),
        ScheduleStatus.FAILED,
        Optional.of("test"))).andReturn(StateChangeResult.SUCCESS);

    control.replay();

    assertOkResponse(thrift.forceTaskState(TASK_ID, status));
  }

  @Test
  public void testBackupControls() throws Exception {
    backup.backupNow();

    Set<String> backups = ImmutableSet.of("a", "b");
    expect(recovery.listBackups()).andReturn(backups);

    String backupId = "backup";
    recovery.stage(backupId);

    Query.Builder query = Query.taskScoped("taskId");
    Set<ScheduledTask> queryResult = ImmutableSet.of(
        ScheduledTask.builder().setStatus(ScheduleStatus.RUNNING).build());
    expect(recovery.query(query)).andReturn(queryResult);

    recovery.deleteTasks(query);

    recovery.commit();

    recovery.unload();

    control.replay();

    assertEquals(okEmptyResponse(), thrift.performBackup());

    assertEquals(
        okResponse(Result.listBackupsResult(ListBackupsResult.create(backups))),
        thrift.listBackups());

    assertEquals(okEmptyResponse(), thrift.stageRecovery(backupId));

    assertEquals(
        okResponse(Result.queryRecoveryResult(QueryRecoveryResult.create(queryResult))),
        thrift.queryRecovery(query.get()));

    assertEquals(okEmptyResponse(), thrift.deleteRecoveryTasks(query.get()));

    assertEquals(okEmptyResponse(), thrift.commitRecovery());

    assertEquals(okEmptyResponse(), thrift.unloadRecovery());
  }

  @Test
  public void testRecoveryException() throws Exception {
    Throwable recoveryException = new RecoveryException("Injected");

    String backupId = "backup";
    recovery.stage(backupId);
    expectLastCall().andThrow(recoveryException);

    control.replay();

    try {
      thrift.stageRecovery(backupId);
      fail("No recovery exception thrown.");
    } catch (RecoveryException e) {
      assertEquals(recoveryException.getMessage(), e.getMessage());
    }
  }

  @Test
  public void testRestartShards() throws Exception {
    Set<Integer> shards = ImmutableSet.of(0);

    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.of(LOCK));
    storageUtil.expectTaskFetch(
        Query.instanceScoped(JOB_KEY, shards).active(),
        buildScheduledTask());

    expect(auditMessages.restartedByRemoteUser())
        .andReturn(Optional.of("test"));
    expect(stateManager.changeState(
        storageUtil.mutableStoreProvider,
        TASK_ID,
        Optional.absent(),
        ScheduleStatus.RESTARTING,
        Optional.of("test"))).andReturn(StateChangeResult.SUCCESS);

    control.replay();

    assertOkResponse(
        thrift.restartShards(JOB_KEY, shards, LOCK));
  }

  @Test
  public void testRestartShardsLockCheckFails() throws Exception {
    Set<Integer> shards = ImmutableSet.of(1, 6);

    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.of(LOCK));
    expectLastCall().andThrow(new LockException("test"));

    control.replay();

    assertResponse(
        LOCK_ERROR,
        thrift.restartShards(JOB_KEY, shards, LOCK));
  }

  @Test
  public void testRestartShardsNotFoundTasksFailure() throws Exception {
    Set<Integer> shards = ImmutableSet.of(1, 6);

    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.of(LOCK));
    storageUtil.expectTaskFetch(Query.instanceScoped(JOB_KEY, shards).active());

    control.replay();

    assertResponse(
        INVALID_REQUEST,
        thrift.restartShards(JOB_KEY, shards, LOCK));
  }

  @Test
  public void testReplaceCronTemplate() throws Exception {
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.empty());
    SanitizedConfiguration sanitized = fromUnsanitized(CRON_JOB);

    expect(taskIdGenerator.generate(sanitized.getJobConfig().getTaskConfig(), 1))
        .andReturn(TASK_ID);
    expectCronQuotaCheck(sanitized.getJobConfig(), ENOUGH_QUOTA);
    cronJobManager.updateJob(anyObject(SanitizedCronJob.class));
    control.replay();

    // Validate key is populated during sanitizing.
    JobConfiguration jobConfig = CRON_JOB.withTaskConfig(tc -> tc.withJob((JobKey) null));
    assertOkResponse(thrift.replaceCronTemplate(jobConfig, null));
  }

  @Test
  public void testReplaceCronTemplateFailedLockValidation() throws Exception {
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.empty());
    expectLastCall().andThrow(new LockException("Failed lock."));
    control.replay();

    assertResponse(LOCK_ERROR, thrift.replaceCronTemplate(CRON_JOB, null));
  }

  @Test
  public void testReplaceCronTemplateDoesNotExist() throws Exception {
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.empty());
    SanitizedConfiguration sanitized = fromUnsanitized(CRON_JOB);

    expect(taskIdGenerator.generate(sanitized.getJobConfig().getTaskConfig(), 1))
        .andReturn(TASK_ID);
    expectCronQuotaCheck(sanitized.getJobConfig(), ENOUGH_QUOTA);
    cronJobManager.updateJob(anyObject(SanitizedCronJob.class));
    expectLastCall().andThrow(new CronException("Nope"));

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.replaceCronTemplate(CRON_JOB, null));
  }

  @Test
  public void testStartCronJob() throws Exception {
    cronJobManager.startJobNow(JOB_KEY);
    control.replay();
    assertResponse(OK, thrift.startCronJob(JOB_KEY));
  }

  @Test
  public void testStartCronJobFailsInCronManager() throws Exception {
    cronJobManager.startJobNow(JOB_KEY);
    expectLastCall().andThrow(new CronException("failed"));
    control.replay();
    assertResponse(INVALID_REQUEST, thrift.startCronJob(JOB_KEY));
  }

  @Test
  public void testScheduleCronCreatesJob() throws Exception {
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.empty());
    SanitizedConfiguration sanitized = fromUnsanitized(CRON_JOB);

    expect(taskIdGenerator.generate(sanitized.getJobConfig().getTaskConfig(), 1))
        .andReturn(TASK_ID);
    expectCronQuotaCheck(sanitized.getJobConfig(), ENOUGH_QUOTA);

    expectNoCronJob().times(2);
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    cronJobManager.createJob(SanitizedCronJob.from(sanitized));
    control.replay();
    assertResponse(OK, thrift.scheduleCronJob(CRON_JOB, null));
  }

  @Test
  public void testScheduleCronFailsCreationDueToExistingNonCron() throws Exception {
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.empty());
    SanitizedConfiguration sanitized = fromUnsanitized(CRON_JOB);

    expect(taskIdGenerator.generate(sanitized.getJobConfig().getTaskConfig(), 1))
        .andReturn(TASK_ID);
    expectCronQuotaCheck(sanitized.getJobConfig(), ENOUGH_QUOTA);

    expectNoCronJob();
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active(), buildScheduledTask());
    control.replay();
    assertEquals(
        invalidResponse(jobAlreadyExistsMessage(JOB_KEY)),
        thrift.scheduleCronJob(CRON_JOB, null));
  }

  @Test
  public void testScheduleCronUpdatesJob() throws Exception {
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.empty());
    SanitizedConfiguration sanitized = fromUnsanitized(CRON_JOB);

    expect(taskIdGenerator.generate(sanitized.getJobConfig().getTaskConfig(), 1))
        .andReturn(TASK_ID);
    expectCronQuotaCheck(sanitized.getJobConfig(), ENOUGH_QUOTA);

    expectCronJob();
    cronJobManager.updateJob(SanitizedCronJob.from(sanitized));
    control.replay();

    // Validate key is populated during sanitizing.
    JobConfiguration jobConfig = CRON_JOB.withTaskConfig(tc -> tc.withJob((JobKey) null));
    assertResponse(OK, thrift.scheduleCronJob(jobConfig, null));
  }

  @Test
  public void testScheduleCronJobFailedTaskConfigValidation() throws Exception {
    control.replay();
    JobConfiguration job = makeJob(null);
    assertResponse(
        INVALID_REQUEST,
        thrift.scheduleCronJob(job, null));
  }

  @Test
  public void testScheduleCronJobFailsLockValidation() throws Exception {
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.of(LOCK));
    expectLastCall().andThrow(new LockException("Failed lock"));
    control.replay();
    assertResponse(LOCK_ERROR, thrift.scheduleCronJob(CRON_JOB, LOCK));
  }

  @Test
  public void testScheduleCronJobFailsWithNoCronSchedule() throws Exception {
    control.replay();

    assertEquals(
        invalidResponse(noCronScheduleMessage(JOB_KEY)),
        thrift.scheduleCronJob(makeJob(), null));
  }

  @Test
  public void testScheduleCronFailsQuotaCheck() throws Exception {
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.empty());
    SanitizedConfiguration sanitized = fromUnsanitized(CRON_JOB);

    expect(taskIdGenerator.generate(sanitized.getJobConfig().getTaskConfig(), 1))
        .andReturn(TASK_ID);
    expectCronQuotaCheck(sanitized.getJobConfig(), NOT_ENOUGH_QUOTA);

    control.replay();
    assertResponse(INVALID_REQUEST, thrift.scheduleCronJob(CRON_JOB, null));
  }

  @Test
  public void testDescheduleCronJob() throws Exception {
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.empty());
    expect(cronJobManager.deleteJob(JOB_KEY)).andReturn(true);

    control.replay();

    assertResponse(OK, thrift.descheduleCronJob(CRON_JOB.getKey(), null));
  }

  @Test
  public void testDescheduleCronJobFailsLockValidation() throws Exception {
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.empty());
    expectLastCall().andThrow(new LockException("Failed lock"));
    control.replay();
    assertResponse(LOCK_ERROR, thrift.descheduleCronJob(CRON_JOB.getKey(), null));
  }

  @Test
  public void testDescheduleNotACron() throws Exception {
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.empty());
    expect(cronJobManager.deleteJob(JOB_KEY)).andReturn(false);
    control.replay();

    assertEquals(
        invalidResponse(notScheduledCronMessage(JOB_KEY)),
        thrift.descheduleCronJob(JOB_KEY, null));
  }

  @Test
  public void testRewriteShardTaskMissing() throws Exception {
    InstanceKey instance = InstanceKey.create(JobKeys.from("foo", "bar", "baz"), 0);

    storageUtil.expectTaskFetch(
        Query.instanceScoped(instance.getJobKey(), instance.getInstanceId())
            .active());

    control.replay();

    RewriteConfigsRequest request = RewriteConfigsRequest.create(
        ImmutableList.of(ConfigRewrite.instanceRewrite(
            InstanceConfigRewrite.create(instance, productionTask(), productionTask()))));
    assertResponse(WARNING, thrift.rewriteConfigs(request));
  }

  @Test
  public void testRewriteNoCommands() throws Exception {
    control.replay();

    RewriteConfigsRequest request = RewriteConfigsRequest.create(ImmutableList.of());
    assertResponse(INVALID_REQUEST, thrift.rewriteConfigs(request));
  }

  @Test(expected = RuntimeException.class)
  public void testRewriteInvalidJob() throws Exception {
    control.replay();

    JobConfiguration job = makeJob();
    thrift.rewriteConfigs(
        RewriteConfigsRequest.create(
            ImmutableList.of(ConfigRewrite.jobRewrite(
                JobConfigRewrite.create(job, job.withTaskConfig((TaskConfig) null))))));
  }

  @Test
  public void testRewriteChangeJobKey() throws Exception {
    control.replay();

    JobConfiguration job = makeJob();
    JobKey rewrittenJobKey = JobKeys.from("a", "b", "c");
    Identity rewrittenIdentity = Identity.create(rewrittenJobKey.getRole(), "steve");
    RewriteConfigsRequest request = RewriteConfigsRequest.create(
        ImmutableList.of(ConfigRewrite.jobRewrite(JobConfigRewrite.create(
            job,
            job.toBuilder()
                .setTaskConfig(job.getTaskConfig().toBuilder()
                    .setJob(rewrittenJobKey)
                    .setOwner(rewrittenIdentity)
                    .build())
                .setOwner(rewrittenIdentity)
                .setKey(rewrittenJobKey).build()))));
    assertResponse(WARNING, thrift.rewriteConfigs(request));
  }

  @Test
  public void testRewriteShardCasMismatch() throws Exception {
    TaskConfig storedConfig = productionTask();
    TaskConfig modifiedConfig =
        storedConfig.withExecutorConfig(ExecutorConfig.create("aurora", "rewritten"));
    ScheduledTask storedTask = ScheduledTask.builder()
        .setAssignedTask(AssignedTask.builder().setTask(storedConfig).build())
        .build();
    InstanceKey instance = InstanceKey.create(
        JobKeys.from(
            storedConfig.getOwner().getRole(),
            storedConfig.getEnvironment(),
            storedConfig.getJobName()),
        0);

    storageUtil.expectTaskFetch(Query.instanceScoped(instance).active(), storedTask);

    control.replay();

    RewriteConfigsRequest request = RewriteConfigsRequest.create(
        ImmutableList.of(ConfigRewrite.instanceRewrite(
            InstanceConfigRewrite.create(instance, modifiedConfig, modifiedConfig))));
    assertResponse(WARNING, thrift.rewriteConfigs(request));
  }

  @Test
  public void testRewriteInstance() throws Exception {
    TaskConfig storedConfig = productionTask();
    TaskConfig modifiedConfig =
        storedConfig.withExecutorConfig(ExecutorConfig.create("aurora", "rewritten"));
    String taskId = "task_id";
    ScheduledTask storedTask = ScheduledTask.builder()
        .setAssignedTask(AssignedTask.builder()
            .setTaskId(taskId)
            .setTask(storedConfig)
            .build())
        .build();
    InstanceKey instanceKey = InstanceKey.create(
        JobKeys.from(
            storedConfig.getOwner().getRole(),
            storedConfig.getEnvironment(),
            storedConfig.getJobName()),
        0);

    storageUtil.expectTaskFetch(Query.instanceScoped(instanceKey).active(), storedTask);
    expect(storageUtil.taskStore.unsafeModifyInPlace(taskId, modifiedConfig)) .andReturn(true);

    control.replay();

    RewriteConfigsRequest request = RewriteConfigsRequest.create(
        ImmutableList.of(ConfigRewrite.instanceRewrite(
            InstanceConfigRewrite.create(instanceKey, storedConfig, modifiedConfig))));
    assertOkResponse(thrift.rewriteConfigs(request));
  }

  @Test
  public void testRewriteInstanceUnchanged() throws Exception {
    TaskConfig config = productionTask();
    String taskId = "task_id";
    ScheduledTask task = ScheduledTask.builder()
        .setAssignedTask(AssignedTask.builder()
            .setTaskId(taskId)
            .setTask(config)
            .build())
        .build();
    InstanceKey instanceKey = InstanceKey.create(
        JobKeys.from(
            config.getOwner().getRole(),
            config.getEnvironment(),
            config.getJobName()),
        0);

    storageUtil.expectTaskFetch(Query.instanceScoped(instanceKey).active(), task);
    expect(storageUtil.taskStore.unsafeModifyInPlace(taskId, config)).andReturn(false);

    control.replay();

    RewriteConfigsRequest request = RewriteConfigsRequest.create(
        ImmutableList.of(ConfigRewrite.instanceRewrite(
            InstanceConfigRewrite.create(instanceKey, config, config))));
    assertResponse(WARNING, thrift.rewriteConfigs(request));
  }

  @Test
  public void testRewriteJobCasMismatch() throws Exception {
    JobConfiguration oldJob = makeJob(productionTask());
    JobConfiguration newJob = oldJob.withTaskConfig(
        tc -> tc.withExecutorConfig(ExecutorConfig.create("aurora", "rewritten")));
    expect(storageUtil.jobStore.fetchJob(oldJob.getKey())).andReturn(Optional.of(oldJob));

    control.replay();

    RewriteConfigsRequest request = RewriteConfigsRequest.create(
        ImmutableList.of(ConfigRewrite.jobRewrite(JobConfigRewrite.create(newJob, newJob))));
    assertResponse(WARNING, thrift.rewriteConfigs(request));
  }

  @Test
  public void testRewriteJobNotFound() throws Exception {
    JobConfiguration oldJob = makeJob(productionTask());
    JobConfiguration newJob = oldJob.withTaskConfig(
        tc -> tc.withExecutorConfig(ExecutorConfig.create("aurora", "rewritten")));
    expect(storageUtil.jobStore.fetchJob(oldJob.getKey())).andReturn(Optional.absent());

    control.replay();

    RewriteConfigsRequest request = RewriteConfigsRequest.create(
        ImmutableList.of(ConfigRewrite.jobRewrite(JobConfigRewrite.create(oldJob, newJob))));
    assertResponse(WARNING, thrift.rewriteConfigs(request));
  }

  @Test
  public void testRewriteJob() throws Exception {
    JobConfiguration oldJob = makeJob(productionTask());
    JobConfiguration newJob = oldJob.withTaskConfig(
        tc -> tc.withExecutorConfig(ExecutorConfig.create("aurora", "rewritten")));
    expect(storageUtil.jobStore.fetchJob(oldJob.getKey())).andReturn(Optional.of(oldJob));
    storageUtil.jobStore.saveAcceptedJob(validateAndPopulate(newJob));

    control.replay();

    // Validate key is populated during sanitizing.
    JobConfiguration requestConfig = oldJob.withTaskConfig(tc -> tc.withJob((JobKey) null));

    RewriteConfigsRequest request = RewriteConfigsRequest.create(
        ImmutableList.of(ConfigRewrite.jobRewrite(JobConfigRewrite.create(oldJob, newJob))));
    assertOkResponse(thrift.rewriteConfigs(request));
  }

  @Test
  public void testUnauthorizedDedicatedJob() throws Exception {
    control.replay();

    TaskConfig task = nonProductionTask()
        .withConstraints(ImmutableSet.of(dedicatedConstraint(ImmutableSet.of("mesos"))));
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task), null));
  }

  private static Set<HostStatus> status(String host, MaintenanceMode mode) {
    return ImmutableSet.of(HostStatus.create(host, mode));
  }

  @Test
  public void testHostMaintenance() throws Exception {
    Set<String> hostnames = ImmutableSet.of("a");
    Set<HostStatus> none = status("a", NONE);
    Set<HostStatus> scheduled = status("a", SCHEDULED);
    Set<HostStatus> draining = status("a", DRAINING);
    Set<HostStatus> drained = status("a", DRAINING);
    expect(maintenance.getStatus(hostnames)).andReturn(none);
    expect(maintenance.startMaintenance(hostnames)).andReturn(scheduled);
    expect(maintenance.drain(hostnames)).andReturn(draining);
    expect(maintenance.getStatus(hostnames)).andReturn(draining);
    expect(maintenance.getStatus(hostnames)).andReturn(drained);
    expect(maintenance.endMaintenance(hostnames)).andReturn(none);

    control.replay();

    Hosts hosts = Hosts.create(hostnames);

    assertEquals(
        none,
        thrift.maintenanceStatus(hosts).getResult().getMaintenanceStatusResult().getStatuses());
    assertEquals(
        scheduled,
        thrift.startMaintenance(hosts).getResult().getStartMaintenanceResult().getStatuses());
    assertEquals(
        draining,
        thrift.drainHosts(hosts).getResult().getDrainHostsResult().getStatuses());
    assertEquals(
        draining,
        thrift.maintenanceStatus(hosts).getResult().getMaintenanceStatusResult().getStatuses());
    assertEquals(
        drained,
        thrift.maintenanceStatus(hosts).getResult().getMaintenanceStatusResult().getStatuses());
    assertEquals(
        none,
        thrift.endMaintenance(hosts).getResult().getEndMaintenanceResult().getStatuses());
  }

  private static Response okEmptyResponse() {
    return response(OK, Optional.absent());
  }

  private static Response invalidResponse(String message) {
    return Responses.invalidRequest(message);
  }

  @Test
  public void testSnapshot() throws Exception {
    storageUtil.storage.snapshot();
    expectLastCall();

    storageUtil.storage.snapshot();
    expectLastCall().andThrow(new StorageException("mock error!"));

    control.replay();

    assertOkResponse(thrift.snapshot());

    try {
      thrift.snapshot();
      fail("No StorageException thrown.");
    } catch (StorageException e) {
      // Expected.
    }
  }

  private static AddInstancesConfig createInstanceConfig(TaskConfig task) {
    return AddInstancesConfig.builder()
        .setTaskConfig(task)
        .setInstanceIds(ImmutableSet.of(0))
        .setKey(JOB_KEY)
        .build();
  }

  @Test
  public void testAddInstances() throws Exception {
    TaskConfig populatedTask = populatedTask();
    expectNoCronJob();
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.of(LOCK));
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expect(taskIdGenerator.generate(populatedTask, 1))
        .andReturn(TASK_ID);
    expectInstanceQuotaCheck(populatedTask, ENOUGH_QUOTA);
    stateManager.insertPendingTasks(
        storageUtil.mutableStoreProvider,
        populatedTask,
        ImmutableSet.of(0));

    control.replay();

    // Validate key is populated during sanitizing.
    AddInstancesConfig config = createInstanceConfig(populatedTask)
        .withTaskConfig(tc -> tc.withJob((JobKey) null));
    assertOkResponse(thrift.addInstances(config, LOCK));
  }

  @Test
  public void testAddInstancesWithNullLock() throws Exception {
    TaskConfig populatedTask = populatedTask();
    AddInstancesConfig config = createInstanceConfig(populatedTask);
    expectNoCronJob();
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.empty());
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expect(taskIdGenerator.generate(populatedTask, 1))
        .andReturn(TASK_ID);
    expectInstanceQuotaCheck(populatedTask, ENOUGH_QUOTA);
    stateManager.insertPendingTasks(
        storageUtil.mutableStoreProvider,
        populatedTask,
        ImmutableSet.of(0));

    control.replay();

    assertOkResponse(thrift.addInstances(config, null));
  }

  @Test
  public void testAddInstancesFailsConfigCheck() throws Exception {
    AddInstancesConfig config = createInstanceConfig(defaultTask(true).withJobName((String) null));

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.addInstances(config, LOCK));
  }

  @Test
  public void testAddInstancesFailsCronJob() throws Exception {
    AddInstancesConfig config = createInstanceConfig(defaultTask(true));
    expectCronJob();

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.addInstances(config, LOCK));
  }

  @Test(expected = StorageException.class)
  public void testAddInstancesFailsWithNonTransient() throws Exception {
    AddInstancesConfig config = createInstanceConfig(defaultTask(true));
    expect(storageUtil.jobStore.fetchJob(JOB_KEY)).andThrow(new StorageException("no retry"));

    control.replay();

    thrift.addInstances(config, LOCK);
  }

  @Test
  public void testAddInstancesLockCheckFails() throws Exception {
    AddInstancesConfig config = createInstanceConfig(defaultTask(true));
    expectNoCronJob();
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.of(LOCK));
    expectLastCall().andThrow(new LockException("Failed lock check."));

    control.replay();

    assertResponse(LOCK_ERROR, thrift.addInstances(config, LOCK));
  }

  @Test
  public void testAddInstancesFailsTaskIdLength() throws Exception {
    TaskConfig populatedTask = populatedTask();
    AddInstancesConfig config = createInstanceConfig(populatedTask);
    expectNoCronJob();
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.of(LOCK));
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expect(quotaManager.checkInstanceAddition(
        anyObject(TaskConfig.class),
        anyInt(),
        eq(storageUtil.mutableStoreProvider))).andReturn(ENOUGH_QUOTA);
    expect(taskIdGenerator.generate(populatedTask, 1))
        .andReturn(Strings.repeat("a", MAX_TASK_ID_LENGTH + 1));

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.addInstances(config, LOCK));
  }

  @Test
  public void testAddInstancesFailsQuotaCheck() throws Exception {
    TaskConfig populatedTask = populatedTask();
    AddInstancesConfig config = createInstanceConfig(populatedTask);
    expectNoCronJob();
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.of(LOCK));
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expect(taskIdGenerator.generate(populatedTask, 1))
        .andReturn(TASK_ID);
    expectInstanceQuotaCheck(populatedTask, NOT_ENOUGH_QUOTA);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.addInstances(config, LOCK));
  }

  @Test
  public void testAddInstancesInstanceCollisionFailure() throws Exception {
    TaskConfig populatedTask = populatedTask();
    AddInstancesConfig config = createInstanceConfig(populatedTask);
    expectNoCronJob();
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.of(LOCK));
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expect(taskIdGenerator.generate(populatedTask, 1))
        .andReturn(TASK_ID);
    expectInstanceQuotaCheck(populatedTask, ENOUGH_QUOTA);
    stateManager.insertPendingTasks(
        storageUtil.mutableStoreProvider,
        populatedTask,
        ImmutableSet.of(0));
    expectLastCall().andThrow(new IllegalArgumentException("instance collision"));

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.addInstances(config, LOCK));
  }

  @Test
  public void testAcquireLock() throws Exception {
    expectGetRemoteUser();
    expect(lockManager.acquireLock(LOCK_KEY, USER)).andReturn(LOCK);

    control.replay();

    Response response = thrift.acquireLock(LOCK_KEY);
    assertEquals(LOCK, response.getResult().getAcquireLockResult().getLock());
  }

  @Test
  public void testAcquireLockFailed() throws Exception {
    expectGetRemoteUser();
    expect(lockManager.acquireLock(LOCK_KEY, USER))
        .andThrow(new LockException("Failed"));

    control.replay();

    assertResponse(LOCK_ERROR, thrift.acquireLock(LOCK_KEY));
  }

  @Test
  public void testReleaseLock() throws Exception {
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.of(LOCK));
    lockManager.releaseLock(LOCK);

    control.replay();

    assertOkResponse(thrift.releaseLock(LOCK, CHECKED));
  }

  @Test
  public void testReleaseLockFailed() throws Exception {
    lockManager.validateIfLocked(LOCK_KEY, java.util.Optional.of(LOCK));
    expectLastCall().andThrow(new LockException("Failed"));

    control.replay();

    assertResponse(LOCK_ERROR, thrift.releaseLock(LOCK, CHECKED));
  }

  @Test
  public void testReleaseLockUnchecked() throws Exception {
    lockManager.releaseLock(LOCK);

    control.replay();

    assertEquals(okEmptyResponse(), thrift.releaseLock(LOCK, UNCHECKED));
  }

  @Test
  public void testStartUpdate() throws Exception {
    expectGetRemoteUser();
    expectNoCronJob();

    TaskConfig newTask = buildTaskForJobUpdate(0).getAssignedTask().getTask();
    expect(taskIdGenerator.generate(newTask, 6)).andReturn(TASK_ID);

    ScheduledTask oldTask1 = buildTaskForJobUpdate(0, "old");
    ScheduledTask oldTask2 = buildTaskForJobUpdate(1, "old");
    ScheduledTask oldTask3 = buildTaskForJobUpdate(2, "old2");
    ScheduledTask oldTask4 = buildTaskForJobUpdate(3, "old2");
    ScheduledTask oldTask5 = buildTaskForJobUpdate(4, "old");
    ScheduledTask oldTask6 = buildTaskForJobUpdate(5, "old");
    ScheduledTask oldTask7 = buildTaskForJobUpdate(6, "old");

    JobUpdate update = buildJobUpdate(6, newTask, ImmutableMap.of(
        oldTask1.getAssignedTask().getTask(),
        ImmutableSet.of(Range.create(0, 1), Range.create(4, 6)),
        oldTask3.getAssignedTask().getTask(),
        ImmutableSet.of(Range.create(2, 3))
    ));

    expect(quotaManager.checkJobUpdate(
        update,
        storageUtil.mutableStoreProvider)).andReturn(ENOUGH_QUOTA);

    expect(uuidGenerator.createNew()).andReturn(UU_ID);
    storageUtil.expectTaskFetch(
        Query.unscoped().byJob(JOB_KEY).active(),
        oldTask1,
        oldTask2,
        oldTask3,
        oldTask4,
        oldTask5,
        oldTask6,
        oldTask7);

    jobUpdateController.start(update, AUDIT);

    control.replay();

    // Validate key is populated during sanitizing.
    JobUpdateRequest request = buildJobUpdateRequest(update)
        .withTaskConfig(tc -> tc.withJob((JobKey) null));

    Response response = assertOkResponse(thrift.startJobUpdate(request, AUDIT_MESSAGE));
    assertEquals(
        StartJobUpdateResult.create(UPDATE_KEY),
        response.getResult().getStartJobUpdateResult());
  }

  private void expectJobUpdateQuotaCheck(QuotaCheckResult result) {
    expect(quotaManager.checkJobUpdate(
        anyObject(JobUpdate.class),
        eq(storageUtil.mutableStoreProvider))).andReturn(result);
  }

  @Test
  public void testStartUpdateEmptyDesired() throws Exception {
    expectGetRemoteUser();
    expectNoCronJob();

    TaskConfig newTask = buildTaskForJobUpdate(0).getAssignedTask().getTask();
    expect(taskIdGenerator.generate(newTask, 1)).andReturn(TASK_ID);

    ScheduledTask oldTask1 = buildTaskForJobUpdate(0);
    ScheduledTask oldTask2 = buildTaskForJobUpdate(1);

    // Set instance count to 1 to generate empty desired state in diff.
    JobUpdate update = buildJobUpdate(
        1,
        newTask,
        ImmutableMap.of(oldTask1.getAssignedTask().getTask(), ImmutableSet.of(Range.create(0, 1))));

    expectJobUpdateQuotaCheck(ENOUGH_QUOTA);

    // Set diff-adjusted JobUpdate expectations.
    JobUpdate expected = update.withInstructions(
        inst -> inst.toBuilder()
            .setInitialState(
                InstanceTaskConfig.create(newTask, ImmutableSet.of(Range.create(1, 1))))
            .setDesiredState(null)
            .build());

    expect(uuidGenerator.createNew()).andReturn(UU_ID);
    storageUtil.expectTaskFetch(
        Query.unscoped().byJob(JOB_KEY).active(),
        oldTask1,
        oldTask2);

    jobUpdateController.start(expected, AUDIT);

    control.replay();

    Response response = assertOkResponse(
        thrift.startJobUpdate(buildJobUpdateRequest(update), AUDIT_MESSAGE));
    assertEquals(
        StartJobUpdateResult.create(UPDATE_KEY),
        response.getResult().getStartJobUpdateResult());
  }

  @Test(expected = NullPointerException.class)
  public void testStartUpdateFailsNullRequest() throws Exception {
    control.replay();
    thrift.startJobUpdate(null, AUDIT_MESSAGE);
  }

  @Test(expected = NullPointerException.class)
  public void testStartUpdateFailsNullTaskConfig() throws Exception {
    control.replay();
    thrift.startJobUpdate(
        JobUpdateRequest.create(null, 5, buildJobUpdateSettings()),
        AUDIT_MESSAGE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testStartUpdateFailsInvalidJobKey() throws Exception {
    control.replay();
    thrift.startJobUpdate(
        JobUpdateRequest.create(
            TaskConfig.builder()
                .setJobName("&")
                .setEnvironment("devel")
                .setOwner(Identity.create(ROLE, null))
                .build(),
            5,
            buildJobUpdateSettings()),
        AUDIT_MESSAGE);
  }

  @Test
  public void testStartUpdateFailsInvalidGroupSize() throws Exception {
    control.replay();

    JobUpdateRequest updateRequest = buildServiceJobUpdateRequest()
        .withSettings(s -> s.withUpdateGroupSize(0));

    assertEquals(
        invalidResponse(SchedulerThriftInterface.INVALID_GROUP_SIZE),
        thrift.startJobUpdate(updateRequest, AUDIT_MESSAGE));
  }

  @Test
  public void testStartUpdateFailsInvalidMaxInstanceFailures() throws Exception {
    control.replay();

    JobUpdateRequest updateRequest = buildServiceJobUpdateRequest()
        .withSettings(s -> s.withMaxPerInstanceFailures(-1));

    assertEquals(
        invalidResponse(SchedulerThriftInterface.INVALID_MAX_INSTANCE_FAILURES),
        thrift.startJobUpdate(updateRequest, AUDIT_MESSAGE));
  }

  @Test
  public void testStartUpdateFailsTooManyPerInstanceFailures() throws Exception {
    control.replay();

    JobUpdateRequest updateRequest = buildServiceJobUpdateRequest()
        .withSettings(
            s -> s.withMaxPerInstanceFailures(THRESHOLDS.getMaxUpdateInstanceFailures() + 10));

    assertEquals(
        invalidResponse(SchedulerThriftInterface.TOO_MANY_POTENTIAL_FAILED_INSTANCES),
        thrift.startJobUpdate(updateRequest, AUDIT_MESSAGE));
  }

  @Test
  public void testStartUpdateFailsInvalidMaxFailedInstances() throws Exception {
    control.replay();

    JobUpdateRequest updateRequest = buildServiceJobUpdateRequest()
        .withSettings(s -> s.withMaxFailedInstances(-1));

    assertEquals(
        invalidResponse(SchedulerThriftInterface.INVALID_MAX_FAILED_INSTANCES),
        thrift.startJobUpdate(updateRequest, AUDIT_MESSAGE));
  }

  @Test
  public void testStartUpdateFailsInvalidMinWaitInRunning() throws Exception {
    control.replay();

    JobUpdateRequest updateRequest = buildServiceJobUpdateRequest()
        .withSettings(s -> s.withMinWaitInInstanceRunningMs(-1));

    assertEquals(
        invalidResponse(SchedulerThriftInterface.INVALID_MIN_WAIT_TO_RUNNING),
        thrift.startJobUpdate(updateRequest, AUDIT_MESSAGE));
  }

  @Test
  public void testStartUpdateFailsNonServiceTask() throws Exception {
    control.replay();
    JobUpdateRequest request = buildJobUpdateRequest(populatedTask().withIsService(false));
    assertResponse(INVALID_REQUEST, thrift.startJobUpdate(request, AUDIT_MESSAGE));
  }

  @Test
  public void testStartUpdateFailsInvalidPulseTimeout() throws Exception {
    control.replay();

    JobUpdateRequest updateRequest = buildServiceJobUpdateRequest()
        .withSettings(s -> s.withBlockIfNoPulsesAfterMs(-1));

    assertEquals(
        invalidResponse(SchedulerThriftInterface.INVALID_PULSE_TIMEOUT),
        thrift.startJobUpdate(updateRequest, AUDIT_MESSAGE));
  }

  @Test
  public void testStartUpdateFailsForCronJob() throws Exception {
    JobUpdateRequest request = buildServiceJobUpdateRequest(populatedTask());
    expectCronJob();

    control.replay();
    assertEquals(invalidResponse(NO_CRON), thrift.startJobUpdate(request, AUDIT_MESSAGE));
  }

  @Test
  public void testStartUpdateFailsConfigValidation() throws Exception {
    JobUpdateRequest request = buildJobUpdateRequest(populatedTask().toBuilder()
        .setIsService(true)
        .setNumCpus(-1)
        .build());

    control.replay();
    assertResponse(INVALID_REQUEST, thrift.startJobUpdate(request, AUDIT_MESSAGE));
  }

  @Test
  public void testStartNoopUpdate() throws Exception {
    expectNoCronJob();
    expect(uuidGenerator.createNew()).andReturn(UU_ID);
    TaskConfig newTask = buildTaskForJobUpdate(0).getAssignedTask().getTask();

    ScheduledTask oldTask = buildTaskForJobUpdate(0);
    storageUtil.expectTaskFetch(Query.unscoped().byJob(JOB_KEY).active(), oldTask);

    JobUpdate update = buildJobUpdate(
        1,
        newTask,
        ImmutableMap.of(oldTask.getAssignedTask().getTask(), ImmutableSet.of(Range.create(0, 0))));

    control.replay();
    JobUpdateRequest request = buildJobUpdateRequest(update);
    Response response = thrift.startJobUpdate(request, AUDIT_MESSAGE);
    assertResponse(OK, response);
    assertEquals(
        NOOP_JOB_UPDATE_MESSAGE,
        Iterables.getOnlyElement(response.getDetails()).getMessage());
  }

  @Test
  public void testStartUpdateInvalidScope() throws Exception {
    expectNoCronJob();
    expect(uuidGenerator.createNew()).andReturn(UU_ID);

    ScheduledTask oldTask = buildTaskForJobUpdate(0);
    storageUtil.expectTaskFetch(Query.unscoped().byJob(JOB_KEY).active(), oldTask);

    TaskConfig newTask = buildTaskForJobUpdate(0).getAssignedTask().getTask();
    JobUpdate builder = buildJobUpdate(
        1,
        newTask,
        ImmutableMap.of(oldTask.getAssignedTask().getTask(), ImmutableSet.of(Range.create(0, 0))))
        .withInstructions(
            inst -> inst.withSettings(
                s -> s.withUpdateOnlyTheseInstances(ImmutableSet.of(Range.create(100, 100)))));

    control.replay();
    JobUpdateRequest request = buildJobUpdateRequest(builder);
    assertResponse(INVALID_REQUEST, thrift.startJobUpdate(request, AUDIT_MESSAGE));
  }

  @Test
  public void testStartScopeIncludesNoop() throws Exception {
    // Test for regression of AURORA-1332: a scoped update should be allowed when unchanged
    // instances are included in the scope.

    expectGetRemoteUser();
    expectNoCronJob();
    expect(uuidGenerator.createNew()).andReturn(UU_ID);

    ScheduledTask newTask = buildTaskForJobUpdate(0)
        .withAssignedTask(at -> at.withTask(t -> t.withNumCpus(100)));

    ScheduledTask oldTask1 = buildTaskForJobUpdate(1);
    ScheduledTask oldTask2 = buildTaskForJobUpdate(2);
    storageUtil.expectTaskFetch(
        Query.unscoped().byJob(JOB_KEY).active(),
        newTask, oldTask1, oldTask2);
    expectJobUpdateQuotaCheck(ENOUGH_QUOTA);
    expect(taskIdGenerator.generate(EasyMock.<TaskConfig>anyObject(), anyInt()))
        .andReturn(TASK_ID);
    jobUpdateController.start(EasyMock.<JobUpdate>anyObject(), eq(AUDIT));

    TaskConfig newConfig = newTask.getAssignedTask().getTask();
    JobUpdate builder = buildJobUpdate(
        3,
        newConfig,
        ImmutableMap.of(
            newConfig, ImmutableSet.of(Range.create(0, 0)),
            oldTask1.getAssignedTask().getTask(), ImmutableSet.of(Range.create(1, 2))))
        .withInstructions(
            inst -> inst.withSettings(
                s -> s.withUpdateOnlyTheseInstances(ImmutableSet.of(Range.create(0, 2)))));

    control.replay();
    JobUpdateRequest request = buildJobUpdateRequest(builder);
    assertResponse(OK, thrift.startJobUpdate(request, AUDIT_MESSAGE));
  }

  @Test
  public void testStartUpdateFailsInstanceCountCheck() throws Exception {
    JobUpdateRequest request = buildServiceJobUpdateRequest(populatedTask())
        .withInstanceCount(THRESHOLDS.getMaxTasksPerJob() + 1);
    expectGetRemoteUser();
    expectNoCronJob();
    expect(uuidGenerator.createNew()).andReturn(UU_ID);
    storageUtil.expectTaskFetch(Query.unscoped().byJob(JOB_KEY).active());
    expectJobUpdateQuotaCheck(ENOUGH_QUOTA);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.startJobUpdate(request, AUDIT_MESSAGE));
  }

  @Test
  public void testStartUpdateFailsTaskIdLength() throws Exception {
    JobUpdateRequest request = buildServiceJobUpdateRequest(populatedTask());
    expectGetRemoteUser();
    expectNoCronJob();
    expect(uuidGenerator.createNew()).andReturn(UU_ID);
    storageUtil.expectTaskFetch(Query.unscoped().byJob(JOB_KEY).active());
    expectJobUpdateQuotaCheck(ENOUGH_QUOTA);

    expect(taskIdGenerator.generate(request.getTaskConfig(), 6))
        .andReturn(Strings.repeat("a", MAX_TASK_ID_LENGTH + 1));

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.startJobUpdate(request, AUDIT_MESSAGE));
  }

  @Test
  public void testStartUpdateFailsQuotaCheck() throws Exception {
    JobUpdateRequest request = buildServiceJobUpdateRequest(populatedTask());
    expectGetRemoteUser();
    expectNoCronJob();
    expect(uuidGenerator.createNew()).andReturn(UU_ID);

    ScheduledTask oldTask = buildTaskForJobUpdate(0);
    storageUtil.expectTaskFetch(Query.unscoped().byJob(JOB_KEY).active(), oldTask);
    expect(taskIdGenerator.generate(request.getTaskConfig(), 6)).andReturn(TASK_ID);

    expectJobUpdateQuotaCheck(NOT_ENOUGH_QUOTA);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.startJobUpdate(request, AUDIT_MESSAGE));
  }

  @Test
  public void testStartUpdateFailsInController() throws Exception {
    expectGetRemoteUser();
    expectNoCronJob();

    ScheduledTask oldTask = buildTaskForJobUpdate(0, "old");
    TaskConfig newTask = buildTaskForJobUpdate(0, "new").getAssignedTask().getTask();

    JobUpdate update = buildJobUpdate(
        1,
        newTask,
        ImmutableMap.of(oldTask.getAssignedTask().getTask(), ImmutableSet.of(Range.create(0, 0))));

    expect(uuidGenerator.createNew()).andReturn(UU_ID);
    expect(taskIdGenerator.generate(newTask, 1)).andReturn(TASK_ID);
    expectJobUpdateQuotaCheck(ENOUGH_QUOTA);

    storageUtil.expectTaskFetch(Query.unscoped().byJob(JOB_KEY).active(), oldTask);
    jobUpdateController.start(update, AUDIT);
    expectLastCall().andThrow(new UpdateStateException("failed"));

    control.replay();

    assertResponse(
        INVALID_REQUEST,
        thrift.startJobUpdate(buildJobUpdateRequest(update), AUDIT_MESSAGE));
  }

  @Test
  public void testPauseJobUpdateByCoordinator() throws Exception {
    expectGetRemoteUser();
    jobUpdateController.pause(UPDATE_KEY, AUDIT);

    control.replay();

    assertResponse(OK, thrift.pauseJobUpdate(UPDATE_KEY, AUDIT_MESSAGE));
  }

  @Test
  public void testPauseJobUpdateByUser() throws Exception {
    expectGetRemoteUser();
    jobUpdateController.pause(UPDATE_KEY, AUDIT);

    control.replay();

    assertResponse(OK, thrift.pauseJobUpdate(UPDATE_KEY, AUDIT_MESSAGE));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPauseMessageTooLong() throws Exception {
    expectGetRemoteUser();

    control.replay();

    assertResponse(
        OK,
        thrift.pauseJobUpdate(
            UPDATE_KEY,
            Strings.repeat("*", AuditData.MAX_MESSAGE_LENGTH + 1)));
  }

  @Test
  public void testPauseJobUpdateFailsInController() throws Exception {
    expectGetRemoteUser();
    jobUpdateController.pause(UPDATE_KEY, AUDIT);
    expectLastCall().andThrow(new UpdateStateException("failed"));

    control.replay();

    assertResponse(
        INVALID_REQUEST,
        thrift.pauseJobUpdate(UPDATE_KEY, AUDIT_MESSAGE));
  }

  @Test
  public void testResumeJobUpdate() throws Exception {
    expectGetRemoteUser();
    jobUpdateController.resume(UPDATE_KEY, AUDIT);

    control.replay();

    assertResponse(OK, thrift.resumeJobUpdate(UPDATE_KEY, AUDIT_MESSAGE));
  }

  @Test
  public void testResumeJobUpdateFailsInController() throws Exception {
    expectGetRemoteUser();
    jobUpdateController.resume(UPDATE_KEY, AUDIT);
    expectLastCall().andThrow(new UpdateStateException("failed"));

    control.replay();

    assertResponse(
        INVALID_REQUEST,
        thrift.resumeJobUpdate(UPDATE_KEY, AUDIT_MESSAGE));
  }

  @Test
  public void testAbortJobUpdateByCoordinator() throws Exception {
    expectGetRemoteUser();
    jobUpdateController.abort(UPDATE_KEY, AUDIT);

    control.replay();

    assertResponse(OK, thrift.abortJobUpdate(UPDATE_KEY, AUDIT_MESSAGE));
  }

  @Test
  public void testAbortJobUpdateByUser() throws Exception {
    expectGetRemoteUser();
    jobUpdateController.abort(UPDATE_KEY, AUDIT);

    control.replay();

    assertResponse(OK, thrift.abortJobUpdate(UPDATE_KEY, AUDIT_MESSAGE));
  }

  @Test
  public void testAbortJobUpdateFailsInController() throws Exception {
    expectGetRemoteUser();
    jobUpdateController.abort(UPDATE_KEY, AUDIT);
    expectLastCall().andThrow(new UpdateStateException("failed"));

    control.replay();

    assertResponse(
        INVALID_REQUEST,
        thrift.abortJobUpdate(UPDATE_KEY, AUDIT_MESSAGE));
  }

  @Test
  public void testPulseJobUpdatePulsedAsCoordinator() throws Exception {
    expect(jobUpdateController.pulse(UPDATE_KEY)).andReturn(JobUpdatePulseStatus.OK);

    control.replay();

    assertEquals(
        okResponse(Result.pulseJobUpdateResult(
            PulseJobUpdateResult.create(JobUpdatePulseStatus.OK))),
        thrift.pulseJobUpdate(UPDATE_KEY));
  }

  @Test
  public void testPulseJobUpdatePulsedAsUser() throws Exception {
    expect(jobUpdateController.pulse(UPDATE_KEY)).andReturn(JobUpdatePulseStatus.OK);

    control.replay();

    assertEquals(
        okResponse(Result.pulseJobUpdateResult(
            PulseJobUpdateResult.create(JobUpdatePulseStatus.OK))),
        thrift.pulseJobUpdate(UPDATE_KEY));
  }

  @Test
  public void testPulseJobUpdateFails() throws Exception {
    expect(jobUpdateController.pulse(UPDATE_KEY)).andThrow(new UpdateStateException("failure"));

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.pulseJobUpdate(UPDATE_KEY));
  }

  @Test
  public void testGetJobUpdateSummaries() throws Exception {
    Response updateSummary = Responses.ok("summary");

    expect(readOnlyScheduler.getJobUpdateSummaries(
        anyObject(JobUpdateQuery.class))).andReturn(updateSummary);
    control.replay();

    assertEquals(updateSummary, thrift.getJobUpdateSummaries(JobUpdateQuery.builder().build()));
  }

  private IExpectationSetters<String> expectGetRemoteUser() {
    return expect(auditMessages.getRemoteUserName()).andReturn(USER);
  }

  private static TaskConfig populatedTask() {
    return defaultTask(true).withConstraints(ImmutableSet.of(
        Constraint.create("host", TaskConstraint.limit(LimitConstraint.create(1)))));
  }

  private static Constraint dedicatedConstraint(int value) {
    return Constraint.create(DEDICATED_ATTRIBUTE,
        TaskConstraint.limit(LimitConstraint.create(value)));
  }

  private static Constraint dedicatedConstraint(Set<String> values) {
    return Constraint.create(DEDICATED_ATTRIBUTE,
        TaskConstraint.value(ValueConstraint.create(false, values)));
  }

  private static JobUpdateRequest buildServiceJobUpdateRequest() {
    return buildServiceJobUpdateRequest(defaultTask(true));
  }

  private static JobUpdateRequest buildServiceJobUpdateRequest(TaskConfig config) {
    return buildJobUpdateRequest(config.withIsService(true));
  }

  private static JobUpdateRequest buildJobUpdateRequest(TaskConfig config) {
    return JobUpdateRequest.builder()
        .setInstanceCount(6)
        .setSettings(buildJobUpdateSettings())
        .setTaskConfig(config)
        .build();
  }

  private static JobUpdateSettings buildJobUpdateSettings() {
    return JobUpdateSettings.builder()
        .setUpdateGroupSize(10)
        .setMaxFailedInstances(2)
        .setMaxPerInstanceFailures(1)
        .setMaxWaitToInstanceRunningMs(30000)
        .setMinWaitInInstanceRunningMs(15000)
        .setRollbackOnFailure(true)
        .build();
  }

  private static Integer rangesToInstanceCount(Set<Range> ranges) {
    int instanceCount = 0;
    for (Range range : ranges) {
      instanceCount += range.getLast() - range.getFirst() + 1;
    }

    return instanceCount;
  }

  private static JobUpdateRequest buildJobUpdateRequest(JobUpdate update) {
    return JobUpdateRequest.builder()
        .setInstanceCount(rangesToInstanceCount(
            update.getInstructions().getDesiredState().getInstances()))
        .setSettings(update.getInstructions().getSettings())
        .setTaskConfig(update.getInstructions().getDesiredState().getTask())
        .build();
  }

  private static JobUpdate buildJobUpdate(
      int instanceCount,
      TaskConfig newConfig,
      ImmutableMap<TaskConfig, ImmutableSet<Range>> oldConfigMap) {

    ImmutableSet.Builder<InstanceTaskConfig> builder = ImmutableSet.builder();
    for (Map.Entry<TaskConfig, ImmutableSet<Range>> entry : oldConfigMap.entrySet()) {
      builder.add(InstanceTaskConfig.create(entry.getKey(), entry.getValue()));
    }

    return JobUpdate.builder()
        .setSummary(JobUpdateSummary.builder()
            .setKey(UPDATE_KEY)
            .setUser(ROLE_IDENTITY.getUser())
            .build())
        .setInstructions(JobUpdateInstructions.builder()
            .setSettings(buildJobUpdateSettings())
            .setDesiredState(InstanceTaskConfig.builder()
                .setTask(newConfig)
                .setInstances(Range.create(0, instanceCount - 1))
                .build())
            .setInitialState(builder.build())
            .build())
        .build();
  }

  private IExpectationSetters<?> expectCronJob() {
    return expect(storageUtil.jobStore.fetchJob(JOB_KEY))
        .andReturn(Optional.of(CRON_JOB));
  }

  private IExpectationSetters<?> expectNoCronJob() {
    return expect(storageUtil.jobStore.fetchJob(JOB_KEY))
        .andReturn(Optional.absent());
  }

  private IExpectationSetters<?> expectInstanceQuotaCheck(
      SanitizedConfiguration sanitized,
      QuotaCheckResult result) {

    return expectInstanceQuotaCheck(sanitized.getJobConfig().getTaskConfig(), result);
  }

  private IExpectationSetters<?> expectInstanceQuotaCheck(
      TaskConfig config,
      QuotaCheckResult result) {

    return expect(quotaManager.checkInstanceAddition(
        config,
        1,
        storageUtil.mutableStoreProvider)).andReturn(result);
  }

  private IExpectationSetters<?> expectCronQuotaCheck(
      JobConfiguration config,
      QuotaCheckResult result) {

    return expect(quotaManager.checkCronUpdate(config, storageUtil.mutableStoreProvider))
        .andReturn(result);
  }
}
