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

import java.util.Date;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.ConfigGroup;
import org.apache.aurora.gen.ConfigSummary;
import org.apache.aurora.gen.ConfigSummaryResult;
import org.apache.aurora.gen.GetJobUpdateDiffResult;
import org.apache.aurora.gen.GetQuotaResult;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobStats;
import org.apache.aurora.gen.JobSummary;
import org.apache.aurora.gen.JobSummaryResult;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.JobUpdateRequest;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.PendingReason;
import org.apache.aurora.gen.PopulateJobResult;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.ReadOnlyScheduler;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.ResponseDetail;
import org.apache.aurora.gen.Result;
import org.apache.aurora.gen.RoleSummary;
import org.apache.aurora.gen.RoleSummaryResult;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Query.Builder;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.configuration.SanitizedConfiguration;
import org.apache.aurora.scheduler.cron.CronPredictor;
import org.apache.aurora.scheduler.cron.CrontabEntry;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.metadata.NearestFit;
import org.apache.aurora.scheduler.quota.QuotaInfo;
import org.apache.aurora.scheduler.quota.QuotaManager;
import org.apache.aurora.scheduler.state.LockManager;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ResponseCode.INVALID_REQUEST;
import static org.apache.aurora.scheduler.ResourceAggregates.LARGE;
import static org.apache.aurora.scheduler.ResourceAggregates.MEDIUM;
import static org.apache.aurora.scheduler.ResourceAggregates.SMALL;
import static org.apache.aurora.scheduler.ResourceAggregates.XLARGE;
import static org.apache.aurora.scheduler.base.Numbers.convertRanges;
import static org.apache.aurora.scheduler.base.Numbers.toRanges;
import static org.apache.aurora.scheduler.thrift.Fixtures.CRON_JOB;
import static org.apache.aurora.scheduler.thrift.Fixtures.CRON_SCHEDULE;
import static org.apache.aurora.scheduler.thrift.Fixtures.JOB_KEY;
import static org.apache.aurora.scheduler.thrift.Fixtures.LOCK;
import static org.apache.aurora.scheduler.thrift.Fixtures.QUOTA;
import static org.apache.aurora.scheduler.thrift.Fixtures.ROLE;
import static org.apache.aurora.scheduler.thrift.Fixtures.ROLE_IDENTITY;
import static org.apache.aurora.scheduler.thrift.Fixtures.UPDATE_KEY;
import static org.apache.aurora.scheduler.thrift.Fixtures.USER;
import static org.apache.aurora.scheduler.thrift.Fixtures.assertOkResponse;
import static org.apache.aurora.scheduler.thrift.Fixtures.assertResponse;
import static org.apache.aurora.scheduler.thrift.Fixtures.defaultTask;
import static org.apache.aurora.scheduler.thrift.Fixtures.jobSummaryResponse;
import static org.apache.aurora.scheduler.thrift.Fixtures.makeDefaultScheduledTasks;
import static org.apache.aurora.scheduler.thrift.Fixtures.makeJob;
import static org.apache.aurora.scheduler.thrift.Fixtures.nonProductionTask;
import static org.apache.aurora.scheduler.thrift.ReadOnlySchedulerImpl.NO_CRON;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReadOnlySchedulerImplTest extends EasyMockTest {
  private StorageTestUtil storageUtil;
  private NearestFit nearestFit;
  private CronPredictor cronPredictor;
  private QuotaManager quotaManager;
  private LockManager lockManager;

  private ReadOnlyScheduler.Iface thrift;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    nearestFit = createMock(NearestFit.class);
    cronPredictor = createMock(CronPredictor.class);
    quotaManager = createMock(QuotaManager.class);
    lockManager = createMock(LockManager.class);

    thrift = new ReadOnlySchedulerImpl(
        TaskTestUtil.CONFIGURATION_MANAGER,
        storageUtil.storage,
        nearestFit,
        cronPredictor,
        quotaManager,
        lockManager);
  }

  @Test
  public void testGetJobSummary() throws Exception {
    long nextCronRunMs = 100;
    TaskConfig ownedCronJobTask = nonProductionTask()
        .setJob(JOB_KEY.newBuilder())
        .setJobName(JOB_KEY.getName())
        .setOwner(ROLE_IDENTITY)
        .setEnvironment(JOB_KEY.getEnvironment());
    JobConfiguration ownedCronJob = makeJob()
        .setCronSchedule(CRON_SCHEDULE)
        .setTaskConfig(ownedCronJobTask);
    ScheduledTask ownedCronJobScheduledTask = ScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask().setTask(ownedCronJobTask))
        .setStatus(ScheduleStatus.ASSIGNED));
    Identity otherOwner = new Identity("other", "other");
    JobConfiguration unownedCronJob = makeJob()
        .setOwner(otherOwner)
        .setCronSchedule(CRON_SCHEDULE)
        .setKey(JOB_KEY.newBuilder().setRole("other"))
        .setTaskConfig(ownedCronJobTask.deepCopy().setOwner(otherOwner));
    TaskConfig ownedImmediateTaskInfo = defaultTask(false)
        .setJob(JOB_KEY.newBuilder().setName("immediate"))
        .setJobName("immediate")
        .setOwner(ROLE_IDENTITY);
    Set<JobConfiguration> ownedCronJobOnly = ImmutableSet.of(ownedCronJob);
    Set<JobSummary> ownedCronJobSummaryOnly = ImmutableSet.of(
        new JobSummary()
            .setJob(ownedCronJob)
            .setStats(new JobStats())
            .setNextCronRunMs(nextCronRunMs));
    Set<JobSummary> ownedCronJobSummaryWithRunningTask = ImmutableSet.of(
        new JobSummary()
            .setJob(ownedCronJob)
            .setStats(new JobStats().setActiveTaskCount(1))
            .setNextCronRunMs(nextCronRunMs));
    Set<JobConfiguration> unownedCronJobOnly = ImmutableSet.of(unownedCronJob);
    Set<JobConfiguration> bothCronJobs = ImmutableSet.of(ownedCronJob, unownedCronJob);

    ScheduledTask ownedImmediateTask = ScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask().setTask(ownedImmediateTaskInfo))
        .setStatus(ScheduleStatus.ASSIGNED));
    JobConfiguration ownedImmediateJob = new JobConfiguration()
        .setKey(JOB_KEY.newBuilder().setName("immediate"))
        .setOwner(ROLE_IDENTITY)
        .setInstanceCount(1)
        .setTaskConfig(ownedImmediateTaskInfo);
    Builder query = Query.roleScoped(ROLE);

    Set<JobSummary> ownedImmediateJobSummaryOnly = ImmutableSet.of(
        new JobSummary().setJob(ownedImmediateJob).setStats(new JobStats().setActiveTaskCount(1)));

    expect(cronPredictor.predictNextRun(CrontabEntry.parse(CRON_SCHEDULE)))
        .andReturn(Optional.of(new Date(nextCronRunMs)))
        .anyTimes();

    storageUtil.expectTaskFetch(query);
    expect(storageUtil.jobStore.fetchJobs())
        .andReturn(JobConfiguration.setFromBuilders(ownedCronJobOnly));

    storageUtil.expectTaskFetch(query);
    expect(storageUtil.jobStore.fetchJobs())
        .andReturn(JobConfiguration.setFromBuilders(bothCronJobs));

    storageUtil.expectTaskFetch(query, ownedImmediateTask);
    expect(storageUtil.jobStore.fetchJobs())
        .andReturn(JobConfiguration.setFromBuilders(unownedCronJobOnly));

    storageUtil.expectTaskFetch(query);
    expect(storageUtil.jobStore.fetchJobs()).andReturn(ImmutableSet.of());

    // Handle the case where a cron job has a running task (same JobKey present in both stores).
    storageUtil.expectTaskFetch(query, ownedCronJobScheduledTask);
    expect(storageUtil.jobStore.fetchJobs())
        .andReturn(JobConfiguration.setFromBuilders(ImmutableSet.of(ownedCronJob)));

    control.replay();

    assertEquals(jobSummaryResponse(ownedCronJobSummaryOnly), thrift.getJobSummary(ROLE));

    assertEquals(jobSummaryResponse(ownedCronJobSummaryOnly), thrift.getJobSummary(ROLE));

    Response jobSummaryResponse = thrift.getJobSummary(ROLE);
    assertEquals(
        jobSummaryResponse(ownedImmediateJobSummaryOnly),
        Response.build(jobSummaryResponse).newBuilder());

    assertEquals(jobSummaryResponse(ImmutableSet.of()), thrift.getJobSummary(ROLE));

    assertEquals(jobSummaryResponse(ownedCronJobSummaryWithRunningTask),
        thrift.getJobSummary(ROLE));
  }

  @Test
  public void testGetJobSummaryWithoutNextRun() throws Exception {
    // 31st of February, there is no such day.
    String cronSchedule = "* * 31 2 *";

    TaskConfig task = nonProductionTask()
        .setJobName(JOB_KEY.getName())
        .setOwner(ROLE_IDENTITY)
        .setEnvironment(JOB_KEY.getEnvironment());
    JobConfiguration job = makeJob()
        .setCronSchedule(cronSchedule)
        .setTaskConfig(task);
    expect(cronPredictor.predictNextRun(CrontabEntry.parse(cronSchedule)))
        .andReturn(Optional.absent())
        .anyTimes();
    storageUtil.expectTaskFetch(Query.roleScoped(ROLE));
    Set<JobConfiguration> jobOnly = ImmutableSet.of(job);
    expect(storageUtil.jobStore.fetchJobs())
        .andReturn(JobConfiguration.setFromBuilders(jobOnly));

    control.replay();

    JobSummaryResult result = thrift.getJobSummary(ROLE).getResult().getJobSummaryResult();
    assertEquals(1, result.getSummaries().size());
    assertFalse(result.getSummariesIterator().next().isSetNextCronRunMs());
  }

  @Test
  public void testGetPendingReason() throws Exception {
    Builder query = Query.unscoped().byJob(JOB_KEY);
    Builder filterQuery = Query.unscoped().byJob(JOB_KEY).byStatus(ScheduleStatus.PENDING);
    String taskId1 = "task_id_test1";
    String taskId2 = "task_id_test2";
    ImmutableSet<Veto> result = ImmutableSet.of(
        Veto.constraintMismatch("first"),
        Veto.constraintMismatch("second"));

    TaskConfig taskConfig = TaskConfig.build(defaultTask(true));
    ScheduledTask pendingTask1 = ScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setTaskId(taskId1)
            .setTask(taskConfig.newBuilder()))
        .setStatus(ScheduleStatus.PENDING));

    ScheduledTask pendingTask2 = ScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setTaskId(taskId2)
            .setTask(taskConfig.newBuilder()))
        .setStatus(ScheduleStatus.PENDING));

    storageUtil.expectTaskFetch(filterQuery, pendingTask1, pendingTask2);
    expect(nearestFit.getNearestFit(TaskGroupKey.from(taskConfig))).andReturn(result).times(2);

    control.replay();

    String reason = "Constraint not satisfied: first,Constraint not satisfied: second";
    Set<PendingReason> expected = ImmutableSet.of(
        new PendingReason().setTaskId(taskId1).setReason(reason),
        new PendingReason().setTaskId(taskId2).setReason(reason));

    Response response = assertOkResponse(thrift.getPendingReason(query.get()));
    assertEquals(expected, response.getResult().getGetPendingReasonResult().getReasons());
  }

  @Test
  public void testPopulateJobConfig() throws Exception {
    JobConfiguration job = JobConfiguration.build(makeJob());
    SanitizedConfiguration sanitized =
        SanitizedConfiguration.fromUnsanitized(TaskTestUtil.CONFIGURATION_MANAGER, job);
    control.replay();

    Response response = assertOkResponse(thrift.populateJobConfig(job.newBuilder()));
    assertEquals(
        Result.populateJobResult(new PopulateJobResult(
            sanitized.getJobConfig().getTaskConfig().newBuilder())),
        response.getResult());
  }

  @Test
  public void testPopulateJobConfigFails() throws Exception {
    JobConfiguration job = JobConfiguration.build(makeJob(null));
    control.replay();

    assertResponse(INVALID_REQUEST, thrift.populateJobConfig(job.newBuilder()));
  }

  @Test
  public void testGetLocks() throws Exception {
    expect(lockManager.getLocks()).andReturn(ImmutableSet.of(LOCK));

    control.replay();

    Response response = thrift.getLocks();
    assertEquals(
        LOCK.newBuilder(),
        Iterables.getOnlyElement(response.getResult().getGetLocksResult().getLocks()));
  }

  @Test
  public void testGetQuota() throws Exception {
    QuotaInfo infoMock = createMock(QuotaInfo.class);
    expect(quotaManager.getQuotaInfo(ROLE, storageUtil.storeProvider)).andReturn(infoMock);
    expect(infoMock.getQuota()).andReturn(QUOTA);
    expect(infoMock.getProdSharedConsumption()).andReturn(XLARGE);
    expect(infoMock.getProdDedicatedConsumption()).andReturn(LARGE);
    expect(infoMock.getNonProdSharedConsumption()).andReturn(MEDIUM);
    expect(infoMock.getNonProdDedicatedConsumption()).andReturn(SMALL);
    control.replay();

    GetQuotaResult expected = new GetQuotaResult()
        .setQuota(QUOTA.newBuilder())
        .setProdSharedConsumption(XLARGE.newBuilder())
        .setProdDedicatedConsumption(LARGE.newBuilder())
        .setNonProdSharedConsumption(MEDIUM.newBuilder())
        .setNonProdDedicatedConsumption(SMALL.newBuilder());

    Response response = assertOkResponse(thrift.getQuota(ROLE));
    assertEquals(expected, response.getResult().getGetQuotaResult());
  }

  @Test
  public void testGetTasksWithoutConfigs() throws Exception {
    Builder query = Query.unscoped();
    storageUtil.expectTaskFetch(query, ImmutableSet.copyOf(makeDefaultScheduledTasks(10)));

    control.replay();

    ImmutableList<ScheduledTask> expected = ScheduledTask.toBuildersList(makeDefaultScheduledTasks(
        10,
        defaultTask(true).setExecutorConfig(null)));

    Response response = assertOkResponse(thrift.getTasksWithoutConfigs(new TaskQuery()));
    assertEquals(expected, response.getResult().getScheduleStatusResult().getTasks());
  }

  @Test
  public void testGetPendingReasonFailsSlavesSet() throws Exception {
    Builder query = Query.unscoped().bySlave("host1");

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.getPendingReason(query.get()));
  }

  @Test
  public void testGetAllJobs() throws Exception {
    JobConfiguration cronJobOne = makeJob()
        .setCronSchedule("1 * * * *")
        .setKey(JOB_KEY.newBuilder())
        .setTaskConfig(nonProductionTask());
    JobKey jobKey2 = JOB_KEY.newBuilder().setRole("other_role");
    JobConfiguration cronJobTwo = makeJob()
        .setCronSchedule("2 * * * *")
        .setKey(jobKey2)
        .setTaskConfig(nonProductionTask());
    TaskConfig immediateTaskConfig = defaultTask(false)
        .setJob(JOB_KEY.newBuilder().setName("immediate"))
        .setJobName("immediate")
        .setOwner(ROLE_IDENTITY);
    ScheduledTask immediateTask = ScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask().setTask(immediateTaskConfig))
        .setStatus(ScheduleStatus.ASSIGNED));
    JobConfiguration immediateJob = new JobConfiguration()
        .setKey(JOB_KEY.newBuilder().setName("immediate"))
        .setOwner(ROLE_IDENTITY)
        .setInstanceCount(1)
        .setTaskConfig(immediateTaskConfig);

    Set<JobConfiguration> crons = ImmutableSet.of(cronJobOne, cronJobTwo);
    expect(storageUtil.jobStore.fetchJobs()).andReturn(JobConfiguration.setFromBuilders(crons));
    storageUtil.expectTaskFetch(Query.unscoped().active(), immediateTask);

    control.replay();

    Set<JobConfiguration> allJobs =
        ImmutableSet.<JobConfiguration>builder().addAll(crons).add(immediateJob).build();
    assertEquals(
        JobConfiguration.setFromBuilders(allJobs),
        JobConfiguration.setFromBuilders(
            thrift.getJobs(null).getResult().getGetJobsResult().getConfigs()));
  }

  @Test
  public void testGetJobs() throws Exception {
    TaskConfig ownedCronJobTask = nonProductionTask()
        .setJobName(JOB_KEY.getName())
        .setOwner(ROLE_IDENTITY)
        .setEnvironment(JOB_KEY.getEnvironment());
    JobConfiguration ownedCronJob = makeJob()
        .setCronSchedule(CRON_SCHEDULE)
        .setTaskConfig(ownedCronJobTask);
    ScheduledTask ownedCronJobScheduledTask = ScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask().setTask(ownedCronJobTask))
        .setStatus(ScheduleStatus.ASSIGNED));
    Identity otherOwner = new Identity("other", "other");
    JobConfiguration unownedCronJob = makeJob()
        .setOwner(otherOwner)
        .setCronSchedule(CRON_SCHEDULE)
        .setKey(JOB_KEY.newBuilder().setRole("other"))
        .setTaskConfig(ownedCronJobTask.deepCopy().setOwner(otherOwner));
    TaskConfig ownedImmediateTaskInfo = defaultTask(false)
        .setJob(JOB_KEY.newBuilder().setName("immediate"))
        .setJobName("immediate")
        .setOwner(ROLE_IDENTITY);
    Set<JobConfiguration> ownedCronJobOnly = ImmutableSet.of(ownedCronJob);
    Set<JobConfiguration> unownedCronJobOnly = ImmutableSet.of(unownedCronJob);
    Set<JobConfiguration> bothCronJobs = ImmutableSet.of(ownedCronJob, unownedCronJob);
    ScheduledTask ownedImmediateTask = ScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask().setTask(ownedImmediateTaskInfo))
        .setStatus(ScheduleStatus.ASSIGNED));
    JobConfiguration ownedImmediateJob = new JobConfiguration()
        .setKey(JOB_KEY.newBuilder().setName("immediate"))
        .setOwner(ROLE_IDENTITY)
        .setInstanceCount(1)
        .setTaskConfig(ownedImmediateTaskInfo);
    Query.Builder query = Query.roleScoped(ROLE).active();

    storageUtil.expectTaskFetch(query);
    expect(storageUtil.jobStore.fetchJobs())
        .andReturn(JobConfiguration.setFromBuilders(ownedCronJobOnly));

    storageUtil.expectTaskFetch(query);
    expect(storageUtil.jobStore.fetchJobs())
        .andReturn(JobConfiguration.setFromBuilders(bothCronJobs));

    storageUtil.expectTaskFetch(query, ownedImmediateTask);
    expect(storageUtil.jobStore.fetchJobs())
        .andReturn(JobConfiguration.setFromBuilders(unownedCronJobOnly));

    expect(storageUtil.jobStore.fetchJobs()).andReturn(ImmutableSet.of());
    storageUtil.expectTaskFetch(query);

    // Handle the case where a cron job has a running task (same JobKey present in both stores).
    storageUtil.expectTaskFetch(query, ownedCronJobScheduledTask);
    expect(storageUtil.jobStore.fetchJobs())
        .andReturn(JobConfiguration.setFromBuilders(ImmutableSet.of(ownedCronJob)));

    control.replay();

    assertJobsEqual(ownedCronJob, Iterables.getOnlyElement(thrift.getJobs(ROLE)
        .getResult().getGetJobsResult().getConfigs()));

    assertJobsEqual(ownedCronJob, Iterables.getOnlyElement(thrift.getJobs(ROLE)
        .getResult().getGetJobsResult().getConfigs()));

    Set<JobConfiguration> queryResult3 =
        thrift.getJobs(ROLE).getResult().getGetJobsResult().getConfigs();
    assertJobsEqual(ownedImmediateJob, Iterables.getOnlyElement(queryResult3));
    assertEquals(
        TaskConfig.build(ownedImmediateTaskInfo),
        TaskConfig.build(Iterables.getOnlyElement(queryResult3).getTaskConfig()));

    assertTrue(thrift.getJobs(ROLE)
        .getResult().getGetJobsResult().getConfigs().isEmpty());

    assertJobsEqual(ownedCronJob, Iterables.getOnlyElement(thrift.getJobs(ROLE)
        .getResult().getGetJobsResult().getConfigs()));
  }

  private static void assertJobsEqual(JobConfiguration expected, JobConfiguration actual) {
    assertEquals(JobConfiguration.build(expected), JobConfiguration.build(actual));
  }

  @Test
  public void testGetTasksStatusPagination() throws Exception {
    Iterable<ScheduledTask> tasks = makeDefaultScheduledTasks(10);

    TaskQuery page1Query = setupPaginatedQuery(tasks, 0, 4);
    TaskQuery page2Query = setupPaginatedQuery(tasks, 4, 4);
    TaskQuery page3Query = setupPaginatedQuery(tasks, 8, 4);

    control.replay();

    Response page1Response = assertOkResponse(thrift.getTasksStatus(page1Query));
    Response page2Response = assertOkResponse(thrift.getTasksStatus(page2Query));
    Response page3Response = assertOkResponse(thrift.getTasksStatus(page3Query));

    Iterable<Integer> page1Ids = Lists.newArrayList(Iterables.transform(
        page1Response.getResult().getScheduleStatusResult().getTasks(), TO_INSTANCE_ID));
    Iterable<Integer> page2Ids = Lists.newArrayList(Iterables.transform(
        page2Response.getResult().getScheduleStatusResult().getTasks(), TO_INSTANCE_ID));
    Iterable<Integer> page3Ids = Lists.newArrayList(Iterables.transform(
        page3Response.getResult().getScheduleStatusResult().getTasks(), TO_INSTANCE_ID));

    assertEquals(Lists.newArrayList(0, 1, 2, 3), page1Ids);
    assertEquals(Lists.newArrayList(4, 5, 6, 7), page2Ids);
    assertEquals(Lists.newArrayList(8, 9), page3Ids);
  }

  private TaskQuery setupPaginatedQuery(Iterable<ScheduledTask> tasks, int offset, int limit) {
    TaskQuery query = new TaskQuery().setOffset(offset).setLimit(limit);
    Builder builder = Query.arbitrary(query);
    storageUtil.expectTaskFetch(builder, ImmutableSet.copyOf(tasks));
    return query;
  }

  private static final Function<ScheduledTask, Integer> TO_INSTANCE_ID =
      new Function<ScheduledTask, Integer>() {
        @Nullable
        @Override
        public Integer apply(@Nullable ScheduledTask input) {
          return input.getAssignedTask().getInstanceId();
        }
      };

  @Test
  public void testGetConfigSummary() throws Exception {
    JobKey key = JobKeys.from("test", "test", "test");

    TaskConfig firstGroupTask = defaultTask(true);
    TaskConfig secondGroupTask = defaultTask(true).setNumCpus(2);

    ScheduledTask first1 = ScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask().setTask(firstGroupTask).setInstanceId(0)));

    ScheduledTask first2 = ScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask().setTask(firstGroupTask).setInstanceId(1)));

    ScheduledTask second = ScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask().setTask(secondGroupTask).setInstanceId(2)));

    storageUtil.expectTaskFetch(Query.jobScoped(key).active(), first1, first2, second);

    ConfigGroup group1 = new ConfigGroup()
        .setConfig(firstGroupTask)
        .setInstances(Range.toBuildersSet(convertRanges(toRanges(ImmutableSet.of(0, 1)))));
    ConfigGroup group2 = new ConfigGroup()
        .setConfig(secondGroupTask)
        .setInstances(Range.toBuildersSet(convertRanges(toRanges(ImmutableSet.of(2)))));

    ConfigSummary summary = new ConfigSummary()
        .setKey(key.newBuilder())
        .setGroups(Sets.newHashSet(group1, group2));

    ConfigSummaryResult expected = new ConfigSummaryResult().setSummary(summary);

    control.replay();

    Response response = assertOkResponse(thrift.getConfigSummary(key.newBuilder()));
    assertEquals(
        ConfigSummaryResult.build(expected),
        ConfigSummaryResult.build(response.getResult().getConfigSummaryResult()));
  }

  @Test
  public void testGetTasksStatus() throws Exception {
    Builder query = Query.unscoped();
    Iterable<ScheduledTask> tasks = makeDefaultScheduledTasks(10);
    storageUtil.expectTaskFetch(query, ImmutableSet.copyOf(tasks));

    control.replay();

    ImmutableList<ScheduledTask> expected = ScheduledTask.toBuildersList(tasks);
    Response response = assertOkResponse(thrift.getTasksStatus(new TaskQuery()));
    assertEquals(expected, response.getResult().getScheduleStatusResult().getTasks());
  }

  @Test
  public void testGetPendingReasonFailsStatusSet() throws Exception {
    Builder query = Query.unscoped().byStatus(ScheduleStatus.ASSIGNED);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.getPendingReason(query.get()));
  }

  @Test
  public void testGetJobUpdateSummaries() throws Exception {
    JobUpdateQuery query = new JobUpdateQuery().setRole(ROLE);
    List<JobUpdateSummary> summaries = createJobUpdateSummaries(5);
    expect(storageUtil.jobUpdateStore.fetchJobUpdateSummaries(JobUpdateQuery.build(query)))
        .andReturn(JobUpdateSummary.listFromBuilders(summaries));

    control.replay();

    Response response = assertOkResponse(thrift.getJobUpdateSummaries(query));
    assertEquals(
        summaries,
        response.getResult().getGetJobUpdateSummariesResult().getUpdateSummaries());
  }

  @Test
  public void testGetJobUpdateDetails() throws Exception {
    JobUpdateDetails details = createJobUpdateDetails();
    expect(storageUtil.jobUpdateStore.fetchJobUpdateDetails(UPDATE_KEY))
        .andReturn(Optional.of(JobUpdateDetails.build(details)));

    control.replay();

    Response response = assertOkResponse(thrift.getJobUpdateDetails(UPDATE_KEY.newBuilder()));
    assertEquals(
        JobUpdateDetails.build(details),
        JobUpdateDetails.build(response.getResult().getGetJobUpdateDetailsResult().getDetails()));
  }

  private static List<JobUpdateSummary> createJobUpdateSummaries(int count) {
    ImmutableList.Builder<JobUpdateSummary> builder = ImmutableList.builder();
    for (int i = 0; i < count; i++) {
      builder.add(new JobUpdateSummary()
          .setKey(new JobUpdateKey(JOB_KEY.newBuilder(), "id" + 1))
          .setUser(USER));
    }
    return builder.build();
  }

  private static JobUpdateDetails createJobUpdateDetails() {
    return new JobUpdateDetails()
        .setUpdate(new JobUpdate().setSummary(createJobUpdateSummaries(1).get(0)));
  }

  @Test
  public void testGetRoleSummary() throws Exception {
    final String BAZ_ROLE = "baz_role";
    final Identity BAZ_ROLE_IDENTITY = new Identity(BAZ_ROLE, USER);

    JobConfiguration cronJobOne = makeJob()
        .setCronSchedule("1 * * * *")
        .setKey(JOB_KEY.newBuilder())
        .setTaskConfig(nonProductionTask());
    JobConfiguration cronJobTwo = makeJob()
        .setCronSchedule("2 * * * *")
        .setKey(JOB_KEY.newBuilder().setName("cronJob2"))
        .setTaskConfig(nonProductionTask());

    JobConfiguration cronJobThree = makeJob()
        .setCronSchedule("3 * * * *")
        .setKey(JOB_KEY.newBuilder().setRole(BAZ_ROLE))
        .setTaskConfig(nonProductionTask())
        .setOwner(BAZ_ROLE_IDENTITY);

    Set<JobConfiguration> crons = ImmutableSet.of(cronJobOne, cronJobTwo, cronJobThree);

    TaskConfig immediateTaskConfig = defaultTask(false)
        .setJob(JOB_KEY.newBuilder().setName("immediate"))
        .setJobName("immediate")
        .setOwner(ROLE_IDENTITY);
    ScheduledTask task1 = ScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask().setTask(immediateTaskConfig)));
    ScheduledTask task2 = ScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask().setTask(immediateTaskConfig.setNumCpus(2))));

    TaskConfig immediateTaskConfigTwo = defaultTask(false)
        .setJob(JOB_KEY.newBuilder().setRole(BAZ_ROLE_IDENTITY.getRole()).setName("immediateTwo"))
        .setJobName("immediateTwo")
        .setOwner(BAZ_ROLE_IDENTITY);
    ScheduledTask task3 = ScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask().setTask(immediateTaskConfigTwo)));

    TaskConfig immediateTaskConfigThree = defaultTask(false)
        .setJob(JOB_KEY.newBuilder().setRole(BAZ_ROLE_IDENTITY.getRole()).setName("immediateThree"))
        .setJobName("immediateThree")
        .setOwner(BAZ_ROLE_IDENTITY);
    ScheduledTask task4 = ScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask().setTask(immediateTaskConfigThree)));

    expect(storageUtil.taskStore.getJobKeys()).andReturn(
        FluentIterable.from(ImmutableSet.of(task1, task2, task3, task4))
            .transform(Tasks::getJob)
            .toSet());
    expect(storageUtil.jobStore.fetchJobs()).andReturn(JobConfiguration.setFromBuilders(crons));

    RoleSummaryResult expectedResult = new RoleSummaryResult();
    expectedResult.addToSummaries(
        new RoleSummary().setRole(ROLE).setCronJobCount(2).setJobCount(1));
    expectedResult.addToSummaries(
        new RoleSummary().setRole(BAZ_ROLE).setCronJobCount(1).setJobCount(2));

    control.replay();

    Response response = assertOkResponse(thrift.getRoleSummary());
    assertEquals(expectedResult, response.getResult().getRoleSummaryResult());
  }

  @Test
  public void testEmptyConfigSummary() throws Exception {
    JobKey key = JobKeys.from("test", "test", "test");

    storageUtil.expectTaskFetch(Query.jobScoped(key).active(), ImmutableSet.of());

    ConfigSummary summary = new ConfigSummary()
        .setKey(key.newBuilder())
        .setGroups(Sets.newHashSet());

    ConfigSummaryResult expected = new ConfigSummaryResult().setSummary(summary);

    control.replay();

    Response response = assertOkResponse(thrift.getConfigSummary(key.newBuilder()));
    assertEquals(expected, response.getResult().getConfigSummaryResult());
  }

  @Test
  public void testGetJobUpdateDetailsInvalidId() throws Exception {
    expect(storageUtil.jobUpdateStore.fetchJobUpdateDetails(UPDATE_KEY))
        .andReturn(Optional.absent());

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.getJobUpdateDetails(UPDATE_KEY.newBuilder()));
  }

  @Test
  public void testGetJobUpdateDiffWithUpdateAdd() throws Exception {
    TaskConfig task1 = defaultTask(false).setNumCpus(1.0);
    TaskConfig task2 = defaultTask(false).setNumCpus(2.0);
    TaskConfig task3 = defaultTask(false).setNumCpus(3.0);
    TaskConfig task4 = defaultTask(false).setNumCpus(4.0);
    TaskConfig task5 = defaultTask(false).setNumCpus(5.0);

    ImmutableSet.Builder<ScheduledTask> tasks = ImmutableSet.builder();
    makeTasks(0, 10, task1, tasks);
    makeTasks(10, 20, task2, tasks);
    makeTasks(20, 30, task3, tasks);
    makeTasks(30, 40, task4, tasks);
    makeTasks(40, 50, task5, tasks);

    expect(storageUtil.jobStore.fetchJob(JOB_KEY)).andReturn(Optional.absent());
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active(), tasks.build());

    control.replay();

    TaskConfig newTask = defaultTask(false).setNumCpus(6.0);
    JobUpdateRequest request = new JobUpdateRequest()
        .setTaskConfig(newTask)
        .setInstanceCount(60)
        .setSettings(new JobUpdateSettings()
            .setUpdateOnlyTheseInstances(ImmutableSet.of(new Range(10, 59))));

    GetJobUpdateDiffResult expected = new GetJobUpdateDiffResult()
        .setAdd(ImmutableSet.of(group(newTask, new Range(50, 59))))
        .setUpdate(ImmutableSet.of(
            group(task2, new Range(10, 19)),
            group(task3, new Range(20, 29)),
            group(task4, new Range(30, 39)),
            group(task5, new Range(40, 49))))
        .setUnchanged(ImmutableSet.of(group(task1, new Range(0, 9))))
        .setRemove(ImmutableSet.of());

    Response response = assertOkResponse(thrift.getJobUpdateDiff(request));
    assertEquals(expected, response.getResult().getGetJobUpdateDiffResult());
  }

  @Test
  public void testGetJobUpdateDiffWithUpdateRemove() throws Exception {
    TaskConfig task1 = defaultTask(false).setNumCpus(1.0);
    TaskConfig task2 = defaultTask(false).setNumCpus(2.0);
    TaskConfig task3 = defaultTask(false).setNumCpus(3.0);

    ImmutableSet.Builder<ScheduledTask> tasks = ImmutableSet.builder();
    makeTasks(0, 10, task1, tasks);
    makeTasks(10, 20, task2, tasks);
    makeTasks(20, 30, task3, tasks);

    expect(storageUtil.jobStore.fetchJob(JOB_KEY)).andReturn(Optional.absent());
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active(), tasks.build());

    control.replay();

    JobUpdateRequest request = new JobUpdateRequest()
        .setTaskConfig(defaultTask(false).setNumCpus(6.0))
        .setInstanceCount(20)
        .setSettings(new JobUpdateSettings());

    GetJobUpdateDiffResult expected = new GetJobUpdateDiffResult()
        .setRemove(ImmutableSet.of(group(task3, new Range(20, 29))))
        .setUpdate(ImmutableSet.of(
            group(task1, new Range(0, 9)),
            group(task2, new Range(10, 19))))
        .setAdd(ImmutableSet.of())
        .setUnchanged(ImmutableSet.of());

    Response response = assertOkResponse(thrift.getJobUpdateDiff(request));
    assertEquals(expected, response.getResult().getGetJobUpdateDiffResult());
  }

  @Test
  public void testGetJobUpdateDiffWithUnchanged() throws Exception {
    expect(storageUtil.jobStore.fetchJob(JOB_KEY)).andReturn(Optional.absent());
    storageUtil.expectTaskFetch(
        Query.jobScoped(JOB_KEY).active(),
        ImmutableSet.copyOf(makeDefaultScheduledTasks(10)));

    control.replay();

    JobUpdateRequest request = new JobUpdateRequest()
        .setTaskConfig(defaultTask(true))
        .setInstanceCount(10)
        .setSettings(new JobUpdateSettings());

    GetJobUpdateDiffResult expected = new GetJobUpdateDiffResult()
        .setUnchanged(ImmutableSet.of(group(defaultTask(true), new Range(0, 9))))
        .setRemove(ImmutableSet.of())
        .setUpdate(ImmutableSet.of())
        .setAdd(ImmutableSet.of());

    Response response = assertOkResponse(thrift.getJobUpdateDiff(request));
    assertEquals(expected, response.getResult().getGetJobUpdateDiffResult());
  }

  @Test
  public void testGetJobUpdateDiffNoCron() throws Exception {
    expect(storageUtil.jobStore.fetchJob(JOB_KEY))
        .andReturn(Optional.of(JobConfiguration.build(CRON_JOB)));

    control.replay();

    JobUpdateRequest request = new JobUpdateRequest().setTaskConfig(defaultTask(false));

    Response expected = Responses.empty()
        .setResponseCode(INVALID_REQUEST)
        .setDetails(ImmutableList.of(new ResponseDetail(NO_CRON)));

    assertEquals(expected, thrift.getJobUpdateDiff(request));
  }

  @Test
  public void testGetJobUpdateDiffInvalidConfig() throws Exception {
    control.replay();

    JobUpdateRequest request =
        new JobUpdateRequest().setTaskConfig(defaultTask(false).setNumCpus(-1));
    assertResponse(INVALID_REQUEST, thrift.getJobUpdateDiff(request));
  }

  private static void makeTasks(
      int start,
      int end,
      TaskConfig config,
      ImmutableSet.Builder<ScheduledTask> builder) {

    for (int i = start; i < end; i++) {
      builder.add(ScheduledTask.build(new ScheduledTask()
          .setAssignedTask(new AssignedTask().setTask(config).setInstanceId(i))));
    }
  }

  private static ConfigGroup group(TaskConfig task, Range range) {
    return new ConfigGroup()
        .setConfig(task)
        .setInstances(ImmutableSet.of(range));
  }
}
