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
package org.apache.aurora.scheduler.quota;

import java.util.List;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.gen.ValueConstraint;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.quota.QuotaManager.QuotaException;
import org.apache.aurora.scheduler.quota.QuotaManager.QuotaManagerImpl;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.ResourceAggregates.EMPTY;
import static org.apache.aurora.scheduler.quota.QuotaCheckResult.Result.INSUFFICIENT_QUOTA;
import static org.apache.aurora.scheduler.quota.QuotaCheckResult.Result.SUFFICIENT_QUOTA;
import static org.apache.aurora.scheduler.quota.QuotaManager.QuotaManagerImpl.updateQuery;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QuotaManagerImplTest extends EasyMockTest {
  private static final String ROLE = "test";
  private static final String ENV = "test_env";
  private static final String JOB_NAME = "job";
  private static final JobUpdateKey UPDATE_KEY =
      JobUpdateKey.create(JobKeys.from(ROLE, ENV, JOB_NAME), "u1");
  private static final ResourceAggregate QUOTA = ResourceAggregate.builder()
      .setNumCpus(1.0)
      .setRamMb(100L)
      .setDiskMb(200L)
      .build();
  private static final Query.Builder ACTIVE_QUERY = Query.roleScoped(ROLE).active();

  private StorageTestUtil storageUtil;
  private JobUpdateStore jobUpdateStore;
  private QuotaManagerImpl quotaManager;
  private StoreProvider storeProvider;

  @Before
  public void setUp() throws Exception {
    storageUtil = new StorageTestUtil(this);
    storeProvider = storageUtil.storeProvider;
    jobUpdateStore = storageUtil.jobUpdateStore;
    quotaManager = new QuotaManagerImpl();
    storageUtil.expectOperations();
  }

  @Test
  public void testGetQuotaInfo() {
    ScheduledTask prodSharedTask = prodTask("foo1", 3, 3, 3);
    ScheduledTask prodDedicatedTask = prodDedicatedTask("foo2", 5, 5, 5);
    ScheduledTask nonProdSharedTask = nonProdTask("bar1", 2, 2, 2);
    ScheduledTask nonProdDedicatedTask = nonProdDedicatedTask("bar2", 7, 7, 7);
    ResourceAggregate quota = ResourceAggregate.create(4, 4, 4);

    expectQuota(quota);
    expectTasks(prodSharedTask, nonProdSharedTask, prodDedicatedTask, nonProdDedicatedTask);
    expectJobUpdates(taskConfig(1, 1, 1, true), taskConfig(1, 1, 1, true));
    expectCronJobs(
        createJob(prodTask("pc", 1, 1, 1), 2),
        createJob(nonProdTask("npc", 7, 7, 7), 1));

    control.replay();

    assertEquals(
        new QuotaInfo(from(4, 4, 4), from(6, 6, 6), from(5, 5, 5), from(9, 9, 9), from(7, 7, 7)),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testGetQuotaInfoWithCronTasks() {
    ScheduledTask prodTask = prodTask("pc", 6, 6, 6);
    ScheduledTask nonProdTask = prodTask("npc", 7, 7, 7);
    ResourceAggregate quota = ResourceAggregate.create(4, 4, 4);

    expectQuota(quota);
    expectTasks(prodTask, nonProdTask);
    expectJobUpdates(taskConfig(1, 1, 1, true), taskConfig(1, 1, 1, true));

    String pcRole = "pc-role";
    ScheduledTask ignoredProdTask = prodTask(pcRole, 20, 20, 20)
        .withAssignedTask(at -> at.withTask(t -> t.withJob(JobKey.create(pcRole, ENV, pcRole))));

    String npcRole = "npc-role";
    ScheduledTask ignoredNonProdTask = nonProdTask(npcRole, 20, 20, 20)
        .withAssignedTask(at -> at.withTask(t -> t.withJob(JobKey.create(npcRole, ENV, npcRole))));

    expectCronJobs(
        createJob(prodTask("pc", 3, 3, 3), 1),
        createJob(nonProdTask("npc", 5, 5, 5), 2),
        createJob(ignoredProdTask, 2),
        createJob(ignoredNonProdTask, 3));

    control.replay();

    assertEquals(
        new QuotaInfo(from(4, 4, 4), from(7, 7, 7), EMPTY, from(10, 10, 10), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testGetQuotaInfoPartialUpdate() {
    ScheduledTask prodTask = prodTask("foo", 3, 3, 3);
    ScheduledTask updatingProdTask = createTask(JOB_NAME, "id1", 3, 3, 3, true, 1);
    ScheduledTask updatingFilteredProdTask = createTask(JOB_NAME, "id0", 3, 3, 3, true, 0);
    ScheduledTask nonProdTask = createTask("bar", "id1", 2, 2, 2, false, 0);
    ResourceAggregate quota = ResourceAggregate.create(4, 4, 4);

    expectQuota(quota);
    expectTasks(prodTask, updatingProdTask, updatingFilteredProdTask, nonProdTask);
    expectJobUpdates(taskConfig(1, 1, 1, true), taskConfig(1, 1, 1, true));
    expectNoCronJobs();

    control.replay();

    // Expected consumption from: prodTask + updatingProdTask + job update.
    assertEquals(
        new QuotaInfo(from(4, 4, 4), from(7, 7, 7), EMPTY, from(2, 2, 2), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testGetQuotaInfoNoTasksNoUpdatesNoCronJobs() {
    ResourceAggregate quota = ResourceAggregate.create(4, 4, 4);

    expectQuota(quota);
    expectNoTasks();
    expectNoJobUpdates();
    expectNoCronJobs();

    control.replay();

    assertEquals(
        new QuotaInfo(from(4, 4, 4), from(0, 0, 0), EMPTY, from(0, 0, 0), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaPasses() {
    expectQuota(ResourceAggregate.create(4, 4, 4));
    expectTasks(prodTask("foo", 2, 2, 2));
    expectJobUpdates(taskConfig(1, 1, 1, true), taskConfig(1, 1, 1, true));
    expectNoCronJobs();

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaPassesNoTasks() {
    expectQuota(ResourceAggregate.create(4, 4, 4));
    expectNoTasks();
    expectJobUpdates(taskConfig(1, 1, 1, true), taskConfig(1, 1, 1, true));
    expectNoCronJobs();

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaPassesNoUpdates() {
    expectQuota(ResourceAggregate.create(4, 4, 4));
    expectTasks(prodTask("foo", 2, 2, 2));
    expectNoJobUpdates();
    expectNoCronJobs();

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaPassesNoTasksNoUpdates() {
    expectQuota(ResourceAggregate.create(4, 4, 4));
    expectNoTasks();
    expectNoJobUpdates();
    expectNoCronJobs();

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaPassesNonProdUnaccounted() {
    expectQuota(ResourceAggregate.create(4, 4, 4));
    expectTasks(prodTask("foo", 2, 2, 2), createTask("bar", "id2", 5, 5, 5, false, 0));

    expectNoJobUpdates();
    expectNoCronJobs();

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaPassesDedicatedUnaccounted() {
    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkInstanceAddition(
        prodDedicatedTask("dedicatedJob", 1, 1, 1).getAssignedTask().getTask(),
        1,
        storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaSkippedForNonProdRequest() {
    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, false), 1, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaNoQuotaSet() {
    expect(storageUtil.quotaStore.fetchQuota(ROLE))
        .andReturn(Optional.absent());

    expectNoTasks();
    expectNoJobUpdates();
    expectNoCronJobs();

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaExceedsCpu() {
    expectQuota(ResourceAggregate.create(4, 4, 4));
    expectTasks(prodTask("foo", 3, 3, 3));
    expectNoJobUpdates();
    expectNoCronJobs();

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(2, 1, 1, true), 1, storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertTrue(checkQuota.getDetails().get().contains("CPU"));
  }

  @Test
  public void testCheckQuotaExceedsRam() {
    expectQuota(ResourceAggregate.create(4, 4, 4));
    expectTasks(prodTask("foo", 3, 3, 3));
    expectNoJobUpdates();
    expectNoCronJobs();

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 2, 1, true), 1, storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertTrue(checkQuota.getDetails().get().contains("RAM"));
  }

  @Test
  public void testCheckQuotaExceedsDisk() {
    expectQuota(ResourceAggregate.create(4, 4, 4));
    expectTasks(prodTask("foo", 3, 3, 3));
    expectNoJobUpdates();
    expectNoCronJobs();

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 2, true), 1, storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertTrue(checkQuota.getDetails().get().contains("DISK"));
  }

  @Test
  public void testCheckQuotaExceedsCron() {
    expectQuota(ResourceAggregate.create(5, 5, 5)).times(2);
    expectNoTasks().times(2);
    expectNoJobUpdates().times(2);
    expectCronJobs(
        createJob(prodTask("pc", 4, 4, 4), 1),
        createJob(nonProdTask("npc", 7, 7, 7), 1)).times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(2, 2, 2, true), 1, storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(5, 5, 5), from(4, 4, 4), EMPTY, from(7, 7, 7), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaUpdatingTasksFilteredOut() {
    expectQuota(ResourceAggregate.create(5, 5, 5)).times(2);
    expectTasks(prodTask("foo", 2, 2, 2), createTask(JOB_NAME, "id2", 3, 3, 3, true, 0))
        .times(2);

    expectJobUpdates(taskConfig(1, 1, 1, true), taskConfig(2, 2, 2, true), 2);
    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(5, 5, 5), from(4, 4, 4), EMPTY, from(0, 0, 0), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaNonProdUpdatesUnaccounted() {
    expectQuota(ResourceAggregate.create(5, 5, 5)).times(2);
    expectTasks(prodTask("foo", 2, 2, 2), prodTask("bar", 2, 2, 2)).times(2);

    expectJobUpdates(taskConfig(8, 8, 8, false), taskConfig(4, 4, 4, false), 2);
    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(5, 5, 5), from(4, 4, 4), EMPTY, from(8, 8, 8), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaProdToNonUpdateUnaccounted() {
    expectQuota(ResourceAggregate.create(5, 5, 5)).times(2);
    expectTasks(prodTask("foo", 2, 2, 2), prodTask("bar", 1, 1, 1)).times(2);

    expectJobUpdates(taskConfig(1, 1, 1, true), taskConfig(7, 7, 7, false), 2);
    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(5, 5, 5), from(4, 4, 4), EMPTY, from(7, 7, 7), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaNonToProdUpdateExceedsQuota() {
    expectQuota(ResourceAggregate.create(5, 5, 5)).times(2);
    expectTasks(prodTask("foo", 2, 2, 2), prodTask("bar", 2, 2, 2)).times(2);

    expectJobUpdates(taskConfig(1, 1, 1, false), taskConfig(1, 1, 1, true), 2);
    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(5, 5, 5), from(5, 5, 5), EMPTY, from(1, 1, 1), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaOldJobUpdateConfigMatters() {
    expectQuota(ResourceAggregate.create(6, 6, 6)).times(2);
    expectTasks(prodTask("foo", 2, 2, 2), prodTask("bar", 2, 2, 2)).times(2);
    expectJobUpdates(taskConfig(2, 2, 2, true), taskConfig(1, 1, 1, true), 2);
    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(6, 6, 6), from(6, 6, 6), EMPTY, from(0, 0, 0), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaUpdateAddsInstances() {
    expectQuota(ResourceAggregate.create(6, 6, 6)).times(2);
    expectTasks(prodTask("foo", 2, 2, 2), prodTask("bar", 2, 2, 2)).times(2);
    expectJobUpdates(taskConfig(1, 1, 1, true), 1, taskConfig(1, 1, 1, true), 2, 2);
    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(6, 6, 6), from(6, 6, 6), EMPTY, from(0, 0, 0), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaUpdateRemovesInstances() {
    expectQuota(ResourceAggregate.create(6, 6, 6)).times(2);
    expectTasks(prodTask("foo", 2, 2, 2), prodTask("bar", 2, 2, 2)).times(2);
    expectJobUpdates(taskConfig(1, 1, 1, true), 2, taskConfig(1, 1, 1, true), 1, 2);
    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(6, 6, 6), from(6, 6, 6), EMPTY, from(0, 0, 0), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaUpdateInitialConfigsUsedForFiltering() {
    expectQuota(ResourceAggregate.create(6, 6, 6)).times(2);
    expectTasks(prodTask("foo", 2, 2, 2), prodTask(JOB_NAME, 2, 2, 2)).times(2);

    TaskConfig config = taskConfig(2, 2, 2, true);
    List<JobUpdateSummary> summaries = buildJobUpdateSummaries(UPDATE_KEY);
    JobUpdate update = buildJobUpdate(summaries.get(0), config, 1, config, 1)
        .withInstructions(inst -> inst.withDesiredState((InstanceTaskConfig) null));

    expect(jobUpdateStore.fetchJobUpdateSummaries(updateQuery(config.getJob().getRole())))
        .andReturn(summaries).times(2);

    expect(jobUpdateStore.fetchJobUpdate(UPDATE_KEY)).andReturn(Optional.of(update)).times(2);

    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(6, 6, 6), from(4, 4, 4), EMPTY, from(0, 0, 0), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaUpdateDesiredConfigsUsedForFiltering() {
    expectQuota(ResourceAggregate.create(6, 6, 6)).times(2);
    expectTasks(prodTask("foo", 2, 2, 2), prodTask(JOB_NAME, 2, 2, 2)).times(2);

    TaskConfig config = taskConfig(2, 2, 2, true);
    List<JobUpdateSummary> summaries = buildJobUpdateSummaries(UPDATE_KEY);
    JobUpdate update = buildJobUpdate(summaries.get(0), config, 1, config, 1);
    JobUpdate builder = update.withInstructions(inst -> inst.withInitialState(ImmutableSet.of()));

    expect(jobUpdateStore.fetchJobUpdateSummaries(updateQuery(config.getJob().getRole())))
        .andReturn(summaries).times(2);

    expect(jobUpdateStore.fetchJobUpdate(UPDATE_KEY)).andReturn(Optional.of(builder)).times(2);

    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(6, 6, 6), from(4, 4, 4), EMPTY, from(0, 0, 0), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaNoDesiredState() {
    expectQuota(ResourceAggregate.create(6, 6, 6)).times(2);
    expectTasks(prodTask("foo", 2, 2, 2), prodTask("bar", 2, 2, 2)).times(2);

    TaskConfig config = taskConfig(2, 2, 2, true);
    List<JobUpdateSummary> summaries = buildJobUpdateSummaries(UPDATE_KEY);
    JobUpdate update = buildJobUpdate(summaries.get(0), config, 1, config, 1);
    JobUpdate builder = update.withInstructions(
        inst -> inst.withDesiredState((InstanceTaskConfig) null));

    expect(jobUpdateStore.fetchJobUpdateSummaries(updateQuery(config.getJob().getRole())))
        .andReturn(summaries).times(2);

    expect(jobUpdateStore.fetchJobUpdate(UPDATE_KEY)).andReturn(Optional.of(builder)).times(2);

    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(6, 6, 6), from(6, 6, 6), EMPTY, from(0, 0, 0), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaNewInPlaceUpdate() {
    expectQuota(ResourceAggregate.create(6, 6, 6)).times(2);
    expectTasks(
        prodTask("foo", 2, 2, 2),
        createTask(JOB_NAME, "id1", 2, 2, 2, true, 0),
        createTask(JOB_NAME, "id12", 2, 2, 2, true, 12)).times(2);
    expectNoJobUpdates().times(2);

    TaskConfig config = taskConfig(1, 1, 1, true);
    JobUpdate update = buildJobUpdate(
        buildJobUpdateSummaries(UPDATE_KEY).get(0),
        taskConfig(2, 2, 2, true),
        1,
        config,
        1);

    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkJobUpdate(update, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(6, 6, 6), from(6, 6, 6), EMPTY, from(0, 0, 0), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaNewUpdateAddsInstances() {
    expectQuota(ResourceAggregate.create(6, 6, 6)).times(2);
    expectTasks(prodTask("foo", 2, 2, 2), prodTask(JOB_NAME, 2, 2, 2)).times(2);
    expectNoJobUpdates().times(2);

    TaskConfig config = taskConfig(2, 2, 2, true);
    JobUpdate update = buildJobUpdate(
        buildJobUpdateSummaries(UPDATE_KEY).get(0),
        config,
        1,
        config,
        3);

    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkJobUpdate(update, storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(6, 6, 6), from(4, 4, 4), EMPTY, from(0, 0, 0), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaNewUpdateRemovesInstances() {
    expectQuota(ResourceAggregate.create(6, 6, 6)).times(2);
    expectTasks(
        prodTask("foo", 2, 2, 2),
        createTask(JOB_NAME, "id1", 2, 2, 2, true, 0),
        createTask(JOB_NAME, "id2", 2, 2, 2, true, 1)).times(2);
    expectNoJobUpdates().times(2);

    TaskConfig config = taskConfig(2, 2, 2, true);
    JobUpdate update = buildJobUpdate(
        buildJobUpdateSummaries(UPDATE_KEY).get(0),
        config,
        1,
        config,
        1);

    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkJobUpdate(update, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(6, 6, 6), from(6, 6, 6), EMPTY, from(0, 0, 0), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaNewUpdateSkippedForNonProdDesiredState() {
    TaskConfig config = taskConfig(2, 2, 2, false);
    JobUpdate update = buildJobUpdate(
        buildJobUpdateSummaries(UPDATE_KEY).get(0),
        taskConfig(2, 2, 2, true),
        1,
        config,
        1);

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkJobUpdate(update, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaNewUpdateSkippedForDedicatedDesiredState() {
    TaskConfig config = taskConfig(2, 2, 2, false);
    JobUpdate update = buildJobUpdate(
        buildJobUpdateSummaries(UPDATE_KEY).get(0),
        prodDedicatedTask("dedicatedJob", 1, 1, 1).getAssignedTask().getTask(),
        1,
        config,
        1);

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkJobUpdate(update, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaNewUpdateSkippedForEmptyDesiredState() {
    TaskConfig config = taskConfig(2, 2, 2, true);
    JobUpdate update = buildJobUpdate(
        buildJobUpdateSummaries(UPDATE_KEY).get(0),
        config,
        1,
        config,
        1);
    JobUpdate updateBuilder = update.withInstructions(
        inst -> inst.withDesiredState((InstanceTaskConfig) null));

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkJobUpdate(updateBuilder, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testSaveQuotaPasses() throws Exception {
    expectNoJobUpdates();
    expectNoCronJobs();
    ScheduledTask prodTask = prodTask("foo", 1, 1, 1);
    expectTasks(prodTask);
    expectQuota(ResourceAggregate.create(1, 1, 1));

    storageUtil.quotaStore.saveQuota(ROLE, QUOTA);

    control.replay();
    quotaManager.saveQuota(
        ROLE,
        QUOTA,
        storageUtil.mutableStoreProvider);
  }

  @Test
  public void testRemoveQuota() throws Exception {
    expectNoJobUpdates();
    expectNoCronJobs();
    expectNoTasks();
    expectQuota(ResourceAggregate.create(1, 1, 1));

    storageUtil.quotaStore.saveQuota(ROLE, EMPTY);

    control.replay();
    quotaManager.saveQuota(
        ROLE,
        ResourceAggregate.builder().build(),
        storageUtil.mutableStoreProvider);
  }

  @Test(expected = QuotaException.class)
  public void testSaveQuotaFailsNegativeValues() throws Exception {
    control.replay();
    quotaManager.saveQuota(
        ROLE,
        ResourceAggregate.create(-2.0, 4, 5),
        storageUtil.mutableStoreProvider);
  }

  @Test(expected = QuotaException.class)
  public void testSaveQuotaFailsWhenBelowCurrentReservation() throws Exception {
    expectNoJobUpdates();
    expectNoCronJobs();
    ScheduledTask prodTask = prodTask("foo", 10, 100, 100);
    expectTasks(prodTask);
    expectQuota(ResourceAggregate.create(20, 200, 200));

    control.replay();

    quotaManager.saveQuota(
        ROLE,
        ResourceAggregate.create(1, 1, 1),
        storageUtil.mutableStoreProvider);
  }

  @Test
  public void testCheckQuotaCronUpdateDownsize() {
    expectQuota(ResourceAggregate.create(5, 5, 5)).times(2);
    expectNoTasks().times(2);
    expectNoJobUpdates().times(2);

    JobConfiguration job = createJob(prodTask("pc", 4, 4, 4), 1);
    expectCronJobs(job, createJob(nonProdTask("npc", 7, 7, 7), 1)).times(2);
    expectCronJob(job);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkCronUpdate(createJob(prodTask("pc", 1, 1, 1), 2), storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(5, 5, 5), from(4, 4, 4), EMPTY, from(7, 7, 7), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaCronUpdateUpsize() {
    expectQuota(ResourceAggregate.create(5, 5, 5)).times(2);
    expectNoTasks().times(2);
    expectNoJobUpdates().times(2);

    JobConfiguration job = createJob(prodTask("pc", 4, 4, 4), 1);
    expectCronJobs(job, createJob(nonProdTask("npc", 7, 7, 7), 1)).times(2);
    expectCronJob(job);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkCronUpdate(createJob(prodTask("pc", 5, 5, 5), 1), storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(5, 5, 5), from(4, 4, 4), EMPTY, from(7, 7, 7), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaCronUpdateFails() {
    expectQuota(ResourceAggregate.create(5, 5, 5)).times(2);
    expectNoTasks().times(2);
    expectNoJobUpdates().times(2);

    JobConfiguration job = createJob(prodTask("pc", 4, 4, 4), 1);
    expectCronJobs(job).times(2);
    expectCronJob(job);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkCronUpdate(createJob(prodTask("pc", 5, 5, 5), 2), storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(5, 5, 5), from(4, 4, 4), EMPTY, EMPTY, EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaCronCreate() {
    expectQuota(ResourceAggregate.create(5, 5, 5)).times(2);
    expectNoTasks().times(2);
    expectNoJobUpdates().times(2);
    expectNoCronJobs().times(2);
    expectNoCronJob();

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkCronUpdate(createJob(prodTask("pc", 5, 5, 5), 1), storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(5, 5, 5), EMPTY, EMPTY, EMPTY, EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaNonProdCron() {
    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkCronUpdate(createJob(nonProdTask("np", 5, 5, 5), 1), storeProvider);

    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  private IExpectationSetters<?> expectTasks(ScheduledTask... tasks) {
    return storageUtil.expectTaskFetch(ACTIVE_QUERY, tasks);
  }

  private void expectJobUpdates(TaskConfig initial, TaskConfig desired) {
    expectJobUpdates(initial, 1, desired, 1, 1);
  }

  private void expectJobUpdates(TaskConfig initial, TaskConfig desired, int times) {
    expectJobUpdates(initial, 1, desired, 1, times);
  }

  private void expectJobUpdates(
      TaskConfig initial,
      int intialInstances,
      TaskConfig desired,
      int desiredInstances,
      int times) {

    JobUpdateKey key = JobUpdateKey.create(initial.getJob(), "u1");
    List<JobUpdateSummary> summaries = buildJobUpdateSummaries(key);
    JobUpdate update =
        buildJobUpdate(summaries.get(0), initial, intialInstances, desired, desiredInstances);

    expect(jobUpdateStore.fetchJobUpdateSummaries(updateQuery(initial.getJob().getRole())))
        .andReturn(summaries)
        .times(times);

    expect(jobUpdateStore.fetchJobUpdate(key)).andReturn(Optional.of(update)).times(times);

  }

  private List<JobUpdateSummary> buildJobUpdateSummaries(JobUpdateKey key) {
    return ImmutableList.of(JobUpdateSummary.builder().setKey(key).build());
  }

  private JobUpdate buildJobUpdate(
      JobUpdateSummary summary,
      TaskConfig initial,
      int intialInstances,
      TaskConfig desired,
      int desiredInstances) {

    return JobUpdate.builder()
        .setSummary(summary)
        .setInstructions(JobUpdateInstructions.builder()
            .setDesiredState(InstanceTaskConfig.builder()
                .setTask(desired)
                .setInstances(Range.create(0, desiredInstances - 1))
                .build())
            .setInitialState(InstanceTaskConfig.builder()
                .setTask(initial)
                .setInstances(Range.create(0, intialInstances - 1))
                .build())
            .build())
        .build();
  }

  private IExpectationSetters<?> expectNoJobUpdates() {
    return expect(jobUpdateStore.fetchJobUpdateSummaries(updateQuery(ROLE)))
        .andReturn(ImmutableList.of());
  }

  private IExpectationSetters<?> expectNoTasks() {
    return expectTasks();
  }

  private IExpectationSetters<?> expectNoCronJobs() {
    return expect(storageUtil.jobStore.fetchJobs()).andReturn(ImmutableSet.of());
  }

  private IExpectationSetters<?> expectCronJobs(JobConfiguration... jobs) {
    ImmutableSet.Builder<JobConfiguration> builder = ImmutableSet.builder();
    for (JobConfiguration job : jobs) {
      builder.add(job);
    }

    return expect(storageUtil.jobStore.fetchJobs()).andReturn(builder.build());
  }

  private IExpectationSetters<?> expectCronJob(JobConfiguration job) {
    return expect(storageUtil.jobStore.fetchJob(job.getKey())).andReturn(Optional.of(job));
  }

  private IExpectationSetters<?> expectNoCronJob() {
    return expect(storageUtil.jobStore.fetchJob(anyObject(JobKey.class)))
        .andReturn(Optional.absent());
  }

  private IExpectationSetters<Optional<ResourceAggregate>> expectQuota(ResourceAggregate quota) {
    return expect(storageUtil.quotaStore.fetchQuota(ROLE))
        .andReturn(Optional.of(quota));
  }

  private TaskConfig taskConfig(int cpus, int ramMb, int diskMb, boolean production) {
    return createTask(JOB_NAME, "newId", cpus, ramMb, diskMb, production, 0)
        .getAssignedTask()
        .getTask();
  }

  private ScheduledTask prodTask(String jobName, int cpus, int ramMb, int diskMb) {
    return createTask(jobName, jobName + "id1", cpus, ramMb, diskMb, true, 0);
  }

  private ScheduledTask prodDedicatedTask(String jobName, int cpus, int ramMb, int diskMb) {
    return makeDedicated(prodTask(jobName, cpus, ramMb, diskMb));
  }

  private ScheduledTask nonProdDedicatedTask(String jobName, int cpus, int ramMb, int diskMb) {
    return makeDedicated(nonProdTask(jobName, cpus, ramMb, diskMb));
  }

  private static ScheduledTask makeDedicated(ScheduledTask task) {
    return task.withAssignedTask(at -> at.withTask(
        t -> t.withConstraints(ImmutableSet.of(
            Constraint.create(
                "dedicated",
                TaskConstraint.value(ValueConstraint.create(false, ImmutableSet.of("host"))))))));
  }

  private ScheduledTask nonProdTask(String jobName, int cpus, int ramMb, int diskMb) {
    return createTask(jobName, jobName + "id1", cpus, ramMb, diskMb, false, 0);
  }

  private ScheduledTask createTask(
      String jobName,
      String taskId,
      int cpus,
      int ramMb,
      int diskMb,
      boolean production,
      int instanceId) {

    return TaskTestUtil.makeTask(taskId, JobKeys.from(ROLE, ENV, jobName))
        .withAssignedTask(
            at -> at.withInstanceId(instanceId)
                .withTask(t -> t.toBuilder()
                    .setNumCpus(cpus)
                    .setRamMb(ramMb)
                    .setDiskMb(diskMb)
                    .setProduction(production)
                    .build()));
  }

  private JobConfiguration createJob(ScheduledTask scheduledTask, int instanceCount) {
    TaskConfig task = scheduledTask.getAssignedTask().getTask();
    return JobConfiguration.builder()
        .setKey(task.getJob())
        .setTaskConfig(task)
        .setInstanceCount(instanceCount)
        .build();
  }

  private static ResourceAggregate from(double cpu, int ramMb, int diskMb) {
    return ResourceAggregate.create(cpu, ramMb, diskMb);
  }
}
