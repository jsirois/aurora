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
package org.apache.aurora.scheduler.preemptor;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.TierManager;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.filter.SchedulingFilterImpl;
import org.apache.aurora.scheduler.mesos.TaskExecutors;
import org.apache.aurora.scheduler.stats.CachedCounters;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.apache.mesos.Protos;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.MaintenanceMode.NONE;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.scheduler.ResourceType.CPUS;
import static org.apache.aurora.scheduler.TierInfo.DEFAULT;
import static org.apache.aurora.scheduler.base.TaskTestUtil.REVOCABLE_TIER;
import static org.apache.aurora.scheduler.filter.AttributeAggregate.EMPTY;
import static org.apache.aurora.scheduler.preemptor.PreemptorMetrics.MISSING_ATTRIBUTES_NAME;
import static org.apache.mesos.Protos.Offer;
import static org.apache.mesos.Protos.Resource;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class PreemptionVictimFilterTest extends EasyMockTest {
  private static final String USER_A = "user_a";
  private static final String USER_B = "user_b";
  private static final String USER_C = "user_c";
  private static final String JOB_A = "job_a";
  private static final String JOB_B = "job_b";
  private static final String JOB_C = "job_c";
  private static final String TASK_ID_A = "task_a";
  private static final String TASK_ID_B = "task_b";
  private static final String TASK_ID_C = "task_c";
  private static final String TASK_ID_D = "task_d";
  private static final String HOST = "host";
  private static final String RACK = "rack";
  private static final String SLAVE_ID = HOST + "_id";
  private static final String RACK_ATTRIBUTE = "rack";
  private static final String HOST_ATTRIBUTE = "host";
  private static final String OFFER = "offer";
  private static final Optional<HostOffer> NO_OFFER = Optional.absent();

  private StorageTestUtil storageUtil;
  private SchedulingFilter schedulingFilter;
  private FakeStatsProvider statsProvider;
  private PreemptorMetrics preemptorMetrics;
  private TierManager tierManager;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    statsProvider = new FakeStatsProvider();
    preemptorMetrics = new PreemptorMetrics(new CachedCounters(statsProvider));
    tierManager = createMock(TierManager.class);
  }

  private Optional<ImmutableSet<PreemptionVictim>> runFilter(
      ScheduledTask pendingTask,
      Optional<HostOffer> offer,
      ScheduledTask... victims) {

    PreemptionVictimFilter.PreemptionVictimFilterImpl filter =
        new PreemptionVictimFilter.PreemptionVictimFilterImpl(
            schedulingFilter,
            TaskExecutors.NO_OVERHEAD_EXECUTOR,
            preemptorMetrics,
            tierManager);

    return filter.filterPreemptionVictims(
        pendingTask.getAssignedTask().getTask(),
        preemptionVictims(victims),
        EMPTY,
        offer,
        storageUtil.mutableStoreProvider);
  }

  @Test
  public void testPreempted() throws Exception {
    setUpHost();

    schedulingFilter = createMock(SchedulingFilter.class);
    expect(tierManager.getTier(EasyMock.anyObject())).andReturn(DEFAULT);
    ScheduledTask lowPriority = assignToHost(makeTask(USER_A, JOB_A, TASK_ID_A));

    ScheduledTask highPriority = makeTask(USER_A, JOB_A, TASK_ID_B, 100);

    expectFiltering();

    control.replay();
    assertVictims(runFilter(highPriority, NO_OFFER, lowPriority), lowPriority);
  }

  @Test
  public void testLowestPriorityPreempted() throws Exception {
    setUpHost();

    schedulingFilter = createMock(SchedulingFilter.class);
    expect(tierManager.getTier(EasyMock.anyObject())).andReturn(DEFAULT);
    assignToHost(makeTask(USER_A, JOB_A, TASK_ID_A, 10));

    ScheduledTask lowerPriority = assignToHost(makeTask(USER_A, JOB_A, TASK_ID_B, 1));

    ScheduledTask highPriority = makeTask(USER_A, JOB_A, TASK_ID_C, 100);

    expectFiltering();

    control.replay();
    assertVictims(runFilter(highPriority, NO_OFFER, lowerPriority), lowerPriority);
  }

  @Test
  public void testOnePreemptableTask() throws Exception {
    setUpHost();

    schedulingFilter = createMock(SchedulingFilter.class);
    expect(tierManager.getTier(EasyMock.anyObject())).andReturn(DEFAULT);
    ScheduledTask highPriority = assignToHost(makeTask(USER_A, JOB_A, TASK_ID_A, 100));

    ScheduledTask lowerPriority = assignToHost(makeTask(USER_A, JOB_A, TASK_ID_B, 99));

    ScheduledTask lowestPriority = assignToHost(makeTask(USER_A, JOB_A, TASK_ID_C, 1));

    ScheduledTask pendingPriority = makeTask(USER_A, JOB_A, TASK_ID_D, 98);

    expectFiltering();

    control.replay();
    assertVictims(
        runFilter(pendingPriority, NO_OFFER, highPriority, lowerPriority, lowestPriority),
        lowestPriority);
  }

  @Test
  public void testHigherPriorityRunning() throws Exception {
    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask highPriority = assignToHost(makeTask(USER_A, JOB_A, TASK_ID_B, 100));

    ScheduledTask task = makeTask(USER_A, JOB_A, TASK_ID_A);

    control.replay();
    assertNoVictims(runFilter(task, NO_OFFER, highPriority));
  }

  @Test
  public void testProductionPreemptingNonproduction() throws Exception {
    setUpHost();

    schedulingFilter = createMock(SchedulingFilter.class);
    expect(tierManager.getTier(EasyMock.anyObject())).andReturn(DEFAULT);
    // Use a very low priority for the production task to show that priority is irrelevant.
    ScheduledTask p1 = makeProductionTask(USER_A, JOB_A, TASK_ID_A + "_p1", -1000);
    ScheduledTask a1 = assignToHost(makeTask(USER_A, JOB_A, TASK_ID_B + "_a1", 100));

    expectFiltering();

    control.replay();
    assertVictims(runFilter(p1, NO_OFFER, a1), a1);
  }

  @Test
  public void testProductionPreemptingNonproductionAcrossUsers() throws Exception {
    setUpHost();

    schedulingFilter = createMock(SchedulingFilter.class);
    expect(tierManager.getTier(EasyMock.anyObject())).andReturn(DEFAULT);
    // Use a very low priority for the production task to show that priority is irrelevant.
    ScheduledTask p1 = makeProductionTask(USER_A, JOB_A, TASK_ID_A + "_p1", -1000);
    ScheduledTask a1 = assignToHost(makeTask(USER_B, JOB_A, TASK_ID_B + "_a1", 100));

    expectFiltering();

    control.replay();
    assertVictims(runFilter(p1, NO_OFFER, a1), a1);
  }

  @Test
  public void testProductionUsersDoNotPreemptEachOther() throws Exception {
    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask p1 = makeProductionTask(USER_A, JOB_A, TASK_ID_A + "_p1", 1000);
    ScheduledTask a1 = assignToHost(makeProductionTask(USER_B, JOB_A, TASK_ID_B + "_a1", 0));

    control.replay();
    assertNoVictims(runFilter(p1, NO_OFFER, a1));
  }

  // Ensures a production task can preempt 2 tasks on the same host.
  @Test
  public void testProductionPreemptingManyNonProduction() throws Exception {
    schedulingFilter = new SchedulingFilterImpl(TaskExecutors.NO_OVERHEAD_EXECUTOR);
    expect(tierManager.getTier(EasyMock.anyObject())).andReturn(DEFAULT).times(5);
    ScheduledTask a1 =
        mutateTaskConfig(
            assignToHost(makeTask(USER_A, JOB_A, TASK_ID_A + "_a1")),
            tcb -> tcb.setNumCpus(1).setRamMb(512));

    ScheduledTask b1 =
        mutateTaskConfig(
            assignToHost(makeTask(USER_B, JOB_B, TASK_ID_B + "_b1")),
            tcb -> tcb.setNumCpus(1).setRamMb(512));

    setUpHost();

    ScheduledTask p1 =
        mutateTaskConfig(
            makeProductionTask(USER_B, JOB_B, TASK_ID_B + "_p1"),
            tcb -> tcb.setNumCpus(2).setRamMb(1024));

    control.replay();
    assertVictims(runFilter(p1, NO_OFFER, a1, b1), a1, b1);
  }

  // Ensures we select the minimal number of tasks to preempt
  @Test
  public void testMinimalSetPreempted() throws Exception {
    schedulingFilter = new SchedulingFilterImpl(TaskExecutors.NO_OVERHEAD_EXECUTOR);
    expect(tierManager.getTier(EasyMock.anyObject())).andReturn(DEFAULT).atLeastOnce();
    ScheduledTask a1 =
        mutateTaskConfig(
            assignToHost(makeTask(USER_A, JOB_A, TASK_ID_A + "_a1")),
            tcb -> tcb.setNumCpus(4).setRamMb(4096));

    ScheduledTask b1 =
        mutateTaskConfig(
            assignToHost(makeTask(USER_B, JOB_B, TASK_ID_B + "_b1")),
            tcb -> tcb.setNumCpus(1).setRamMb(512));

    ScheduledTask b2 =
        mutateTaskConfig(
            assignToHost(makeTask(USER_B, JOB_B, TASK_ID_B + "_b2")),
            tcb -> tcb.setNumCpus(1).setRamMb(512));

    setUpHost();

    ScheduledTask p1 =
        mutateTaskConfig(
            makeProductionTask(USER_C, JOB_C, TASK_ID_C + "_p1"),
            tcb -> tcb.setNumCpus(2).setRamMb(1024));

    control.replay();
    assertVictims(runFilter(p1, NO_OFFER, b1, b2, a1), a1);
  }

  // Ensures a production task *never* preempts a production task from another job.
  @Test
  public void testProductionJobNeverPreemptsProductionJob() throws Exception {
    schedulingFilter = new SchedulingFilterImpl(TaskExecutors.NO_OVERHEAD_EXECUTOR);
    ScheduledTask p1 =
        mutateTaskConfig(
            assignToHost(makeProductionTask(USER_A, JOB_A, TASK_ID_A + "_p1")),
            tcb -> tcb.setNumCpus(2).setRamMb(1024));

    setUpHost();

    ScheduledTask p2 =
        mutateTaskConfig(
            makeProductionTask(USER_B, JOB_B, TASK_ID_B + "_p2"),
            tcb -> tcb.setNumCpus(1).setRamMb(512));

    control.replay();
    assertNoVictims(runFilter(p2, NO_OFFER, p1));
  }

  // Ensures that we can preempt if a task + offer can satisfy a pending task.
  @Test
  public void testPreemptWithOfferAndTask() throws Exception {
    schedulingFilter = new SchedulingFilterImpl(TaskExecutors.NO_OVERHEAD_EXECUTOR);
    expect(tierManager.getTier(EasyMock.anyObject())).andReturn(DEFAULT);

    setUpHost();

    ScheduledTask a1 =
        mutateTaskConfig(
            assignToHost(makeTask(USER_A, JOB_A, TASK_ID_A + "_a1")),
            tcb -> tcb.setNumCpus(1).setRamMb(512));

    ScheduledTask p1 =
        mutateTaskConfig(
            makeProductionTask(USER_B, JOB_B, TASK_ID_B + "_p1"),
            tcb -> tcb.setNumCpus(2).setRamMb(1024));

    control.replay();
    assertVictims(
        runFilter(
            p1,
            makeOffer(OFFER, 1, Amount.of(512L, Data.MB), Amount.of(1L, Data.MB), 1, false),
            a1),
        a1);
  }

  // Ensures revocable offer resources are filtered out.
  @Test
  public void testRevocableOfferFiltered() throws Exception {
    schedulingFilter = new SchedulingFilterImpl(TaskExecutors.NO_OVERHEAD_EXECUTOR);
    expect(tierManager.getTier(EasyMock.anyObject())).andReturn(DEFAULT);

    setUpHost();

    ScheduledTask a1 =
        mutateTaskConfig(
            assignToHost(makeTask(USER_A, JOB_A, TASK_ID_A + "_a1")),
            tcb -> tcb.setNumCpus(1).setRamMb(512));

    ScheduledTask p1 =
        mutateTaskConfig(
            makeProductionTask(USER_B, JOB_B, TASK_ID_B + "_p1"),
            tcb -> tcb.setNumCpus(2).setRamMb(1024));

    control.replay();
    assertNoVictims(runFilter(
        p1,
        makeOffer(OFFER, 1, Amount.of(512L, Data.MB), Amount.of(1L, Data.MB), 1, true),
        a1));
  }

  // Ensures revocable task CPU is not considered for preemption.
  @Test
  public void testRevocableVictimsFiltered() throws Exception {
    schedulingFilter = new SchedulingFilterImpl(TaskExecutors.NO_OVERHEAD_EXECUTOR);
    expect(tierManager.getTier(EasyMock.anyObject())).andReturn(REVOCABLE_TIER);

    setUpHost();

    ScheduledTask a1 =
        mutateTaskConfig(
            assignToHost(makeTask(USER_A, JOB_A, TASK_ID_A + "_a1")),
            tcb -> tcb.setNumCpus(1).setRamMb(512));

    ScheduledTask p1 =
        mutateTaskConfig(
            makeProductionTask(USER_B, JOB_B, TASK_ID_B + "_p1"),
            tcb -> tcb.setNumCpus(2).setRamMb(1024));

    control.replay();
    assertNoVictims(runFilter(
        p1,
        makeOffer(OFFER, 1, Amount.of(512L, Data.MB), Amount.of(1L, Data.MB), 1, false),
        a1));
  }

  // Ensures revocable victim non-compressible resources are still considered.
  @Test
  public void testRevocableVictimRamUsed() throws Exception {
    schedulingFilter = new SchedulingFilterImpl(TaskExecutors.NO_OVERHEAD_EXECUTOR);
    expect(tierManager.getTier(EasyMock.anyObject())).andReturn(REVOCABLE_TIER);

    setUpHost();

    ScheduledTask a1 =
        mutateTaskConfig(
            assignToHost(makeTask(USER_A, JOB_A, TASK_ID_A + "_a1")),
            tcb -> tcb.setNumCpus(1).setRamMb(512));

    ScheduledTask p1 =
        mutateTaskConfig(
            makeProductionTask(USER_B, JOB_B, TASK_ID_B + "_p1"),
            tcb -> tcb.setNumCpus(2).setRamMb(1024));

    control.replay();
    assertVictims(
        runFilter(
            p1,
            makeOffer(OFFER, 2, Amount.of(512L, Data.MB), Amount.of(1L, Data.MB), 1, false),
            a1),
        a1);
  }

  // Ensures we can preempt if two tasks and an offer can satisfy a pending task.
  @Test
  public void testPreemptWithOfferAndMultipleTasks() throws Exception {
    schedulingFilter = new SchedulingFilterImpl(TaskExecutors.NO_OVERHEAD_EXECUTOR);
    expect(tierManager.getTier(EasyMock.anyObject())).andReturn(DEFAULT).times(5);

    setUpHost();

    ScheduledTask a1 =
        mutateTaskConfig(
            assignToHost(makeTask(USER_A, JOB_A, TASK_ID_A + "_a1")),
            tcb -> tcb.setNumCpus(1).setRamMb(512));

    ScheduledTask a2 =
        mutateTaskConfig(
            assignToHost(makeTask(USER_A, JOB_B, TASK_ID_A + "_a2")),
            tcb -> tcb.setNumCpus(1).setRamMb(512));

    ScheduledTask p1 =
        mutateTaskConfig(
            makeProductionTask(USER_B, JOB_B, TASK_ID_B + "_p1"),
            tcb -> tcb.setNumCpus(4).setRamMb(2048));

    control.replay();
    Optional<HostOffer> offer =
        makeOffer(OFFER, 2, Amount.of(1024L, Data.MB), Amount.of(1L, Data.MB), 1, false);
    assertVictims(runFilter(p1, offer, a1, a2), a1, a2);
  }

  @Test
  public void testNoPreemptionVictims() {
    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask task = makeProductionTask(USER_A, JOB_A, TASK_ID_A);

    control.replay();

    assertNoVictims(runFilter(task, NO_OFFER));
  }

  @Test
  public void testMissingAttributes() {
    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask task = assignToHost(makeProductionTask(USER_A, JOB_A, TASK_ID_A));

    ScheduledTask a1 =
        mutateTaskConfig(
            assignToHost(makeTask(USER_A, JOB_A, TASK_ID_A + "_a1")),
            tcb -> tcb.setNumCpus(1).setRamMb(512));

    expect(storageUtil.attributeStore.getHostAttributes(HOST)).andReturn(Optional.absent());

    control.replay();

    assertNoVictims(runFilter(task, NO_OFFER, a1));
    assertEquals(1L, statsProvider.getLongValue(MISSING_ATTRIBUTES_NAME));
  }

  @Test
  public void testAllVictimsVetoed() {
    schedulingFilter = createMock(SchedulingFilter.class);
    expect(tierManager.getTier(EasyMock.anyObject())).andReturn(DEFAULT);
    ScheduledTask task = assignToHost(makeProductionTask(USER_A, JOB_A, TASK_ID_A));

    ScheduledTask a1 =
        mutateTaskConfig(
            assignToHost(makeTask(USER_A, JOB_A, TASK_ID_A + "_a1")),
            tcb -> tcb.setNumCpus(1).setRamMb(512));

    setUpHost();
    expectFiltering(Optional.of(Veto.constraintMismatch("ban")));

    control.replay();

    assertNoVictims(runFilter(task, NO_OFFER, a1));
  }

  private ScheduledTask mutateTaskConfig(
      ScheduledTask scheduledTask,
      Consumer<TaskConfig.Builder> mutator) {

    TaskConfig.Builder mutableConfig = scheduledTask.getAssignedTask().getTask().toBuilder();
    mutator.accept(mutableConfig);
    return scheduledTask.toBuilder()
        .setAssignedTask(scheduledTask.getAssignedTask().toBuilder()
            .setTask(mutableConfig.build())
            .build())
        .build();
  }

  private static ImmutableSet<PreemptionVictim> preemptionVictims(ScheduledTask... tasks) {
    return FluentIterable.from(ImmutableSet.copyOf(tasks))
        .transform(
            new Function<ScheduledTask, PreemptionVictim>() {
              @Override
              public PreemptionVictim apply(ScheduledTask task) {
                return PreemptionVictim.fromTask(task.getAssignedTask());
              }
            }).toSet();
  }

  private static void assertVictims(
      Optional<ImmutableSet<PreemptionVictim>> actual,
      ScheduledTask... expected) {

    assertEquals(Optional.of(preemptionVictims(expected)), actual);
  }

  private static void assertNoVictims(Optional<ImmutableSet<PreemptionVictim>> actual) {
    assertEquals(Optional.<ImmutableSet<PreemptionVictim>>absent(), actual);
  }

  private Optional<HostOffer> makeOffer(
      String offerId,
      double cpu,
      Amount<Long, Data> ram,
      Amount<Long, Data> disk,
      int numPorts,
      boolean revocable) {

    List<Resource> resources =
        new ResourceSlot(cpu, ram, disk, numPorts).toResourceList(DEFAULT);
    if (revocable) {
      resources = ImmutableList.<Resource>builder()
          .addAll(FluentIterable.from(resources)
              .filter(e -> !e.getName().equals(CPUS.getName()))
              .toList())
          .add(Protos.Resource.newBuilder()
              .setName(CPUS.getName())
              .setType(Protos.Value.Type.SCALAR)
              .setScalar(Protos.Value.Scalar.newBuilder().setValue(cpu))
              .setRevocable(Resource.RevocableInfo.newBuilder())
              .build())
          .build();
    }
    Offer.Builder builder = Offer.newBuilder();
    builder.getIdBuilder().setValue(offerId);
    builder.getFrameworkIdBuilder().setValue("framework-id");
    builder.getSlaveIdBuilder().setValue(SLAVE_ID);
    builder.setHostname(HOST);
    builder.addAllResources(resources);

    return Optional.of(new HostOffer(
        builder.build(),
        HostAttributes.builder().setMode(MaintenanceMode.NONE).build()));
  }

  private IExpectationSetters<Set<SchedulingFilter.Veto>> expectFiltering() {
    return expectFiltering(Optional.absent());
  }

  private IExpectationSetters<Set<SchedulingFilter.Veto>> expectFiltering(
      final Optional<Veto> veto) {

    return expect(schedulingFilter.filter(
        EasyMock.anyObject(),
        EasyMock.anyObject()))
        .andAnswer(
            new IAnswer<Set<SchedulingFilter.Veto>>() {
              @Override
              public Set<SchedulingFilter.Veto> answer() {
                return veto.asSet();
              }
            });
  }

  static ScheduledTask makeTask(
      String role,
      String job,
      String taskId,
      int priority,
      String env,
      boolean production) {

    AssignedTask assignedTask = AssignedTask.builder()
        .setTaskId(taskId)
        .setTask(TaskConfig.builder()
            .setJob(JobKey.create(role, env, job))
            .setPriority(priority)
            .setProduction(production)
            .setJobName(job)
            .setEnvironment(env)
            .setConstraints()
            .build())
        .build();
    return ScheduledTask.builder().setAssignedTask(assignedTask).build();
  }

  static ScheduledTask makeTask(String role, String job, String taskId) {
    return makeTask(role, job, taskId, 0, "dev", false);
  }

  private ScheduledTask makeProductionTask(String role, String job, String taskId) {
    return makeTask(role, job, taskId, 0, "prod", true);
  }

  private ScheduledTask makeProductionTask(String role, String job, String taskId, int priority) {
    return makeTask(role, job, taskId, priority, "prod", true);
  }

  private ScheduledTask makeTask(String role, String job, String taskId, int priority) {
    return makeTask(role, job, taskId, priority, "dev", false);
  }

  private ScheduledTask assignToHost(ScheduledTask task) {
    return task.toBuilder()
        .setStatus(RUNNING)
        .setTaskEvents(
            ImmutableList.<TaskEvent>builder()
                .addAll(task.getTaskEvents())
                .add(TaskEvent.create(0, RUNNING))
                .build())
        .setAssignedTask(task.getAssignedTask().toBuilder()
            .setSlaveHost(HOST)
            .setSlaveId(SLAVE_ID)
            .build())
        .build();
  }

  private Attribute host(String host) {
    return Attribute.create(HOST_ATTRIBUTE, ImmutableSet.of(host));
  }

  private Attribute rack(String rack) {
    return Attribute.create(RACK_ATTRIBUTE, ImmutableSet.of(rack));
  }

  // Sets up a normal host, no dedicated hosts and no maintenance.
  private void setUpHost() {
    HostAttributes hostAttrs = HostAttributes.builder()
        .setHost(HOST)
        .setSlaveId(HOST + "_id")
        .setMode(NONE)
        .setAttributes(rack(RACK), host(RACK))
        .build();

    expect(storageUtil.attributeStore.getHostAttributes(HOST))
        .andReturn(Optional.of(hostAttrs)).anyTimes();
  }
}
