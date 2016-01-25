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
package org.apache.aurora.scheduler.storage;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

import org.apache.aurora.common.testing.TearDownTestCase;
import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.MesosContainer;
import org.apache.aurora.gen.Metadata;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.TaskStore.Mutable.TaskMutation;
import org.apache.aurora.scheduler.storage.testing.StorageEntityUtil;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.ASSIGNED;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.scheduler.base.TaskTestUtil.makeTask;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractTaskStoreTest extends TearDownTestCase {
  protected static final HostAttributes HOST_A = HostAttributes.builder()
      .setHost("hostA")
      .setAttributes(Attribute.create("zone", ImmutableSet.of("1a")))
      .setSlaveId("slaveIdA")
      .setMode(MaintenanceMode.NONE)
      .build();
  protected static final HostAttributes HOST_B = HostAttributes.builder()
      .setHost("hostB")
      .setAttributes(Attribute.create("zone", ImmutableSet.of("1a")))
      .setSlaveId("slaveIdB")
      .setMode(MaintenanceMode.NONE)
      .build();
  protected static final ScheduledTask TASK_A = createTask("a");
  protected static final ScheduledTask TASK_B =
      setContainer(createTask("b"), Container.mesos(MesosContainer.create()));
  protected static final ScheduledTask TASK_C = createTask("c");
  protected static final ScheduledTask TASK_D = createTask("d");

  protected Injector injector;
  protected Storage storage;

  protected abstract Module getStorageModule();

  @Before
  public void baseSetUp() {
    injector = Guice.createInjector(getStorageModule());
    storage = injector.getInstance(Storage.class);
    storage.prepare();

    storage.write((NoResult.Quiet) storeProvider -> {
      AttributeStore.Mutable attributeStore = storeProvider.getAttributeStore();
      attributeStore.saveHostAttributes(HOST_A);
      attributeStore.saveHostAttributes(HOST_B);
    });
  }

  private Optional<ScheduledTask> fetchTask(String taskId) {
    return storage.read(storeProvider -> storeProvider.getTaskStore().fetchTask(taskId));
  }

  private Iterable<ScheduledTask> fetchTasks(Query.Builder query) {
    return storage.read(storeProvider -> storeProvider.getTaskStore().fetchTasks(query));
  }

  protected void saveTasks(ScheduledTask... tasks) {
    saveTasks(ImmutableSet.copyOf(tasks));
  }

  private void saveTasks(Set<ScheduledTask> tasks) {
    storage.write((NoResult.Quiet)
        storeProvider -> storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.copyOf(tasks)));
  }

  private Optional<ScheduledTask> mutateTask(String taskId, TaskMutation mutation) {
    return storage.write(
        storeProvider -> storeProvider.getUnsafeTaskStore().mutateTask(taskId, mutation));
  }

  private boolean unsafeModifyInPlace(String taskId, TaskConfig taskConfiguration) {
    return storage.write(storeProvider ->
        storeProvider.getUnsafeTaskStore().unsafeModifyInPlace(taskId, taskConfiguration));
  }

  protected void deleteTasks(String... taskIds) {
    storage.write((NoResult.Quiet) storeProvider ->
        storeProvider.getUnsafeTaskStore().deleteTasks(ImmutableSet.copyOf(taskIds)));
  }

  protected void deleteAllTasks() {
    storage.write(
        (NoResult.Quiet) storeProvider -> storeProvider.getUnsafeTaskStore().deleteAllTasks());
  }

  @Test
  public void testSave() {
    ScheduledTask aWithHost = setHost(TASK_A, HOST_A);
    StorageEntityUtil.assertFullyPopulated(aWithHost);

    saveTasks(aWithHost, TASK_B);
    assertStoreContents(aWithHost, TASK_B);

    saveTasks(TASK_C, TASK_D);
    assertStoreContents(aWithHost, TASK_B, TASK_C, TASK_D);

    // Saving the same task should overwrite.
    ScheduledTask taskAModified = aWithHost.withStatus(RUNNING);
    saveTasks(taskAModified);
    assertStoreContents(taskAModified, TASK_B, TASK_C, TASK_D);
  }

  @Test
  public void testSaveWithMetadata() {
    ScheduledTask task = TASK_A.withAssignedTask(
        at -> at.withTask(
            t -> t.withMetadata(ImmutableSet.of(
                Metadata.create("package", "a"),
                Metadata.create("package", "b")))));
    saveTasks(task);
    assertStoreContents(task);
  }

  @Test
  public void testQuery() {
    assertStoreContents();
    saveTasks(TASK_A, TASK_B, TASK_C, TASK_D);

    assertQueryResults(Query.taskScoped("b"), TASK_B);
    assertQueryResults(Query.taskScoped("a", "d"), TASK_A, TASK_D);
    assertQueryResults(Query.roleScoped("role-c"), TASK_C);
    assertQueryResults(Query.envScoped("role-c", "env-c"), TASK_C);
    assertQueryResults(Query.envScoped("role-c", "devel"));
    assertQueryResults(
        Query.unscoped().byStatus(ASSIGNED),
        TASK_A, TASK_B, TASK_C, TASK_D);
    assertQueryResults(
        Query.instanceScoped(JobKeys.from("role-a", "env-a", "job-a"), 2).active(), TASK_A);
    assertQueryResults(Query.jobScoped(JobKeys.from("role-b", "env-b", "job-b")).active(), TASK_B);
    assertQueryResults(Query.jobScoped(JobKeys.from("role-b", "devel", "job-b")).active());

    assertQueryResults(TaskQuery.builder().setTaskIds().build(), TASK_A, TASK_B, TASK_C, TASK_D);
    assertQueryResults(
        TaskQuery.builder().setInstanceIds().build(),
        TASK_A, TASK_B, TASK_C, TASK_D);
    assertQueryResults(TaskQuery.builder().setStatuses().build(), TASK_A, TASK_B, TASK_C, TASK_D);
  }

  @Test
  public void testQueryMultipleInstances() {
    ImmutableSet.Builder<ScheduledTask> tasksBuilder = ImmutableSet.builder();
    for (int i = 0; i < 100; i++) {
      int id = i; // Capture i for the mutation closures below.
      tasksBuilder.add(TASK_A.withAssignedTask(at -> at.toBuilder()
          .setTaskId("id " + id)
          .setInstanceId(id)
          .build()));
    }
    Set<ScheduledTask> tasks = tasksBuilder.build();
    saveTasks(tasks);
    assertQueryResults(Query.unscoped(), tasks);
  }

  @Test
  public void testQueryBySlaveHost() {
    ScheduledTask a = setHost(makeTask("a", JobKeys.from("role", "env", "job")), HOST_A);
    ScheduledTask b = setHost(makeTask("b", JobKeys.from("role", "env", "job")), HOST_B);
    saveTasks(a, b);

    assertQueryResults(Query.slaveScoped(HOST_A.getHost()), a);
    assertQueryResults(Query.slaveScoped(HOST_A.getHost(), HOST_B.getHost()), a, b);
  }

  @Test
  public void testQueryByJobKeys() {
    assertStoreContents();
    saveTasks(TASK_A, TASK_B, TASK_C, TASK_D);

    assertQueryResults(
        Query.jobScoped(ImmutableSet.of(
            JobKeys.from("role-a", "env-a", "job-a"),
            JobKeys.from("role-b", "env-b", "job-b"),
            JobKeys.from("role-c", "env-c", "job-c"))),
        TASK_A, TASK_B, TASK_C);

    // Conflicting jobs will produce the result from the last added JobKey
    assertQueryResults(
        Query.jobScoped(JobKeys.from("role-a", "env-a", "job-a"))
            .byJobKeys(ImmutableSet.of(JobKeys.from("role-b", "env-b", "job-b"))),
        TASK_B);

    // The .byJobKeys will override the previous scoping and OR all of the keys.
    assertQueryResults(
        Query.jobScoped(JobKeys.from("role-a", "env-a", "job-a"))
            .byJobKeys(ImmutableSet.of(
                JobKeys.from("role-b", "env-b", "job-b"),
                JobKeys.from("role-a", "env-a", "job-a"))),
        TASK_A, TASK_B);

    // Combination of individual field and jobKeys is allowed.
    assertQueryResults(
        Query.roleScoped("role-b")
            .byJobKeys(ImmutableSet.of(
                JobKeys.from("role-b", "env-b", "job-b"),
                JobKeys.from("role-a", "env-a", "job-a"))),
        TASK_B);
  }

  @Test
  public void testMutate() {
    saveTasks(TASK_A, TASK_B, TASK_C, TASK_D);
    assertQueryResults(Query.statusScoped(RUNNING));

    mutateTask("a", task -> task.withStatus(RUNNING));

    assertQueryResults(
        Query.statusScoped(RUNNING),
        TASK_A.withStatus(RUNNING));

    assertEquals(Optional.absent(), mutateTask("nonexistent", task -> task.withStatus(RUNNING)));

    assertStoreContents(
        TASK_A.withStatus(RUNNING),
        TASK_B.withStatus(ASSIGNED),
        TASK_C.withStatus(ASSIGNED),
        TASK_D.withStatus(ASSIGNED));
  }

  @Test
  public void testUnsafeModifyInPlace() {
    TaskConfig updated =  TASK_A.getAssignedTask().getTask()
        .withExecutorConfig(ExecutorConfig.create("aurora", "new_config"));

    String taskId = Tasks.id(TASK_A);
    assertFalse(unsafeModifyInPlace(taskId, updated));

    saveTasks(TASK_A);
    assertTrue(unsafeModifyInPlace(taskId, updated));
    assertEquals(updated, fetchTask(taskId).get().getAssignedTask().getTask());

    deleteTasks(taskId);
    assertFalse(unsafeModifyInPlace(taskId, updated));
  }

  @Test
  public void testDelete() {
    saveTasks(TASK_A, TASK_B, TASK_C, TASK_D);
    deleteTasks("a");
    assertStoreContents(TASK_B, TASK_C, TASK_D);
    deleteTasks("c");
    assertStoreContents(TASK_B, TASK_D);
    deleteTasks("b", "d");
    assertStoreContents();
  }

  @Test
  public void testDeleteAll() {
    saveTasks(TASK_A, TASK_B, TASK_C, TASK_D);
    deleteAllTasks();
    assertStoreContents();
  }

  @Test
  public void testConsistentJobIndex() {
    ScheduledTask a = makeTask("a", JobKeys.from("jim", "test", "job"));
    ScheduledTask b = makeTask("b", JobKeys.from("jim", "test", "job"));
    ScheduledTask c = makeTask("c", JobKeys.from("jim", "test", "job2"));
    ScheduledTask d = makeTask("d", JobKeys.from("joe", "test", "job"));
    ScheduledTask e = makeTask("e", JobKeys.from("jim", "prod", "job"));
    Query.Builder jimsJob = Query.jobScoped(JobKeys.from("jim", "test", "job"));
    Query.Builder jimsJob2 = Query.jobScoped(JobKeys.from("jim", "test", "job2"));
    Query.Builder joesJob = Query.jobScoped(JobKeys.from("joe", "test", "job"));

    saveTasks(a, b, c, d, e);
    assertQueryResults(jimsJob, a, b);
    assertQueryResults(jimsJob2, c);
    assertQueryResults(joesJob, d);

    deleteTasks(Tasks.id(b));
    assertQueryResults(jimsJob, a);
    assertQueryResults(jimsJob2, c);
    assertQueryResults(joesJob, d);

    mutateTask(Tasks.id(a), task -> task.withStatus(RUNNING));
    ScheduledTask aRunning = a.withStatus(RUNNING);
    assertQueryResults(jimsJob, aRunning);
    assertQueryResults(jimsJob2, c);
    assertQueryResults(joesJob, d);

    deleteTasks(Tasks.id(d));
    assertQueryResults(joesJob);

    deleteTasks(Tasks.id(d));
    assertQueryResults(jimsJob, aRunning);
    assertQueryResults(jimsJob2, c);
    assertQueryResults(joesJob);

    saveTasks(b);
    assertQueryResults(jimsJob, aRunning, b);
    assertQueryResults(jimsJob2, c);
    assertQueryResults(joesJob);
  }

  @Test
  public void testCanonicalTaskConfigs() {
    ScheduledTask a = createTask("a");
    ScheduledTask b = createTask("a");
    ScheduledTask c = createTask("a");
    saveTasks(a, b, c);
    Set<ScheduledTask> inserted = ImmutableSet.of(a, b, c);

    Set<TaskConfig> storedConfigs = FluentIterable.from(fetchTasks(Query.unscoped()))
        .transform(Tasks::getConfig)
        .toSet();
    assertEquals(
        FluentIterable.from(inserted).transform(Tasks::getConfig).toSet(),
        storedConfigs);
    Map<TaskConfig, TaskConfig> identityMap = Maps.newIdentityHashMap();
    for (TaskConfig stored : storedConfigs) {
      identityMap.put(stored, stored);
    }
    assertEquals(
        ImmutableMap.of(Tasks.getConfig(a), Tasks.getConfig(a)),
        identityMap);
  }

  private static ScheduledTask setHost(ScheduledTask task, HostAttributes host) {
    return task.withAssignedTask(
        at -> at.toBuilder().setSlaveHost(host.getHost()).setSlaveId(host.getSlaveId()).build());
  }

  private static ScheduledTask unsetHost(ScheduledTask task) {
    return task.withAssignedTask(at -> at.toBuilder().setSlaveHost(null).setSlaveId(null).build());
  }

  private static ScheduledTask setConfigData(ScheduledTask task, String configData) {
    return task.withAssignedTask(
        at -> at.withTask(t -> t.withExecutorConfig(ec -> ec.withData(configData))));
  }

  @Test
  public void testAddSlaveHost() {
    final ScheduledTask a = createTask("a");
    saveTasks(a);
    assertQueryResults(Query.slaveScoped(HOST_A.getHost()));

    final ScheduledTask b = setHost(a, HOST_A);
    Optional<ScheduledTask> result = mutateTask(Tasks.id(a),
        task -> {
          assertEquals(a, task);
          return b;
        });
    assertEquals(Optional.of(b), result);
    assertQueryResults(Query.slaveScoped(HOST_A.getHost()), b);

    // Unrealistic behavior, but proving that the secondary index can handle key mutations.
    final ScheduledTask c = setHost(b, HOST_B);
    Optional<ScheduledTask> result2 = mutateTask(Tasks.id(a),
        task -> {
          assertEquals(b, task);
          return c;
        });
    assertEquals(Optional.of(c), result2);
    assertQueryResults(Query.slaveScoped(HOST_B.getHost()), c);

    deleteTasks(Tasks.id(a));
    assertQueryResults(Query.slaveScoped(HOST_B.getHost()));
  }

  @Test
  public void testUnsetSlaveHost() {
    // Unrealistic behavior, but proving that the secondary index does not become stale.

    final ScheduledTask a = setHost(createTask("a"), HOST_A);
    saveTasks(a);
    assertQueryResults(Query.slaveScoped(HOST_A.getHost()), a);

    final ScheduledTask b = unsetHost(a);
    Optional<ScheduledTask> result = mutateTask(Tasks.id(a),
        task -> {
          assertEquals(a, task);
          return b;
        });
    assertEquals(Optional.of(b), result);
    assertQueryResults(Query.slaveScoped(HOST_A.getHost()));
    assertQueryResults(Query.taskScoped(Tasks.id(b)), b);
  }

  @Test
  public void testTasksOnSameHost() {
    final ScheduledTask a = setHost(createTask("a"), HOST_A);
    final ScheduledTask b = setHost(createTask("b"), HOST_A);
    saveTasks(a, b);
    assertQueryResults(Query.slaveScoped(HOST_A.getHost()), a, b);
  }

  @Test
  public void testSaveOverwrites() {
    // Ensures that saving a task with an existing task ID is effectively the same as a mutate,
    // and does not result in a duplicate object in the primary or secondary index.

    final ScheduledTask a = setHost(createTask("a"), HOST_A);
    saveTasks(a);

    final ScheduledTask updated = setConfigData(a, "new config data");
    saveTasks(updated);
    assertQueryResults(Query.taskScoped(Tasks.id(a)), updated);
    assertQueryResults(Query.slaveScoped(HOST_A.getHost()), updated);
  }

  private Set<JobKey> getJobKeys() {
    return storage.read(storeProvider -> storeProvider.getTaskStore().getJobKeys());
  }

  private Set<JobKey> toJobKeys(ScheduledTask... tasks) {
    return FluentIterable.from(ImmutableSet.copyOf(tasks))
        .transform(Tasks::getJob)
        .toSet();
  }

  @Test
  public void testGetsJobKeys() {
    assertEquals(ImmutableSet.of(), getJobKeys());
    saveTasks(TASK_A);
    assertEquals(toJobKeys(TASK_A), getJobKeys());
    saveTasks(TASK_B, TASK_C);
    assertEquals(toJobKeys(TASK_A, TASK_B, TASK_C), getJobKeys());
    deleteTasks(Tasks.id(TASK_B));
    assertEquals(toJobKeys(TASK_A, TASK_C), getJobKeys());
    JobKey multiInstanceJob = JobKeys.from("role", "env", "instances");
    saveTasks(
        makeTask("instance1", multiInstanceJob),
        makeTask("instance2", multiInstanceJob),
        makeTask("instance3", multiInstanceJob));
    assertEquals(
        ImmutableSet.builder().addAll(toJobKeys(TASK_A, TASK_C)).add(multiInstanceJob).build(),
        getJobKeys());
  }

  @Ignore
  @Test
  public void testReadSecondaryIndexMultipleThreads() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(4,
        new ThreadFactoryBuilder().setNameFormat("SlowRead-%d").setDaemon(true).build());

    try {
      ImmutableSet.Builder<ScheduledTask> builder = ImmutableSet.builder();
      final int numTasks = 100;
      final int numJobs = 100;
      for (int j = 0; j < numJobs; j++) {
        for (int t = 0; t < numTasks; t++) {
          builder.add(makeTask("" + j + "-" + t, JobKeys.from("role", "env", "name" + j)));
        }
      }
      saveTasks(builder.build());

      final CountDownLatch read = new CountDownLatch(numJobs);
      for (int j = 0; j < numJobs; j++) {
        final int id = j;
        executor.submit(() -> {
          assertNotNull(fetchTasks(Query.jobScoped(JobKeys.from("role", "env", "name" + id))));
          read.countDown();
        });
        executor.submit(() -> saveTasks(createTask("TaskNew1" + id)));
      }

      read.await();
    } finally {
      MoreExecutors.shutdownAndAwaitTermination(executor, 1, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testNullVsEmptyRelations() throws Exception {
    // TODO(John Sirois): XXX no more null - trash test or reword motivation / test name?
    // Test for regression of AURORA-1476.

    TaskConfig nullMetadata = TaskTestUtil.makeConfig(TaskTestUtil.JOB)
        .withMetadata(ImmutableSet.of());

    ScheduledTask a = makeTask("a", nullMetadata);
    ScheduledTask b = makeTask("a", nullMetadata);
    ScheduledTask c = makeTask("a", nullMetadata);
    saveTasks(a);
    saveTasks(b);
    saveTasks(c);
  }

  private void assertStoreContents(ScheduledTask... tasks) {
    assertQueryResults(Query.unscoped(), tasks);
  }

  private void assertQueryResults(TaskQuery query, ScheduledTask... tasks) {
    assertQueryResults(Query.arbitrary(query), tasks);
  }

  private void assertQueryResults(Query.Builder query, ScheduledTask... tasks) {
    assertQueryResults(query, ImmutableSet.copyOf(tasks));
  }

  private void assertQueryResults(Query.Builder query, Set<ScheduledTask> tasks) {
    assertEquals(tasks, fetchTasks(query));
  }

  private static ScheduledTask createTask(String id) {
    return makeTask(id, JobKeys.from("role-" + id, "env-" + id, "job-" + id));
  }

  private static ScheduledTask setContainer(ScheduledTask task, Container container) {
    return task.withAssignedTask(at -> at.withTask(t -> t.withContainer(container)));
  }
}
