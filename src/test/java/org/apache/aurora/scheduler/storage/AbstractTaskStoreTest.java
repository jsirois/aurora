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
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
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
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.TaskStore.Mutable.TaskMutation;
import org.apache.aurora.scheduler.storage.testing.StorageEntityUtil;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.scheduler.base.TaskTestUtil.makeTask;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractTaskStoreTest extends TearDownTestCase {
  protected static final HostAttributes HOST_A =
      HostAttributes.builder()
          .setHost("hostA")
          .setAttributes(Attribute.create("zone", ImmutableSet.of("1a")))
          .setSlaveId("slaveIdA")
          .setMode(MaintenanceMode.NONE)
          .build();
  protected static final HostAttributes HOST_B =
      HostAttributes.builder()
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

    storage.write(new Storage.MutateWork.NoResult.Quiet() {
      @Override
      public void execute(Storage.MutableStoreProvider storeProvider) {
        AttributeStore.Mutable attributeStore = storeProvider.getAttributeStore();
        attributeStore.saveHostAttributes(HOST_A);
        attributeStore.saveHostAttributes(HOST_B);
      }
    });
  }

  private Iterable<ScheduledTask> fetchTasks(final Query.Builder query) {
    return storage.read(new Storage.Work.Quiet<Iterable<ScheduledTask>>() {
      @Override
      public Iterable<ScheduledTask> apply(Storage.StoreProvider storeProvider) {
        return storeProvider.getTaskStore().fetchTasks(query);
      }
    });
  }

  protected void saveTasks(final ScheduledTask... tasks) {
    saveTasks(ImmutableSet.copyOf(tasks));
  }

  private void saveTasks(final Set<ScheduledTask> tasks) {
    storage.write(new Storage.MutateWork.NoResult.Quiet() {
      @Override
      public void execute(Storage.MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.copyOf(tasks));
      }
    });
  }

  private ImmutableSet<ScheduledTask> mutateTasks(
      final Query.Builder query,
      final TaskMutation mutation) {

    return storage.write(new Storage.MutateWork.Quiet<ImmutableSet<ScheduledTask>>() {
      @Override
      public ImmutableSet<ScheduledTask> apply(Storage.MutableStoreProvider storeProvider) {
        return storeProvider.getUnsafeTaskStore().mutateTasks(query, mutation);
      }
    });
  }

  private boolean unsafeModifyInPlace(final String taskId, final TaskConfig taskConfiguration) {
    return storage.write(new Storage.MutateWork.Quiet<Boolean>() {
      @Override
      public Boolean apply(Storage.MutableStoreProvider storeProvider) {
        return storeProvider.getUnsafeTaskStore().unsafeModifyInPlace(taskId, taskConfiguration);
      }
    });
  }

  protected void deleteTasks(final String... taskIds) {
    storage.write(new Storage.MutateWork.NoResult.Quiet() {
      @Override
      public void execute(Storage.MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().deleteTasks(ImmutableSet.copyOf(taskIds));
      }
    });
  }

  protected void deleteAllTasks() {
    storage.write(new Storage.MutateWork.NoResult.Quiet() {
      @Override
      public void execute(Storage.MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().deleteAllTasks();
      }
    });
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
    ScheduledTask taskAModified = aWithHost.toBuilder().setStatus(RUNNING).build();
    saveTasks(taskAModified);
    assertStoreContents(taskAModified, TASK_B, TASK_C, TASK_D);
  }

  @Test
  public void testSaveWithMetadata() {
    ScheduledTask task = TASK_A.toBuilder()
        .setAssignedTask(TASK_A.getAssignedTask().toBuilder()
            .setTask(TASK_A.getAssignedTask().getTask().toBuilder()
                .setMetadata(Metadata.create("package", "a"), Metadata.create("package", "b"))
                .build())
            .build())
        .build();
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
        Query.unscoped().byStatus(ScheduleStatus.PENDING),
        TASK_A, TASK_B, TASK_C, TASK_D);
    assertQueryResults(
        Query.instanceScoped(JobKeys.from("role-a", "env-a", "job-a"), 2).active(), TASK_A);
    assertQueryResults(Query.jobScoped(JobKeys.from("role-b", "env-b", "job-b")).active(), TASK_B);
    assertQueryResults(Query.jobScoped(JobKeys.from("role-b", "devel", "job-b")).active());

    assertQueryResults(
        TaskQuery.builder().setTaskIds().build(),
        TASK_A, TASK_B, TASK_C, TASK_D);
    assertQueryResults(
        TaskQuery.builder().setInstanceIds().build(),
        TASK_A, TASK_B, TASK_C, TASK_D);
    assertQueryResults(
        TaskQuery.builder().setStatuses().build(),
        TASK_A, TASK_B, TASK_C, TASK_D);
  }

  @Test
  public void testQueryMultipleInstances() {
    ImmutableSet.Builder<ScheduledTask> tasksBuilder = ImmutableSet.builder();
    for (int i = 0; i < 100; i++) {
      tasksBuilder.add(TASK_A.toBuilder()
          .setAssignedTask(TASK_A.getAssignedTask().toBuilder()
              .setTaskId("id" + i)
              .setInstanceId(i)
              .build())
          .build());
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

    mutateTasks(Query.taskScoped("a"), new TaskMutation() {
      @Override
      public ScheduledTask apply(ScheduledTask task) {
        return task.toBuilder().setStatus(RUNNING).build();
      }
    });

    assertQueryResults(
        Query.statusScoped(RUNNING),
        TASK_A.toBuilder().setStatus(RUNNING).build());

    mutateTasks(Query.unscoped(), new TaskMutation() {
      @Override
      public ScheduledTask apply(ScheduledTask task) {
        return task.toBuilder().setStatus(ScheduleStatus.ASSIGNED).build();
      }
    });

    assertStoreContents(
        TASK_A.toBuilder().setStatus(ScheduleStatus.ASSIGNED).build(),
        TASK_B.toBuilder().setStatus(ScheduleStatus.ASSIGNED).build(),
        TASK_C.toBuilder().setStatus(ScheduleStatus.ASSIGNED).build(),
        TASK_D.toBuilder().setStatus(ScheduleStatus.ASSIGNED).build());
  }

  @Test
  public void testUnsafeModifyInPlace() {
    TaskConfig updated =
        TASK_A.getAssignedTask().getTask().toBuilder()
            .setExecutorConfig(ExecutorConfig.create("aurora", "new_config"))
            .build();

    String taskId = Tasks.id(TASK_A);
    assertFalse(unsafeModifyInPlace(taskId, updated));

    saveTasks(TASK_A);
    assertTrue(unsafeModifyInPlace(taskId, updated));
    Query.Builder query = Query.taskScoped(taskId);
    TaskConfig stored =
        Iterables.getOnlyElement(fetchTasks(query)).getAssignedTask().getTask();
    assertEquals(updated, stored);

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
    final ScheduledTask a = makeTask("a", JobKeys.from("jim", "test", "job"));
    final ScheduledTask b = makeTask("b", JobKeys.from("jim", "test", "job"));
    final ScheduledTask c = makeTask("c", JobKeys.from("jim", "test", "job2"));
    final ScheduledTask d = makeTask("d", JobKeys.from("joe", "test", "job"));
    final ScheduledTask e = makeTask("e", JobKeys.from("jim", "prod", "job"));
    final Query.Builder jimsJob = Query.jobScoped(JobKeys.from("jim", "test", "job"));
    final Query.Builder jimsJob2 = Query.jobScoped(JobKeys.from("jim", "test", "job2"));
    final Query.Builder joesJob = Query.jobScoped(JobKeys.from("joe", "test", "job"));

    saveTasks(a, b, c, d, e);
    assertQueryResults(jimsJob, a, b);
    assertQueryResults(jimsJob2, c);
    assertQueryResults(joesJob, d);

    deleteTasks(Tasks.id(b));
    assertQueryResults(jimsJob, a);
    assertQueryResults(jimsJob2, c);
    assertQueryResults(joesJob, d);

    mutateTasks(jimsJob, new TaskMutation() {
      @Override
      public ScheduledTask apply(ScheduledTask task) {
        return task.toBuilder().setStatus(RUNNING).build();
      }
    });
    ScheduledTask aRunning = a.toBuilder().setStatus(RUNNING).build();
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
    return task.toBuilder()
        .setAssignedTask(task.getAssignedTask().toBuilder()
            .setSlaveHost(host.getHost())
            .setSlaveId(host.getSlaveId())
            .build())
        .build();
  }

  private static ScheduledTask unsetHost(ScheduledTask task) {
    return task.toBuilder()
        .setAssignedTask(task.getAssignedTask().toBuilder()
            .setSlaveHost(null)
            .setSlaveId(null)
            .build())
        .build();
  }

  private static ScheduledTask setConfigData(ScheduledTask task, String configData) {
    return task.toBuilder()
        .setAssignedTask(task.getAssignedTask().toBuilder()
            .setTask(task.getAssignedTask().getTask().toBuilder()
                .setExecutorConfig(task.getAssignedTask().getTask().getExecutorConfig().toBuilder()
                    .setData(configData)
                    .build())
                .build())
            .build())
        .build();
  }

  @Test
  public void testAddSlaveHost() {
    final ScheduledTask a = createTask("a");
    saveTasks(a);
    assertQueryResults(Query.slaveScoped(HOST_A.getHost()));

    final ScheduledTask b = setHost(a, HOST_A);
    Set<ScheduledTask> result = mutateTasks(Query.taskScoped(Tasks.id(a)),
        new TaskMutation() {
          @Override
          public ScheduledTask apply(ScheduledTask task) {
            assertEquals(a, task);
            return b;
          }
        });
    assertEquals(ImmutableSet.of(b), result);
    assertQueryResults(Query.slaveScoped(HOST_A.getHost()), b);

    // Unrealistic behavior, but proving that the secondary index can handle key mutations.
    final ScheduledTask c = setHost(b, HOST_B);
    Set<ScheduledTask> result2 = mutateTasks(Query.taskScoped(Tasks.id(a)),
        new TaskMutation() {
          @Override
          public ScheduledTask apply(ScheduledTask task) {
            assertEquals(b, task);
            return c;
          }
        });
    assertEquals(ImmutableSet.of(c), result2);
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
    Set<ScheduledTask> result = mutateTasks(Query.taskScoped(Tasks.id(a)),
        new TaskMutation() {
          @Override
          public ScheduledTask apply(ScheduledTask task) {
            assertEquals(a, task);
            return b;
          }
        });
    assertEquals(ImmutableSet.of(b), result);
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
    return storage.read(new Storage.Work.Quiet<Set<JobKey>>() {
      @Override
      public Set<JobKey> apply(Storage.StoreProvider storeProvider) {
        return storeProvider.getTaskStore().getJobKeys();
      }
    });
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
        executor.submit(new Runnable() {
          @Override
          public void run() {
            assertNotNull(fetchTasks(Query.jobScoped(JobKeys.from("role", "env", "name" + id))));
            read.countDown();
          }
        });
        executor.submit(new Runnable() {
          @Override
          public void run() {
            saveTasks(createTask("TaskNew1" + id));
          }
        });
      }

      read.await();
    } finally {
      MoreExecutors.shutdownAndAwaitTermination(executor, 1, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testLegacyPermissiveTransactionIsolation() throws Exception {
    // Ensures that a thread launched within a transaction can read the uncommitted changes caused
    // by the transaction.  This is not a pattern that we should embrace, but is necessary for
    // DbStorage to match behavior with MemStorage.
    // TODO(wfarner): Create something like a transaction-aware Executor so that we can still
    // asynchronously react to a completed transaction, but in a way that allows for more strict
    // transaction isolation.

    ExecutorService executor = Executors.newFixedThreadPool(1,
        new ThreadFactoryBuilder().setNameFormat("AsyncRead-%d").setDaemon(true).build());
    addTearDown(() -> MoreExecutors.shutdownAndAwaitTermination(executor, 1, TimeUnit.SECONDS));

    saveTasks(TASK_A);
    storage.write(new Storage.MutateWork.NoResult<Exception>() {
      @Override
      public void execute(Storage.MutableStoreProvider storeProvider) throws Exception {
        ScheduledTask taskARunning = TaskTestUtil.addStateTransition(TASK_A, RUNNING, 1000L);
        saveTasks(taskARunning);

        Future<ScheduleStatus> asyncReadState = executor.submit(new Callable<ScheduleStatus>() {
          @Override
          public ScheduleStatus call() {
            return Iterables.getOnlyElement(fetchTasks(Query.taskScoped(Tasks.id(TASK_A))))
                .getStatus();
          }
        });
        assertEquals(RUNNING, asyncReadState.get());
      }
    });
  }

  @Test
  public void testEmptyRelations() throws Exception {
    // Test for regression of AURORA-1476.

    TaskConfig nullMetadata =
        TaskTestUtil.makeConfig(TaskTestUtil.JOB).toBuilder().setMetadata().build();

    ScheduledTask a = TaskTestUtil.makeTask("a", nullMetadata);
    ScheduledTask b = TaskTestUtil.makeTask("a", nullMetadata);
    ScheduledTask c = TaskTestUtil.makeTask("a", nullMetadata);
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
    return task.toBuilder()
        .setAssignedTask(task.getAssignedTask().toBuilder()
            .setTask(task.getAssignedTask().getTask().toBuilder()
                .setContainer(container)
                .build())
            .build())
        .build();
  }
}
