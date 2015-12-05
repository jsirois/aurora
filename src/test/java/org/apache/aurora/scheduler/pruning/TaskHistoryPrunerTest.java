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
package org.apache.aurora.scheduler.pruning;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;

import org.apache.aurora.common.base.Command;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.testing.FakeClock;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.async.AsyncModule;
import org.apache.aurora.scheduler.async.AsyncModule.AsyncExecutor;
import org.apache.aurora.scheduler.async.DelayExecutor;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.pruning.TaskHistoryPruner.HistoryPrunnerSettings;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.ASSIGNED;
import static org.apache.aurora.gen.ScheduleStatus.FINISHED;
import static org.apache.aurora.gen.ScheduleStatus.KILLED;
import static org.apache.aurora.gen.ScheduleStatus.LOST;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.gen.ScheduleStatus.STARTING;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.fail;

public class TaskHistoryPrunerTest extends EasyMockTest {
  private static final String JOB_A = "job-a";
  private static final String TASK_ID = "task_id";
  private static final String SLAVE_HOST = "HOST_A";
  private static final Amount<Long, Time> ONE_MS = Amount.of(1L, Time.MILLISECONDS);
  private static final Amount<Long, Time> ONE_MINUTE = Amount.of(1L, Time.MINUTES);
  private static final Amount<Long, Time> ONE_DAY = Amount.of(1L, Time.DAYS);
  private static final Amount<Long, Time> ONE_HOUR = Amount.of(1L, Time.HOURS);
  private static final int PER_JOB_HISTORY = 2;

  private DelayExecutor executor;
  private FakeClock clock;
  private StateManager stateManager;
  private StorageTestUtil storageUtil;
  private TaskHistoryPruner pruner;
  private Closer closer;

  @Before
  public void setUp() {
    executor = createMock(DelayExecutor.class);
    clock = new FakeClock();
    stateManager = createMock(StateManager.class);
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    pruner = new TaskHistoryPruner(
        executor,
        stateManager,
        clock,
        new HistoryPrunnerSettings(ONE_DAY, ONE_MINUTE, PER_JOB_HISTORY),
        storageUtil.storage);
    closer = Closer.create();
  }

  @After
  public void tearDownCloser() throws Exception {
    closer.close();
  }

  @Test
  public void testNoPruning() {
    long taskATimestamp = clock.nowMillis();
    ScheduledTask a = makeTask("a", FINISHED);

    clock.advance(ONE_MS);
    long taskBTimestamp = clock.nowMillis();
    ScheduledTask b = makeTask("b", LOST);

    expectNoImmediatePrune(ImmutableSet.of(a));
    expectOneDelayedPrune(taskATimestamp);
    expectNoImmediatePrune(ImmutableSet.of(a, b));
    expectOneDelayedPrune(taskBTimestamp);

    control.replay();

    pruner.recordStateChange(TaskStateChange.initialized(a));
    pruner.recordStateChange(TaskStateChange.initialized(b));
  }

  @Test
  public void testStorageStartedWithPruning() {
    long taskATimestamp = clock.nowMillis();
    ScheduledTask a = makeTask("a", FINISHED);

    clock.advance(ONE_MINUTE);
    long taskBTimestamp = clock.nowMillis();
    ScheduledTask b = makeTask("b", LOST);

    clock.advance(ONE_MINUTE);
    long taskCTimestamp = clock.nowMillis();
    ScheduledTask c = makeTask("c", FINISHED);

    clock.advance(ONE_MINUTE);
    ScheduledTask d = makeTask("d", FINISHED);
    ScheduledTask e = makeTask("job-x", "e", FINISHED);

    expectNoImmediatePrune(ImmutableSet.of(a));
    expectOneDelayedPrune(taskATimestamp);
    expectNoImmediatePrune(ImmutableSet.of(a, b));
    expectOneDelayedPrune(taskBTimestamp);
    expectImmediatePrune(ImmutableSet.of(a, b, c), a);
    expectOneDelayedPrune(taskCTimestamp);
    expectImmediatePrune(ImmutableSet.of(b, c, d), b);
    expectDefaultDelayedPrune();
    expectNoImmediatePrune(ImmutableSet.of(e));
    expectDefaultDelayedPrune();

    control.replay();

    for (ScheduledTask task : ImmutableList.of(a, b, c, d, e)) {
      pruner.recordStateChange(TaskStateChange.initialized(task));
    }
  }

  @Test
  public void testStateChange() {
    ScheduledTask starting = makeTask("a", STARTING);
    ScheduledTask running = copy(starting, RUNNING);
    ScheduledTask killed = copy(starting, KILLED);

    expectNoImmediatePrune(ImmutableSet.of(killed));
    expectDefaultDelayedPrune();

    control.replay();

    // No future set for non-terminal state transition.
    changeState(starting, running);

    // Future set for terminal state transition.
    changeState(running, killed);
  }

  @Test
  public void testActivateFutureAndExceedHistoryGoal() {
    ScheduledTask running = makeTask("a", RUNNING);
    ScheduledTask killed = copy(running, KILLED);
    expectNoImmediatePrune(ImmutableSet.of(running));
    Capture<Runnable> delayedDelete = expectDefaultDelayedPrune();

    // Expect task "a" to be pruned when future is activated.
    expectDeleteTasks("a");

    control.replay();

    // Capture future for inactive task "a"
    changeState(running, killed);
    clock.advance(ONE_HOUR);
    // Execute future to prune task "a" from the system.
    delayedDelete.getValue().run();
  }

  @Test
  public void testSuppressEmptyDelete() {
    ScheduledTask running = makeTask("a", RUNNING);
    ScheduledTask killed = copy(running, KILLED);
    expectImmediatePrune(
        ImmutableSet.of(makeTask("b", KILLED), makeTask("c", KILLED), makeTask("d", KILLED)));
    expectDefaultDelayedPrune();

    control.replay();

    changeState(running, killed);
  }

  @Test
  public void testJobHistoryExceeded() {
    ScheduledTask a = makeTask("a", RUNNING);
    clock.advance(ONE_MS);
    ScheduledTask aKilled = copy(a, KILLED);

    ScheduledTask b = makeTask("b", RUNNING);
    clock.advance(ONE_MS);
    ScheduledTask bKilled = copy(b, KILLED);

    ScheduledTask c = makeTask("c", RUNNING);
    clock.advance(ONE_MS);
    ScheduledTask cLost = copy(c, LOST);

    ScheduledTask d = makeTask("d", RUNNING);
    clock.advance(ONE_MS);
    ScheduledTask dLost = copy(d, LOST);

    expectNoImmediatePrune(ImmutableSet.of(a));
    expectDefaultDelayedPrune();
    expectNoImmediatePrune(ImmutableSet.of(a, b));
    expectDefaultDelayedPrune();
    expectNoImmediatePrune(ImmutableSet.of(a, b)); // no pruning yet due to min threshold
    expectDefaultDelayedPrune();
    clock.advance(ONE_HOUR);
    expectImmediatePrune(ImmutableSet.of(a, b, c, d), a, b); // now prune 2 tasks
    expectDefaultDelayedPrune();

    control.replay();

    changeState(a, aKilled);
    changeState(b, bKilled);
    changeState(c, cLost);
    changeState(d, dLost);
  }

  // TODO(William Farner): Consider removing the thread safety tests.  Now that intrinsic locks
  // are not used, it is rather awkward to test this.
  @Test
  public void testThreadSafeStateChangeEvent() throws Exception {
    // This tests against regression where an executor pruning a task holds an intrinsic lock and
    // an unrelated task state change in the scheduler fires an event that requires this intrinsic
    // lock. This causes a deadlock when the executor tries to acquire a lock held by the event
    // fired.

    ScheduledThreadPoolExecutor realExecutor = new ScheduledThreadPoolExecutor(1,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("testThreadSafeEvents-executor")
            .build());
    closer.register(new Closeable() {
      @Override
      public void close() throws IOException {
        MoreExecutors.shutdownAndAwaitTermination(realExecutor, 1L, TimeUnit.SECONDS);
      }
    });

    Injector injector = Guice.createInjector(
        new AsyncModule(realExecutor),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(StatsProvider.class).toInstance(new FakeStatsProvider());
          }
        });
    executor = injector.getInstance(Key.get(DelayExecutor.class, AsyncExecutor.class));

    pruner = buildPruner(executor);
    // The goal is to verify that the call does not deadlock. We do not care about the outcome.
    Command onDeleted = () -> changeState(makeTask("b", ASSIGNED), STARTING);
    CountDownLatch taskDeleted = expectTaskDeleted(onDeleted, TASK_ID);

    control.replay();

    // Change the task to a terminal state and wait for it to be pruned.
    changeState(makeTask(TASK_ID, RUNNING), KILLED);
    taskDeleted.await();
  }

  private TaskHistoryPruner buildPruner(DelayExecutor delayExecutor) {
    return new TaskHistoryPruner(
        delayExecutor,
        stateManager,
        clock,
        new HistoryPrunnerSettings(Amount.of(1L, Time.MILLISECONDS), ONE_MS, PER_JOB_HISTORY),
        storageUtil.storage);
  }

  private CountDownLatch expectTaskDeleted(final Command onDelete, String taskId) {
    final CountDownLatch deleteCalled = new CountDownLatch(1);
    final CountDownLatch eventDelivered = new CountDownLatch(1);

    Thread eventDispatch = new Thread() {
      @Override
      public void run() {
        try {
          deleteCalled.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          fail("Interrupted while awaiting for delete call.");
          return;
        }
        onDelete.execute();
        eventDelivered.countDown();
      }
    };
    eventDispatch.setDaemon(true);
    eventDispatch.setName(getClass().getName() + "-EventDispatch");
    eventDispatch.start();

    stateManager.deleteTasks(storageUtil.mutableStoreProvider, ImmutableSet.of(taskId));
    expectLastCall().andAnswer(new IAnswer<Void>() {
      @Override
      public Void answer() {
        deleteCalled.countDown();
        try {
          eventDelivered.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          fail("Interrupted while awaiting for event delivery.");
        }
        return null;
      }
    });

    return eventDelivered;
  }

  private void expectDeleteTasks(String... tasks) {
    stateManager.deleteTasks(storageUtil.mutableStoreProvider, ImmutableSet.copyOf(tasks));
  }

  private Capture<Runnable> expectDefaultDelayedPrune() {
    return expectDelayedPrune(ONE_DAY.as(Time.MILLISECONDS));
  }

  private Capture<Runnable> expectOneDelayedPrune(long timestampMillis) {
    return expectDelayedPrune(timestampMillis);
  }

  private void expectNoImmediatePrune(ImmutableSet<ScheduledTask> tasksInJob) {
    expectImmediatePrune(tasksInJob);
  }

  private void expectImmediatePrune(
      ImmutableSet<ScheduledTask> tasksInJob,
      ScheduledTask... pruned) {

    // Expect a deferred prune operation when a new task is being watched.
    executor.execute(EasyMock.<Runnable>anyObject());
    expectLastCall().andAnswer(
        new IAnswer<Future<?>>() {
          @Override
          public Future<?> answer() {
            Runnable work = (Runnable) EasyMock.getCurrentArguments()[0];
            work.run();
            return null;
          }
        }
    );

    JobKey jobKey = Iterables.getOnlyElement(
        FluentIterable.from(tasksInJob).transform(Tasks::getJob).toSet());
    storageUtil.expectTaskFetch(TaskHistoryPruner.jobHistoryQuery(jobKey), tasksInJob);
    if (pruned.length > 0) {
      stateManager.deleteTasks(storageUtil.mutableStoreProvider, Tasks.ids(pruned));
    }
  }

  private Capture<Runnable> expectDelayedPrune(long timestampMillis) {
    Capture<Runnable> capture = createCapture();
    executor.execute(
        EasyMock.capture(capture),
        eq(Amount.of(pruner.calculateTimeout(timestampMillis), Time.MILLISECONDS)));
    return capture;
  }

  private void changeState(ScheduledTask oldStateTask, ScheduledTask newStateTask) {
    pruner.recordStateChange(TaskStateChange.transition(newStateTask, oldStateTask.getStatus()));
  }

  private void changeState(ScheduledTask oldStateTask, ScheduleStatus status) {
    pruner.recordStateChange(
        TaskStateChange.transition(copy(oldStateTask, status), oldStateTask.getStatus()));
  }

  private ScheduledTask copy(ScheduledTask task, ScheduleStatus status) {
    return task.toBuilder().setStatus(status).build();
  }

  private ScheduledTask makeTask(
      String job,
      String taskId,
      ScheduleStatus status) {

    return ScheduledTask.builder()
        .setStatus(status)
        .setTaskEvents(ImmutableList.of(TaskEvent.create(clock.nowMillis(), status)))
        .setAssignedTask(makeAssignedTask(job, taskId))
        .build();
  }

  private ScheduledTask makeTask(String taskId, ScheduleStatus status) {
    return makeTask(JOB_A, taskId, status);
  }

  private AssignedTask makeAssignedTask(String job, String taskId) {
    return AssignedTask.builder()
        .setSlaveHost(SLAVE_HOST)
        .setTaskId(taskId)
        .setTask(TaskConfig.builder()
            .setJob(JobKey.create("role", "staging45", job))
            .setOwner(Identity.create("role", "user"))
            .setEnvironment("staging45")
            .setJobName(job)
            .setExecutorConfig(ExecutorConfig.create("aurora", "config"))
            .build())
        .build();
  }
}
