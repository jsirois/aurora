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
package org.apache.aurora.scheduler.storage.log;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import org.apache.aurora.codec.ThriftBinaryCodec;
import org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateAction;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.storage.Constants;
import org.apache.aurora.gen.storage.DeduplicatedSnapshot;
import org.apache.aurora.gen.storage.LogEntry;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.PruneJobUpdateHistory;
import org.apache.aurora.gen.storage.RemoveJob;
import org.apache.aurora.gen.storage.RemoveLock;
import org.apache.aurora.gen.storage.RemoveQuota;
import org.apache.aurora.gen.storage.RemoveTasks;
import org.apache.aurora.gen.storage.RewriteTask;
import org.apache.aurora.gen.storage.SaveCronJob;
import org.apache.aurora.gen.storage.SaveFrameworkId;
import org.apache.aurora.gen.storage.SaveHostAttributes;
import org.apache.aurora.gen.storage.SaveJobInstanceUpdateEvent;
import org.apache.aurora.gen.storage.SaveJobUpdate;
import org.apache.aurora.gen.storage.SaveJobUpdateEvent;
import org.apache.aurora.gen.storage.SaveLock;
import org.apache.aurora.gen.storage.SaveQuota;
import org.apache.aurora.gen.storage.SaveTasks;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.gen.storage.Transaction;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.log.Log;
import org.apache.aurora.scheduler.log.Log.Entry;
import org.apache.aurora.scheduler.log.Log.Position;
import org.apache.aurora.scheduler.log.Log.Stream;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.SnapshotStore;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult.Quiet;
import org.apache.aurora.scheduler.storage.log.LogStorage.SchedulingService;
import org.apache.aurora.scheduler.storage.log.SnapshotDeduplicator.SnapshotDeduplicatorImpl;
import org.apache.aurora.scheduler.storage.log.testing.LogOpMatcher;
import org.apache.aurora.scheduler.storage.log.testing.LogOpMatcher.StreamMatcher;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.notNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LogStorageTest extends EasyMockTest {

  private static final Amount<Long, Time> SNAPSHOT_INTERVAL = Amount.of(1L, Time.MINUTES);
  private static final JobKey JOB_KEY = JobKeys.from("role", "env", "name");
  private static final JobUpdateKey UPDATE_ID = JobUpdateKey.create(JOB_KEY, "testUpdateId");
  private static final long NOW = 42L;

  private LogStorage logStorage;
  private Log log;
  private SnapshotDeduplicator deduplicator;
  private Stream stream;
  private Position position;
  private StreamMatcher streamMatcher;
  private SchedulingService schedulingService;
  private SnapshotStore<Snapshot> snapshotStore;
  private StorageTestUtil storageUtil;
  private EventSink eventSink;

  @Before
  public void setUp() {
    log = createMock(Log.class);
    deduplicator = createMock(SnapshotDeduplicator.class);

    StreamManagerFactory streamManagerFactory = logStream -> {
      HashFunction md5 = Hashing.md5();
      return new StreamManagerImpl(
          logStream,
          new EntrySerializer.EntrySerializerImpl(Amount.of(1, Data.GB), md5),
          md5,
          deduplicator);
    };
    LogManager logManager = new LogManager(log, streamManagerFactory);

    schedulingService = createMock(SchedulingService.class);
    snapshotStore = createMock(new Clazz<SnapshotStore<Snapshot>>() { });
    storageUtil = new StorageTestUtil(this);
    eventSink = createMock(EventSink.class);

    logStorage = new LogStorage(
        logManager,
        schedulingService,
        snapshotStore,
        SNAPSHOT_INTERVAL,
        storageUtil.storage,
        storageUtil.schedulerStore,
        storageUtil.jobStore,
        storageUtil.taskStore,
        storageUtil.lockStore,
        storageUtil.quotaStore,
        storageUtil.attributeStore,
        storageUtil.jobUpdateStore,
        eventSink,
        new ReentrantLock());

    stream = createMock(Stream.class);
    streamMatcher = LogOpMatcher.matcherFor(stream);
    position = createMock(Position.class);

    storageUtil.storage.prepare();
  }

  @Test
  public void testStart() throws Exception {
    // We should open the log and arrange for its clean shutdown.
    expect(log.open()).andReturn(stream);

    // Our start should recover the log and then forward to the underlying storage start of the
    // supplied initialization logic.
    AtomicBoolean initialized = new AtomicBoolean(false);
    MutateWork.NoResult.Quiet initializationLogic = provider -> {
      // Creating a mock and expecting apply(storeProvider) does not work here for whatever
      // reason.
      initialized.set(true);
    };

    Capture<MutateWork.NoResult.Quiet> recoverAndInitializeWork = createCapture();
    storageUtil.storage.write(capture(recoverAndInitializeWork));
    expectLastCall().andAnswer(() -> {
      recoverAndInitializeWork.getValue().apply(storageUtil.mutableStoreProvider);
      return null;
    });

    Capture<MutateWork<Void, RuntimeException>> recoveryWork = createCapture();
    expect(storageUtil.storage.write(capture(recoveryWork))).andAnswer(
        () -> {
          recoveryWork.getValue().apply(storageUtil.mutableStoreProvider);
          return null;
        });

    Capture<MutateWork<Void, RuntimeException>> initializationWork = createCapture();
    expect(storageUtil.storage.write(capture(initializationWork))).andAnswer(
        () -> {
          initializationWork.getValue().apply(storageUtil.mutableStoreProvider);
          return null;
        });

    // We should perform a snapshot when the snapshot thread runs.
    Capture<Runnable> snapshotAction = createCapture();
    schedulingService.doEvery(eq(SNAPSHOT_INTERVAL), capture(snapshotAction));
    Snapshot snapshotContents = Snapshot.builder()
        .setTimestamp(NOW)
        .setTasks(TaskTestUtil.makeTask("task_id", TaskTestUtil.JOB))
        .build();
    expect(snapshotStore.createSnapshot()).andReturn(snapshotContents);
    DeduplicatedSnapshot deduplicated =
        new SnapshotDeduplicatorImpl().deduplicate(snapshotContents);
    expect(deduplicator.deduplicate(snapshotContents)).andReturn(deduplicated);
    streamMatcher.expectSnapshot(deduplicated).andReturn(position);
    stream.truncateBefore(position);
    Capture<MutateWork<Void, RuntimeException>> snapshotWork = createCapture();
    expect(storageUtil.storage.write(capture(snapshotWork))).andAnswer(
        () -> {
          snapshotWork.getValue().apply(storageUtil.mutableStoreProvider);
          return null;
        }).anyTimes();

    // Populate all LogEntry types.
    buildReplayLogEntries();

    control.replay();

    logStorage.prepare();
    logStorage.start(initializationLogic);
    assertTrue(initialized.get());

    // Run the snapshot thread.
    snapshotAction.getValue().run();

    // Assert all LogEntry types have handlers defined.
    // Our current StreamManagerImpl.readFromBeginning() does not let some entries escape
    // the decoding routine making handling them in replay unnecessary.
    assertEquals(
        Sets.complementOf(EnumSet.of(
            LogEntry.Fields.FRAME,
            LogEntry.Fields.DEDUPLICATED_SNAPSHOT,
            LogEntry.Fields.DEFLATED_ENTRY)),
        EnumSet.copyOf(logStorage.buildLogEntryReplayActions().keySet()));

    // Assert all Transaction types have handlers defined.
    assertEquals(
        EnumSet.allOf(Op.Fields.class),
        EnumSet.copyOf(logStorage.buildTransactionReplayActions().keySet()));
  }

  private void buildReplayLogEntries() throws Exception {
    ImmutableSet.Builder<LogEntry> builder = ImmutableSet.builder();

    builder.add(createTransaction(Op.saveFrameworkId(SaveFrameworkId.create("bob"))));
    storageUtil.schedulerStore.saveFrameworkId("bob");

    SaveCronJob cronJob = SaveCronJob.create(JobConfiguration.builder().build());
    builder.add(createTransaction(Op.saveCronJob(cronJob)));
    storageUtil.jobStore.saveAcceptedJob(cronJob.getJobConfig());

    RemoveJob removeJob = RemoveJob.create(JOB_KEY);
    builder.add(createTransaction(Op.removeJob(removeJob)));
    storageUtil.jobStore.removeJob(JOB_KEY);

    SaveTasks saveTasks = SaveTasks.create(ImmutableSet.of(ScheduledTask.builder().build()));
    builder.add(createTransaction(Op.saveTasks(saveTasks)));
    storageUtil.taskStore.saveTasks(saveTasks.getTasks());

    RewriteTask rewriteTask = RewriteTask.create("id1", TaskConfig.builder().build());
    builder.add(createTransaction(Op.rewriteTask(rewriteTask)));
    expect(storageUtil.taskStore.unsafeModifyInPlace(
        rewriteTask.getTaskId(),
        rewriteTask.getTask())).andReturn(true);

    RemoveTasks removeTasks = RemoveTasks.create(ImmutableSet.of("taskId1"));
    builder.add(createTransaction(Op.removeTasks(removeTasks)));
    storageUtil.taskStore.deleteTasks(removeTasks.getTaskIds());

    SaveQuota saveQuota = SaveQuota.create(JOB_KEY.getRole(), ResourceAggregate.builder().build());
    builder.add(createTransaction(Op.saveQuota(saveQuota)));
    storageUtil.quotaStore.saveQuota(saveQuota.getRole(), saveQuota.getQuota());

    builder.add(createTransaction(Op.removeQuota(RemoveQuota.create(JOB_KEY.getRole()))));
    storageUtil.quotaStore.removeQuota(JOB_KEY.getRole());

    // This entry lacks a slave ID, and should therefore be discarded.
    SaveHostAttributes hostAttributes1 = SaveHostAttributes.create(HostAttributes.builder()
        .setHost("host1")
        .setMode(MaintenanceMode.DRAINED)
        .build());
    builder.add(createTransaction(Op.saveHostAttributes(hostAttributes1)));

    SaveHostAttributes hostAttributes2 = SaveHostAttributes.create(HostAttributes.builder()
        .setHost("host2")
        .setSlaveId("slave2")
        .setMode(MaintenanceMode.DRAINED)
        .build());
    builder.add(createTransaction(Op.saveHostAttributes(hostAttributes2)));
    expect(storageUtil.attributeStore.saveHostAttributes(
        hostAttributes2.getHostAttributes())).andReturn(true);

    SaveLock saveLock = SaveLock.create(Lock.builder().setKey(LockKey.job(JOB_KEY)).build());
    builder.add(createTransaction(Op.saveLock(saveLock)));
    storageUtil.lockStore.saveLock(saveLock.getLock());

    RemoveLock removeLock = RemoveLock.create(LockKey.job(JOB_KEY));
    builder.add(createTransaction(Op.removeLock(removeLock)));
    storageUtil.lockStore.removeLock(removeLock.getLockKey());

    JobUpdate update = JobUpdate.builder().setSummary(
        JobUpdateSummary.builder().setKey(UPDATE_ID).build()).build();
    SaveJobUpdate saveUpdate = SaveJobUpdate.create(update, "token");
    builder.add(createTransaction(Op.saveJobUpdate(saveUpdate)));
    storageUtil.jobUpdateStore.saveJobUpdate(
        saveUpdate.getJobUpdate(),
        Optional.of(saveUpdate.getLockToken()));

    SaveJobUpdateEvent saveUpdateEvent =
        SaveJobUpdateEvent.create(JobUpdateEvent.builder().build(), UPDATE_ID);
    builder.add(createTransaction(Op.saveJobUpdateEvent(saveUpdateEvent)));
    storageUtil.jobUpdateStore.saveJobUpdateEvent(UPDATE_ID, saveUpdateEvent.getEvent());

    SaveJobInstanceUpdateEvent saveInstanceEvent = SaveJobInstanceUpdateEvent.create(
        JobInstanceUpdateEvent.builder().build(),
        UPDATE_ID);
    builder.add(createTransaction(Op.saveJobInstanceUpdateEvent(saveInstanceEvent)));
    storageUtil.jobUpdateStore.saveJobInstanceUpdateEvent(UPDATE_ID, saveInstanceEvent.getEvent());

    builder.add(createTransaction(Op.pruneJobUpdateHistory(PruneJobUpdateHistory.create(5, 10L))));
    expect(storageUtil.jobUpdateStore.pruneHistory(5, 10L))
        .andReturn(ImmutableSet.of(UPDATE_ID));

    // NOOP LogEntry
    builder.add(LogEntry.noop(true));

    // Snapshot LogEntry
    Snapshot snapshot = Snapshot.builder().build();
    builder.add(LogEntry.snapshot(snapshot));
    snapshotStore.applySnapshot(snapshot);

    ImmutableSet.Builder<Entry> entryBuilder = ImmutableSet.builder();
    for (LogEntry logEntry : builder.build()) {
      Entry entry = createMock(Entry.class);
      entryBuilder.add(entry);
      expect(entry.contents()).andReturn(ThriftBinaryCodec.encodeNonNull(logEntry));
    }

    storageUtil.storage.bulkLoad(EasyMock.anyObject());
    expectLastCall().andAnswer(() -> {
      NoResult work = (NoResult<?>) EasyMock.getCurrentArguments()[0];
      work.apply(storageUtil.mutableStoreProvider);
      return null;
    });
    expect(stream.readAll()).andReturn(entryBuilder.build().iterator());
  }

  abstract class AbstractStorageFixture {
    private final AtomicBoolean runCalled = new AtomicBoolean(false);

    AbstractStorageFixture() {
      // Prevent otherwise silent noop tests that forget to call run().
      addTearDown(new TearDown() {
        @Override
        public void tearDown() {
          assertTrue(runCalled.get());
        }
      });
    }

    void run() throws Exception {
      runCalled.set(true);

      // Expect basic start operations.

      // Open the log stream.
      expect(log.open()).andReturn(stream);

      // Replay the log and perform and supplied initializationWork.
      // Simulate NOOP initialization work
      // Creating a mock and expecting apply(storeProvider) does not work here for whatever
      // reason.
      MutateWork.NoResult.Quiet initializationLogic = storeProvider -> {
        // No-op.
      };

      Capture<MutateWork.NoResult.Quiet> recoverAndInitializeWork = createCapture();
      storageUtil.storage.write(capture(recoverAndInitializeWork));
      expectLastCall().andAnswer(() -> {
        recoverAndInitializeWork.getValue().apply(storageUtil.mutableStoreProvider);
        return null;
      });

      storageUtil.storage.bulkLoad(EasyMock.anyObject());
      expectLastCall().andAnswer(() -> {
        NoResult work = (NoResult<?>) EasyMock.getCurrentArguments()[0];
        work.apply(storageUtil.mutableStoreProvider);
        return null;
      });
      expect(stream.readAll()).andReturn(Collections.emptyIterator());
      Capture<MutateWork<Void, RuntimeException>> recoveryWork = createCapture();
      expect(storageUtil.storage.write(capture(recoveryWork))).andAnswer(
          () -> {
            recoveryWork.getValue().apply(storageUtil.mutableStoreProvider);
            return null;
          });

      // Schedule snapshots.
      schedulingService.doEvery(eq(SNAPSHOT_INTERVAL), notNull(Runnable.class));

      // Setup custom test expectations.
      setupExpectations();

      control.replay();

      // Start the system.
      logStorage.prepare();
      logStorage.start(initializationLogic);

      // Exercise the system.
      runTest();
    }

    protected void setupExpectations() throws Exception {
      // Default to no expectations.
    }

    protected abstract void runTest();
  }

  abstract class AbstractMutationFixture extends AbstractStorageFixture {
    @Override
    protected void runTest() {
      logStorage.write((Quiet) AbstractMutationFixture.this::performMutations);
    }

    protected abstract void performMutations(MutableStoreProvider storeProvider);
  }

  @Test
  public void testSaveFrameworkId() throws Exception {
    String frameworkId = "bob";
    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws CodingException {
        storageUtil.expectWrite();
        storageUtil.schedulerStore.saveFrameworkId(frameworkId);
        streamMatcher.expectTransaction(Op.saveFrameworkId(SaveFrameworkId.create(frameworkId)))
            .andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getSchedulerStore().saveFrameworkId(frameworkId);
      }
    }.run();
  }

  @Test
  public void testSaveAcceptedJob() throws Exception {
    JobConfiguration jobConfig = JobConfiguration.builder().setKey(JOB_KEY).build();
    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        storageUtil.jobStore.saveAcceptedJob(jobConfig);
        streamMatcher.expectTransaction(
            Op.saveCronJob(SaveCronJob.create(jobConfig)))
            .andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getCronJobStore().saveAcceptedJob(jobConfig);
      }
    }.run();
  }

  @Test
  public void testRemoveJob() throws Exception {
    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        storageUtil.jobStore.removeJob(JOB_KEY);
        streamMatcher.expectTransaction(
            Op.removeJob(RemoveJob.create(JOB_KEY)))
            .andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getCronJobStore().removeJob(JOB_KEY);
      }
    }.run();
  }

  @Test
  public void testSaveTasks() throws Exception {
    Set<ScheduledTask> tasks = ImmutableSet.of(task("a", ScheduleStatus.INIT));
    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        storageUtil.taskStore.saveTasks(tasks);
        streamMatcher.expectTransaction(
            Op.saveTasks(SaveTasks.create(tasks)))
            .andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(tasks);
      }
    }.run();
  }

  @Test
  public void testMutateTasks() throws Exception {
    String taskId = "fred";
    Function<ScheduledTask, ScheduledTask> mutation = Functions.identity();
    Optional<ScheduledTask> mutated = Optional.of(task("a", ScheduleStatus.STARTING));
    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        expect(storageUtil.taskStore.mutateTask(taskId, mutation)).andReturn(mutated);
        streamMatcher.expectTransaction(
            Op.saveTasks(new SaveTasks(ImmutableSet.of(mutated.get().newBuilder()))))
            .andReturn(null);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        assertEquals(mutated, storeProvider.getUnsafeTaskStore().mutateTask(taskId, mutation));
      }
    }.run();
  }

  @Test
  public void testUnsafeModifyInPlace() throws Exception {
    String taskId = "wilma";
    String taskId2 = "barney";
    TaskConfig updatedConfig = task(taskId, ScheduleStatus.RUNNING).getAssignedTask().getTask();
    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        expect(storageUtil.taskStore.unsafeModifyInPlace(taskId2, updatedConfig)).andReturn(false);
        expect(storageUtil.taskStore.unsafeModifyInPlace(taskId, updatedConfig)).andReturn(true);
        streamMatcher.expectTransaction(
            Op.rewriteTask(RewriteTask.create(taskId, updatedConfig)))
            .andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().unsafeModifyInPlace(taskId2, updatedConfig);
        storeProvider.getUnsafeTaskStore().unsafeModifyInPlace(taskId, updatedConfig);
      }
    }.run();
  }

  @Test
  public void testNestedTransactions() throws Exception {
    String taskId = "fred";
    Function<ScheduledTask, ScheduledTask> mutation = Functions.identity();
    Optional<ScheduledTask> mutated = Optional.of(task("a", ScheduleStatus.STARTING));
    ImmutableSet<String> tasksToRemove = ImmutableSet.of("b");

    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        expect(storageUtil.taskStore.mutateTask(taskId, mutation)).andReturn(mutated);

        storageUtil.taskStore.deleteTasks(tasksToRemove);

        streamMatcher.expectTransaction(
            Op.saveTasks(new SaveTasks(ImmutableSet.of(mutated.get().newBuilder()))),
            Op.removeTasks(new RemoveTasks(tasksToRemove)))
            .andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        assertEquals(mutated, storeProvider.getUnsafeTaskStore().mutateTask(taskId, mutation));

        logStorage.write((NoResult.Quiet)
            innerProvider -> innerProvider.getUnsafeTaskStore().deleteTasks(tasksToRemove));
      }
    }.run();
  }

  @Test
  public void testSaveAndMutateTasks() throws Exception {
    String taskId = "fred";
    Function<ScheduledTask, ScheduledTask> mutation = Functions.identity();
    Set<ScheduledTask> saved = ImmutableSet.of(task("a", ScheduleStatus.INIT));
    Optional<ScheduledTask> mutated = Optional.of(task("a", ScheduleStatus.PENDING));

    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        storageUtil.taskStore.saveTasks(saved);

        // Nested transaction with result.
        expect(storageUtil.taskStore.mutateTask(taskId, mutation)).andReturn(mutated);

        // Resulting stream operation.
        streamMatcher.expectTransaction(Op.saveTasks(
            new SaveTasks(ImmutableSet.of(mutated.get().newBuilder()))))
            .andReturn(null);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(saved);
        assertEquals(mutated, storeProvider.getUnsafeTaskStore().mutateTask(taskId, mutation));
      }
    }.run();
  }

  @Test
  public void testSaveAndMutateTasksNoCoalesceUniqueIds() throws Exception {
    String taskId = "fred";
    Function<ScheduledTask, ScheduledTask> mutation = Functions.identity();
    Set<ScheduledTask> saved = ImmutableSet.of(task("b", ScheduleStatus.INIT));
    Optional<ScheduledTask> mutated = Optional.of(task("a", ScheduleStatus.PENDING));

    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        storageUtil.taskStore.saveTasks(saved);

        // Nested transaction with result.
        expect(storageUtil.taskStore.mutateTask(taskId, mutation)).andReturn(mutated);

        // Resulting stream operation.
        streamMatcher.expectTransaction(
            Op.saveTasks(SaveTasks.create(
                ImmutableSet.<ScheduledTask>builder()
                    .addAll(ScheduledTask.toBuildersList(saved))
                    .add(mutated.get().newBuilder())
                    .build())))
            .andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(saved);
        assertEquals(mutated, storeProvider.getUnsafeTaskStore().mutateTask(taskId, mutation));
      }
    }.run();
  }

  @Test
  public void testRemoveTasksQuery() throws Exception {
    ScheduledTask task = task("a", ScheduleStatus.FINISHED);
    Set<String> taskIds = Tasks.ids(task);
    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        storageUtil.taskStore.deleteTasks(taskIds);
        streamMatcher.expectTransaction(Op.removeTasks(RemoveTasks.create(taskIds)))
            .andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().deleteTasks(taskIds);
      }
    }.run();
  }

  @Test
  public void testRemoveTasksIds() throws Exception {
    Set<String> taskIds = ImmutableSet.of("42");
    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        storageUtil.taskStore.deleteTasks(taskIds);
        streamMatcher.expectTransaction(Op.removeTasks(RemoveTasks.create(taskIds)))
            .andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().deleteTasks(taskIds);
      }
    }.run();
  }

  @Test
  public void testSaveQuota() throws Exception {
    String role = "role";
    ResourceAggregate quota = ResourceAggregate.create(1.0, 128L, 1024L);

    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        storageUtil.quotaStore.saveQuota(role, quota);
        streamMatcher.expectTransaction(Op.saveQuota(SaveQuota.create(role, quota)))
            .andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getQuotaStore().saveQuota(role, quota);
      }
    }.run();
  }

  @Test
  public void testRemoveQuota() throws Exception {
    String role = "role";
    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        storageUtil.quotaStore.removeQuota(role);
        streamMatcher.expectTransaction(Op.removeQuota(RemoveQuota.create(role)))
            .andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getQuotaStore().removeQuota(role);
      }
    }.run();
  }

  @Test
  public void testSaveLock() throws Exception {
    Lock lock = Lock.builder()
        .setKey(LockKey.job(JOB_KEY))
        .setToken("testLockId")
        .setUser("testUser")
        .setTimestampMs(12345L)
        .build();
    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        storageUtil.lockStore.saveLock(lock);
        streamMatcher.expectTransaction(Op.saveLock(SaveLock.create(lock)))
            .andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getLockStore().saveLock(lock);
      }
    }.run();
  }

  @Test
  public void testRemoveLock() throws Exception {
    LockKey lockKey = LockKey.job(JOB_KEY);
    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        storageUtil.lockStore.removeLock(lockKey);
        streamMatcher.expectTransaction(Op.removeLock(RemoveLock.create(lockKey)))
            .andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getLockStore().removeLock(lockKey);
      }
    }.run();
  }

  @Test
  public void testSaveHostAttributes() throws Exception {
    String host = "hostname";
    Set<Attribute> attributes = ImmutableSet.of(Attribute.create("attr", ImmutableSet.of("value")));
    Optional<HostAttributes> hostAttributes = Optional.of(HostAttributes.create(host, attributes));

    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        expect(storageUtil.attributeStore.getHostAttributes(host))
            .andReturn(Optional.absent());

        expect(storageUtil.attributeStore.getHostAttributes(host)).andReturn(hostAttributes);

        expect(storageUtil.attributeStore.saveHostAttributes(hostAttributes.get())).andReturn(true);
        eventSink.post(new PubsubEvent.HostAttributesChanged(hostAttributes.get()));
        streamMatcher.expectTransaction(
            Op.saveHostAttributes(SaveHostAttributes.create(hostAttributes.get())))
            .andReturn(position);

        expect(storageUtil.attributeStore.saveHostAttributes(hostAttributes.get()))
            .andReturn(false);

        expect(storageUtil.attributeStore.getHostAttributes(host)).andReturn(hostAttributes);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        AttributeStore.Mutable store = storeProvider.getAttributeStore();
        assertEquals(Optional.absent(), store.getHostAttributes(host));

        assertTrue(store.saveHostAttributes(hostAttributes.get()));

        assertEquals(hostAttributes, store.getHostAttributes(host));

        assertFalse(store.saveHostAttributes(hostAttributes.get()));

        assertEquals(hostAttributes, store.getHostAttributes(host));
      }
    }.run();
  }

  @Test
  public void testSaveUpdateWithLockToken() throws Exception {
    saveAndAssertJobUpdate(Optional.of("token"));
  }

  @Test
  public void testSaveUpdateWithNullLockToken() throws Exception {
    saveAndAssertJobUpdate(Optional.absent());
  }

  private void saveAndAssertJobUpdate(Optional<String> lockToken) throws Exception {
    JobUpdate update = JobUpdate.builder()
        .setSummary(JobUpdateSummary.builder()
            .setKey(UPDATE_ID)
            .setUser("user")
            .build())
        .setInstructions(JobUpdateInstructions.builder()
            .setDesiredState(InstanceTaskConfig.builder()
                .setTask(TaskConfig.builder().build())
                .setInstances(Range.create(0, 3))
                .build())
            .setInitialState(InstanceTaskConfig.builder()
                .setTask(TaskConfig.builder().build())
                .setInstances(Range.create(0, 3))
                .build())
            .setSettings(JobUpdateSettings.builder().build())
            .build())
        .build();

    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        storageUtil.jobUpdateStore.saveJobUpdate(update, lockToken);
        streamMatcher.expectTransaction(
            Op.saveJobUpdate(SaveJobUpdate.create(update, lockToken.orNull())))
            .andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getJobUpdateStore().saveJobUpdate(update, lockToken);
      }
    }.run();
  }

  @Test
  public void testSaveJobUpdateEvent() throws Exception {
    JobUpdateEvent event = JobUpdateEvent.builder()
        .setStatus(JobUpdateStatus.ROLLING_BACK)
        .setTimestampMs(12345L)
        .build();

    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        storageUtil.jobUpdateStore.saveJobUpdateEvent(UPDATE_ID, event);
        streamMatcher.expectTransaction(Op.saveJobUpdateEvent(SaveJobUpdateEvent.create(
            event,
            UPDATE_ID))).andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getJobUpdateStore().saveJobUpdateEvent(UPDATE_ID, event);
      }
    }.run();
  }

  @Test
  public void testSaveJobInstanceUpdateEvent() throws Exception {
    JobInstanceUpdateEvent event = JobInstanceUpdateEvent.builder()
        .setAction(JobUpdateAction.INSTANCE_ROLLING_BACK)
        .setTimestampMs(12345L)
        .setInstanceId(0)
        .build();

    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        storageUtil.jobUpdateStore.saveJobInstanceUpdateEvent(UPDATE_ID, event);
        streamMatcher.expectTransaction(Op.saveJobInstanceUpdateEvent(
            SaveJobInstanceUpdateEvent.create(event, UPDATE_ID)))
            .andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getJobUpdateStore().saveJobInstanceUpdateEvent(UPDATE_ID, event);
      }
    }.run();
  }

  @Test
  public void testPruneHistory() throws Exception {
    PruneJobUpdateHistory pruneHistory = PruneJobUpdateHistory.builder()
        .setHistoryPruneThresholdMs(1L)
        .setPerJobRetainCount(1)
        .build();

    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        expect(storageUtil.jobUpdateStore.pruneHistory(
            pruneHistory.getPerJobRetainCount(),
            pruneHistory.getHistoryPruneThresholdMs()))
            .andReturn(ImmutableSet.of(UPDATE_ID));

        streamMatcher.expectTransaction(Op.pruneJobUpdateHistory(pruneHistory)).andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getJobUpdateStore().pruneHistory(
            pruneHistory.getPerJobRetainCount(),
            pruneHistory.getHistoryPruneThresholdMs());
      }
    }.run();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testBulkLoad() throws Exception {
    expect(log.open()).andReturn(stream);
    MutateWork.NoResult.Quiet load = createMock(new Clazz<NoResult.Quiet>() { });

    control.replay();

    logStorage.prepare();
    logStorage.bulkLoad(load);
  }

  private LogEntry createTransaction(Op... ops) {
    return LogEntry.transaction(
        Transaction.create(ImmutableList.copyOf(ops), Constants.CURRENT_SCHEMA_VERSION));
  }

  private static ScheduledTask task(String id, ScheduleStatus status) {
    return ScheduledTask.builder()
        .setStatus(status)
        .setAssignedTask(AssignedTask.builder()
            .setTaskId(id)
            .setTask(TaskConfig.builder().build())
            .build())
        .build();
  }
}
