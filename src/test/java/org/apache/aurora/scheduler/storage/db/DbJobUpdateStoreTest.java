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

package org.apache.aurora.scheduler.storage.db;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.Injector;

import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateAction;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateState;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.storage.StoredJobUpdateDetails;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.Storage.Work.Quiet;
import org.apache.aurora.scheduler.storage.testing.StorageEntityUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.JobUpdateAction.INSTANCE_ROLLBACK_FAILED;
import static org.apache.aurora.gen.JobUpdateAction.INSTANCE_ROLLED_BACK;
import static org.apache.aurora.gen.JobUpdateAction.INSTANCE_ROLLING_BACK;
import static org.apache.aurora.gen.JobUpdateAction.INSTANCE_UPDATED;
import static org.apache.aurora.gen.JobUpdateAction.INSTANCE_UPDATING;
import static org.apache.aurora.gen.JobUpdateStatus.ABORTED;
import static org.apache.aurora.gen.JobUpdateStatus.ERROR;
import static org.apache.aurora.gen.JobUpdateStatus.FAILED;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLED_BACK;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_BACK;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_FORWARD;
import static org.apache.aurora.gen.JobUpdateStatus.ROLL_BACK_PAUSED;
import static org.apache.aurora.gen.JobUpdateStatus.ROLL_FORWARD_PAUSED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DbJobUpdateStoreTest {

  private static final JobKey JOB = JobKeys.from("testRole", "testEnv", "job");
  private static final JobUpdateKey UPDATE1 =
      JobUpdateKey.create(JOB, "update1");
  private static final JobUpdateKey UPDATE2 =
      JobUpdateKey.create(JobKeys.from("testRole", "testEnv", "job2"), "update2");
  private static final long CREATED_MS = 111L;
  private static final JobUpdateEvent FIRST_EVENT =
      makeJobUpdateEvent(ROLLING_FORWARD, CREATED_MS);

  private Storage storage;
  private FakeStatsProvider stats;

  @Before
  public void setUp() throws Exception {
    Injector injector = DbUtil.createStorageInjector(DbModule.testModule());
    storage = injector.getInstance(Storage.class);
    stats = injector.getInstance(FakeStatsProvider.class);
  }

  @After
  public void tearDown() throws Exception {
    truncateUpdates();
  }

  private static JobUpdate makeFullyPopulatedUpdate(JobUpdateKey key) {
    Function<Range, Range> makeFullyPopulated = r ->
        r.toBuilder()
            .setFirst(r.getFirst() == 0 ? 1 : r.getFirst())
            .setLast(r.getLast() == 0 ? 1 : r.getLast())
            .build();

    Function<Set<Range>, Iterable<Range>> fix = ranges ->
        ranges.stream().map(makeFullyPopulated).collect(Collectors.toList());

    JobUpdate jobUpdate = makeJobUpdate(key);
    JobUpdateInstructions instructions = jobUpdate.getInstructions();
    Set<InstanceTaskConfig> initialState =
        instructions.getInitialState().stream().map(itc ->
            itc.toBuilder()
                .setInstances(fix.apply(itc.getInstances()))
                .build())
            .collect(Collectors.toSet());

    InstanceTaskConfig desiredState = instructions.getDesiredState();
    JobUpdateSettings settings = instructions.getSettings();

    return jobUpdate.toBuilder()
        .setInstructions(instructions.toBuilder()
            .setInitialState(initialState)
            .setDesiredState(desiredState.toBuilder()
                .setInstances(fix.apply(desiredState.getInstances()))
                .build())
            .setSettings(settings.toBuilder()
                .setUpdateOnlyTheseInstances(fix.apply(settings.getUpdateOnlyTheseInstances()))
                .build())
            .build())
        .build();
  }

  @Test
  public void testSaveJobUpdates() {
    JobUpdateKey updateId1 = makeKey(JobKeys.from("role", "env", "name1"), "u1");
    JobUpdateKey updateId2 = makeKey(JobKeys.from("role", "env", "name2"), "u2");

    JobUpdate update1 = makeFullyPopulatedUpdate(updateId1);
    JobUpdate update2 = makeJobUpdate(updateId2);

    assertEquals(Optional.absent(), getUpdate(updateId1));
    assertEquals(Optional.absent(), getUpdate(updateId2));

    StorageEntityUtil.assertFullyPopulated(
        update1,
        JobUpdateSummary.Fields.STATE,
        Range.Fields.FIRST,
        Range.Fields.LAST);
    saveUpdate(update1, Optional.of("lock1"));
    assertUpdate(update1);

    saveUpdate(update2, Optional.absent());
    assertUpdate(update2);

    // Colliding update keys should be forbidden.
    JobUpdate update3 = makeJobUpdate(updateId2);
    try {
      saveUpdate(update3, Optional.absent());
      fail("Update ID collision should not be allowed");
    } catch (StorageException e) {
      // Expected.
    }
  }

  @Test
  public void testSaveJobUpdateWithLargeTaskConfigValues() {
    // AURORA-1494 regression test validating max resources values are allowed.
    JobUpdateKey updateId = makeKey(JobKeys.from("role", "env", "name1"), "u1");

    JobUpdate jobUpdate = makeFullyPopulatedUpdate(updateId);
    JobUpdate update = jobUpdate.toBuilder()
        .setInstructions(jobUpdate.getInstructions().toBuilder()
            .setDesiredState(jobUpdate.getInstructions().getDesiredState().toBuilder()
                .setTask(jobUpdate.getInstructions().getDesiredState().getTask().toBuilder()
                    .setNumCpus(Double.MAX_VALUE)
                    .setRamMb(Long.MAX_VALUE)
                    .setDiskMb(Long.MAX_VALUE)
                    .build())
                .build())
            .build())
        .build();

    assertEquals(Optional.absent(), getUpdate(updateId));

    StorageEntityUtil.assertFullyPopulated(
        update,
        JobUpdateSummary.Fields.STATE,
        Range.Fields.FIRST,
        Range.Fields.LAST);
    saveUpdate(update, Optional.of("lock1"));
    assertUpdate(update);
  }

  @Test
  public void testSaveEmptyInitialState() {
    JobUpdate jobUpdate = makeJobUpdate(makeKey("u1"));
    JobUpdate builder = jobUpdate.toBuilder()
        .setInstructions(jobUpdate.getInstructions().toBuilder()
            .setInitialState()
            .build())
        .build();

    // Save with null initial state instances.
    saveUpdate(builder, Optional.of("lock"));

    assertUpdate(builder);
  }

  @Test
  public void testSaveNullDesiredState() {
    JobUpdate jobUpdate = makeJobUpdate(makeKey("u1"));
    JobUpdate builder = jobUpdate.toBuilder()
        .setInstructions(jobUpdate.getInstructions().toBuilder()
            .setDesiredState(null)
            .build())
        .build();

    // Save with null desired state instances.
    saveUpdate(builder, Optional.of("lock"));

    assertUpdate(builder);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSaveBothInitialAndDesiredMissingThrows() {
    JobUpdate jobUpdate = makeJobUpdate(makeKey("u1"));
    JobUpdate builder = jobUpdate.toBuilder()
        .setInstructions(jobUpdate.getInstructions().toBuilder()
            .setInitialState()
            .setDesiredState(null)
            .build())
        .build();

    saveUpdate(builder, Optional.of("lock"));
  }

  @Test(expected = NullPointerException.class)
  public void testSaveNullInitialStateTaskThrows() {
    JobUpdate jobUpdate = makeJobUpdate(makeKey("u1"));
    JobUpdate builder = jobUpdate.toBuilder()
        .setInstructions(jobUpdate.getInstructions().toBuilder()
            .setInitialState(
                ImmutableSet.<InstanceTaskConfig>builder()
                    .addAll(jobUpdate.getInstructions().getInitialState())
                    .add(InstanceTaskConfig.create(null, ImmutableSet.of()))
                    .build())
            .build())
        .build();

    saveUpdate(builder, Optional.of("lock"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSaveEmptyInitialStateRangesThrows() {
    JobUpdate jobUpdate = makeJobUpdate(makeKey("u1"));
    JobUpdate builder = jobUpdate.toBuilder()
        .setInstructions(jobUpdate.getInstructions().toBuilder()
            .setInitialState(
                ImmutableSet.<InstanceTaskConfig>builder()
                    .addAll(jobUpdate.getInstructions().getInitialState())
                    .add(
                        InstanceTaskConfig.create(
                            TaskTestUtil.makeConfig(TaskTestUtil.JOB),
                            ImmutableSet.of()))
                    .build())
            .build())
        .build();

    saveUpdate(builder, Optional.of("lock"));
  }

  @Test(expected = NullPointerException.class)
  public void testSaveNullDesiredStateTaskThrows() {
    JobUpdate jobUpdate = makeJobUpdate(makeKey("u1"));
    JobUpdate builder = jobUpdate.toBuilder()
        .setInstructions(jobUpdate.getInstructions().toBuilder()
            .setDesiredState(jobUpdate.getInstructions().getDesiredState().toBuilder()
                .setTask(null)
                .build())
            .build())
        .build();

    saveUpdate(builder, Optional.of("lock"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSaveEmptyDesiredStateRangesThrows() {
    JobUpdate jobUpdate = makeJobUpdate(makeKey("u1"));
    JobUpdate builder = jobUpdate.toBuilder()
        .setInstructions(jobUpdate.getInstructions().toBuilder()
            .setDesiredState(jobUpdate.getInstructions().getDesiredState().toBuilder()
                .setInstances()
                .build())
            .build())
        .build();

    saveUpdate(builder, Optional.of("lock"));
  }

  @Test
  public void testSaveJobUpdateEmptyInstanceOverrides() {
    JobUpdateKey updateId = makeKey("u1");

    JobUpdate update = makeJobUpdate(updateId);
    JobUpdate expected = update.toBuilder()
        .setInstructions(update.getInstructions().toBuilder()
            .setSettings(update.getInstructions().getSettings().toBuilder()
                .setUpdateOnlyTheseInstances()
                .build())
            .build())
        .build();

    // Save with empty overrides.
    saveUpdate(expected, Optional.of("lock"));
    assertUpdate(expected);
  }

  @Test(expected = StorageException.class)
  public void testSaveJobUpdateTwiceThrows() {
    JobUpdateKey updateId = makeKey("u1");
    JobUpdate update = makeJobUpdate(updateId);

    saveUpdate(update, Optional.of("lock1"));
    saveUpdate(update, Optional.of("lock2"));
  }

  @Test
  public void testSaveJobEvents() {
    JobUpdateKey updateId = makeKey("u3");
    JobUpdate update = makeJobUpdate(updateId);
    JobUpdateEvent event1 = makeJobUpdateEvent(ROLLING_FORWARD, 124L);
    JobUpdateEvent event2 = makeJobUpdateEvent(ROLL_FORWARD_PAUSED, 125L);

    saveUpdate(update, Optional.of("lock1"));
    assertUpdate(update);
    assertEquals(ImmutableList.of(FIRST_EVENT), getUpdateDetails(updateId).get().getUpdateEvents());

    saveJobEvent(event1, updateId);
    assertEquals(
        populateExpected(update, ROLLING_FORWARD, CREATED_MS, 124L),
        getUpdateDetails(updateId).get().getUpdate());
    assertEquals(event1, getUpdateDetails(updateId).get().getUpdateEvents().get(1));

    saveJobEvent(event2, updateId);
    assertEquals(
        populateExpected(update, ROLL_FORWARD_PAUSED, CREATED_MS, 125L),
        getUpdateDetails(updateId).get().getUpdate());
    assertEquals(event1, getUpdateDetails(updateId).get().getUpdateEvents().get(1));
    assertEquals(event2, getUpdateDetails(updateId).get().getUpdateEvents().get(2));
    assertStats(ImmutableMap.of(ROLL_FORWARD_PAUSED, 1, ROLLING_FORWARD, 2));
  }

  private <T extends Number> void assertStats(Map<JobUpdateStatus, T> expected) {
    for (Map.Entry<JobUpdateStatus, T> entry : expected.entrySet()) {
      assertEquals(
          entry.getValue().longValue(),
          stats.getLongValue(DbJobUpdateStore.statName(entry.getKey())));
    }
  }

  @Test
  public void testSaveInstanceEvents() {
    JobUpdateKey updateId = makeKey("u3");
    JobUpdate update = makeJobUpdate(updateId);
    JobInstanceUpdateEvent event1 = makeJobInstanceEvent(0, 125L, INSTANCE_UPDATED);
    JobInstanceUpdateEvent event2 = makeJobInstanceEvent(1, 126L, INSTANCE_ROLLING_BACK);

    saveUpdate(update, Optional.of("lock"));
    assertUpdate(update);
    assertEquals(0, getUpdateDetails(updateId).get().getInstanceEvents().size());

    saveJobInstanceEvent(event1, updateId);
    assertEquals(
        populateExpected(update, ROLLING_FORWARD, CREATED_MS, 125L),
        getUpdateDetails(updateId).get().getUpdate());
    assertEquals(
        event1,
        Iterables.getOnlyElement(getUpdateDetails(updateId).get().getInstanceEvents()));

    saveJobInstanceEvent(event2, updateId);
    assertEquals(
        populateExpected(update, ROLLING_FORWARD, CREATED_MS, 126L),
        getUpdateDetails(updateId).get().getUpdate());
    assertEquals(event1, getUpdateDetails(updateId).get().getInstanceEvents().get(0));
    assertEquals(event2, getUpdateDetails(updateId).get().getInstanceEvents().get(1));
  }

  @Test(expected = StorageException.class)
  public void testSaveJobEventWithoutUpdateFails() {
    saveJobEvent(makeJobUpdateEvent(ROLLING_FORWARD, 123L), makeKey("u2"));
  }

  @Test(expected = StorageException.class)
  public void testSaveInstanceEventWithoutUpdateFails() {
    saveJobInstanceEvent(makeJobInstanceEvent(0, 125L, INSTANCE_UPDATED), makeKey("u1"));
  }

  @Test
  public void testSaveJobUpdateStateIgnored() {
    JobUpdateKey updateId = makeKey("u1");
    JobUpdate update = populateExpected(makeJobUpdate(updateId), ABORTED, 567L, 567L);
    saveUpdate(update, Optional.of("lock1"));

    // Assert state fields were ignored.
    assertUpdate(update);
  }

  @Test
  public void testSaveJobUpdateWithoutEventFailsSelect() {
    final JobUpdateKey updateId = makeKey("u3");
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        JobUpdate update = makeJobUpdate(updateId);
        storeProvider.getLockStore().saveLock(makeLock(update, "lock1"));
        storeProvider.getJobUpdateStore().saveJobUpdate(update, Optional.of("lock1"));
      }
    });
    assertEquals(Optional.absent(), getUpdateDetails(updateId));
  }

  @Test
  public void testMultipleJobDetails() {
    JobUpdateKey updateId1 = makeKey(JobKeys.from("role", "env", "name1"), "u1");
    JobUpdateKey updateId2 = makeKey(JobKeys.from("role", "env", "name2"), "u2");
    JobUpdateDetails details1 = makeJobDetails(makeJobUpdate(updateId1));
    JobUpdateDetails details2 = makeJobDetails(makeJobUpdate(updateId2));

    assertEquals(ImmutableList.of(), getInstanceEvents(updateId2, 3));

    saveUpdate(details1.getUpdate(), Optional.of("lock1"));
    saveUpdate(details2.getUpdate(), Optional.of("lock2"));

    details1 = updateJobDetails(populateExpected(details1.getUpdate()), FIRST_EVENT);
    details2 = updateJobDetails(populateExpected(details2.getUpdate()), FIRST_EVENT);
    assertEquals(Optional.of(details1), getUpdateDetails(updateId1));
    assertEquals(Optional.of(details2), getUpdateDetails(updateId2));

    JobUpdateEvent jEvent11 = makeJobUpdateEvent(ROLLING_FORWARD, 456L);
    JobUpdateEvent jEvent12 = makeJobUpdateEvent(ERROR, 457L);
    JobInstanceUpdateEvent iEvent11 = makeJobInstanceEvent(1, 451L, INSTANCE_UPDATED);
    JobInstanceUpdateEvent iEvent12 = makeJobInstanceEvent(2, 452L, INSTANCE_UPDATING);

    JobUpdateEvent jEvent21 = makeJobUpdateEvent(ROLL_FORWARD_PAUSED, 567L);
    JobUpdateEvent jEvent22 = makeJobUpdateEvent(ABORTED, 568L);
    JobInstanceUpdateEvent iEvent21 = makeJobInstanceEvent(3, 561L, INSTANCE_UPDATING);
    JobInstanceUpdateEvent iEvent22 = makeJobInstanceEvent(3, 562L, INSTANCE_UPDATED);

    saveJobEvent(jEvent11, updateId1);
    saveJobEvent(jEvent12, updateId1);
    saveJobInstanceEvent(iEvent11, updateId1);
    saveJobInstanceEvent(iEvent12, updateId1);

    saveJobEvent(jEvent21, updateId2);
    saveJobEvent(jEvent22, updateId2);
    assertEquals(ImmutableList.of(), getInstanceEvents(updateId2, 3));
    saveJobInstanceEvent(iEvent21, updateId2);

    assertEquals(ImmutableList.of(iEvent21), getInstanceEvents(updateId2, 3));
    saveJobInstanceEvent(iEvent22, updateId2);
    assertEquals(ImmutableList.of(iEvent21, iEvent22), getInstanceEvents(updateId2, 3));

    details1 = updateJobDetails(
        populateExpected(details1.getUpdate(), ERROR, CREATED_MS, 457L),
        ImmutableList.of(FIRST_EVENT, jEvent11, jEvent12), ImmutableList.of(iEvent11, iEvent12));

    details2 = updateJobDetails(
        populateExpected(details2.getUpdate(), ABORTED, CREATED_MS, 568L),
        ImmutableList.of(FIRST_EVENT, jEvent21, jEvent22), ImmutableList.of(iEvent21, iEvent22));

    assertEquals(Optional.of(details1), getUpdateDetails(updateId1));
    assertEquals(Optional.of(details2), getUpdateDetails(updateId2));

    assertEquals(
        ImmutableSet.of(
            StoredJobUpdateDetails.create(details1, "lock1"),
            StoredJobUpdateDetails.create(details2, "lock2")),
        getAllUpdateDetails());

    assertEquals(
        ImmutableList.of(getUpdateDetails(updateId2).get(), getUpdateDetails(updateId1).get()),
        queryDetails(JobUpdateQuery.builder().setRole("role").build()));
  }

  @Test
  public void testTruncateJobUpdates() {
    JobUpdateKey updateId = makeKey("u5");
    JobUpdate update = makeJobUpdate(updateId);
    JobInstanceUpdateEvent instanceEvent =
        JobInstanceUpdateEvent.create(0, 125L, INSTANCE_ROLLBACK_FAILED);

    saveUpdate(update, Optional.of("lock"));
    saveJobEvent(makeJobUpdateEvent(ROLLING_FORWARD, 123L), updateId);
    saveJobInstanceEvent(instanceEvent, updateId);
    assertEquals(
        populateExpected(update, ROLLING_FORWARD, CREATED_MS, 125L),
        getUpdate(updateId).get());
    assertEquals(2, getUpdateDetails(updateId).get().getUpdateEvents().size());
    assertEquals(1, getUpdateDetails(updateId).get().getInstanceEvents().size());

    truncateUpdates();
    assertEquals(Optional.absent(), getUpdateDetails(updateId));
  }

  @Test
  public void testPruneHistory() {
    JobUpdateKey updateId1 = makeKey("u11");
    JobUpdateKey updateId2 = makeKey("u12");
    JobUpdateKey updateId3 = makeKey("u13");
    JobUpdateKey updateId4 = makeKey("u14");
    JobKey job2 = JobKeys.from("testRole2", "testEnv2", "job2");
    JobUpdateKey updateId5 = makeKey(job2, "u15");
    JobUpdateKey updateId6 = makeKey(job2, "u16");
    JobUpdateKey updateId7 = makeKey(job2, "u17");

    JobUpdate update1 = makeJobUpdate(updateId1);
    JobUpdate update2 = makeJobUpdate(updateId2);
    JobUpdate update3 = makeJobUpdate(updateId3);
    JobUpdate update4 = makeJobUpdate(updateId4);
    JobUpdate update5 = makeJobUpdate(updateId5);
    JobUpdate update6 = makeJobUpdate(updateId6);
    JobUpdate update7 = makeJobUpdate(updateId7);

    JobUpdateEvent updateEvent1 = makeJobUpdateEvent(ROLLING_BACK, 123L);
    JobUpdateEvent updateEvent2 = makeJobUpdateEvent(ABORTED, 124L);
    JobUpdateEvent updateEvent3 = makeJobUpdateEvent(ROLLED_BACK, 125L);
    JobUpdateEvent updateEvent4 = makeJobUpdateEvent(FAILED, 126L);
    JobUpdateEvent updateEvent5 = makeJobUpdateEvent(ERROR, 123L);
    JobUpdateEvent updateEvent6 = makeJobUpdateEvent(FAILED, 125L);
    JobUpdateEvent updateEvent7 = makeJobUpdateEvent(ROLLING_FORWARD, 126L);

    update1 = populateExpected(
        saveUpdateNoEvent(update1, Optional.of("lock1")), ROLLING_BACK, 123L, 123L);
    update2 = populateExpected(
        saveUpdateNoEvent(update2, Optional.absent()), ABORTED, 124L, 124L);
    update3 = populateExpected(
        saveUpdateNoEvent(update3, Optional.absent()), ROLLED_BACK, 125L, 125L);
    update4 = populateExpected(
        saveUpdateNoEvent(update4, Optional.absent()), FAILED, 126L, 126L);
    update5 = populateExpected(
        saveUpdateNoEvent(update5, Optional.absent()), ERROR, 123L, 123L);
    update6 = populateExpected(
        saveUpdateNoEvent(update6, Optional.absent()), FAILED, 125L, 125L);
    update7 = populateExpected(
        saveUpdateNoEvent(update7, Optional.of("lock2")), ROLLING_FORWARD, 126L, 126L);

    saveJobEvent(updateEvent1, updateId1);
    saveJobEvent(updateEvent2, updateId2);
    saveJobEvent(updateEvent3, updateId3);
    saveJobEvent(updateEvent4, updateId4);
    saveJobEvent(updateEvent5, updateId5);
    saveJobEvent(updateEvent6, updateId6);
    saveJobEvent(updateEvent7, updateId7);

    assertEquals(update1, getUpdate(updateId1).get());
    assertEquals(update2, getUpdate(updateId2).get());
    assertEquals(update3, getUpdate(updateId3).get());
    assertEquals(update4, getUpdate(updateId4).get());
    assertEquals(update5, getUpdate(updateId5).get());
    assertEquals(update6, getUpdate(updateId6).get());
    assertEquals(update7, getUpdate(updateId7).get());

    long pruningThreshold = 120L;

    // No updates pruned.
    assertEquals(ImmutableSet.of(), pruneHistory(3, pruningThreshold));
    assertEquals(Optional.of(update7), getUpdate(updateId7)); // active update
    assertEquals(Optional.of(update6), getUpdate(updateId6));
    assertEquals(Optional.of(update5), getUpdate(updateId5));

    assertEquals(Optional.of(update4), getUpdate(updateId4));
    assertEquals(Optional.of(update3), getUpdate(updateId3));
    assertEquals(Optional.of(update2), getUpdate(updateId2));
    assertEquals(Optional.of(update1), getUpdate(updateId1)); // active update

    assertEquals(ImmutableSet.of(updateId2), pruneHistory(2, pruningThreshold));
    // No updates pruned.
    assertEquals(Optional.of(update7), getUpdate(updateId7)); // active update
    assertEquals(Optional.of(update6), getUpdate(updateId6));
    assertEquals(Optional.of(update5), getUpdate(updateId5));

    // 1 update pruned.
    assertEquals(Optional.of(update4), getUpdate(updateId4));
    assertEquals(Optional.of(update3), getUpdate(updateId3));
    assertEquals(Optional.absent(), getUpdate(updateId2));
    assertEquals(Optional.of(update1), getUpdate(updateId1)); // active update

    assertEquals(ImmutableSet.of(updateId5, updateId3), pruneHistory(1, pruningThreshold));
    // 1 update pruned.
    assertEquals(Optional.of(update7), getUpdate(updateId7)); // active update
    assertEquals(Optional.of(update6), getUpdate(updateId6));
    assertEquals(Optional.absent(), getUpdate(updateId5));

    // 2 updates pruned.
    assertEquals(Optional.of(update4), getUpdate(updateId4));
    assertEquals(Optional.absent(), getUpdate(updateId3));
    assertEquals(Optional.of(update1), getUpdate(updateId1)); // active update

    // The oldest update is pruned.
    assertEquals(ImmutableSet.of(updateId6), pruneHistory(1, 126L));
    assertEquals(Optional.of(update7), getUpdate(updateId7)); // active update
    assertEquals(Optional.absent(), getUpdate(updateId6));

    assertEquals(Optional.of(update4), getUpdate(updateId4));
    assertEquals(Optional.of(update1), getUpdate(updateId1)); // active update

    // Nothing survives the 0 per job count.
    assertEquals(ImmutableSet.of(updateId4), pruneHistory(0, pruningThreshold));
    assertEquals(Optional.of(update7), getUpdate(updateId7)); // active update

    assertEquals(Optional.absent(), getUpdate(updateId4));
    assertEquals(Optional.of(update1), getUpdate(updateId1)); // active update
  }

  @Test(expected = StorageException.class)
  public void testSaveUpdateWithoutLock() {
    final JobUpdate update = makeJobUpdate(makeKey("updateId"));
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        storeProvider.getJobUpdateStore().saveJobUpdate(update, Optional.of("lock"));
      }
    });
  }

  @Test(expected = StorageException.class)
  public void testSaveTwoUpdatesForOneJob() {
    final JobUpdate update = makeJobUpdate(makeKey("updateId"));
    saveUpdate(update, Optional.of("lock1"));
    saveUpdate(update, Optional.of("lock2"));
  }

  @Test(expected = StorageException.class)
  public void testSaveTwoUpdatesSameJobKey() {
    final JobUpdate update1 = makeJobUpdate(makeKey("update1"));
    final JobUpdate update2 = makeJobUpdate(makeKey("update2"));
    saveUpdate(update1, Optional.of("lock1"));
    saveUpdate(update2, Optional.of("lock1"));
  }

  @Test
  public void testLockCleared() {
    final JobUpdate update = makeJobUpdate(makeKey("update1"));
    saveUpdate(update, Optional.of("lock1"));

    removeLock(update, "lock1");

    assertEquals(
        Optional.of(updateJobDetails(populateExpected(update), FIRST_EVENT)),
        getUpdateDetails(makeKey("update1")));
    assertEquals(
        ImmutableSet.of(
            StoredJobUpdateDetails.create(
                updateJobDetails(populateExpected(update), FIRST_EVENT),
                null)),
        getAllUpdateDetails());

    assertEquals(
        ImmutableList.of(populateExpected(update).getSummary()),
        getSummaries(JobUpdateQuery.builder().setKey(UPDATE1).build()));

    // If the lock has been released for this job, we can start another update.
    saveUpdate(makeJobUpdate(makeKey("update2")), Optional.of("lock2"));
  }

  private static final Optional<String> NO_TOKEN = Optional.absent();

  @Test
  public void testGetLockToken() {
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        final JobUpdate update1 = makeJobUpdate(UPDATE1);
        final JobUpdate update2 = makeJobUpdate(UPDATE2);
        saveUpdate(update1, Optional.of("lock1"));
        assertEquals(
            Optional.of("lock1"),
            storeProvider.getJobUpdateStore().getLockToken(UPDATE1));
        assertEquals(NO_TOKEN, storeProvider.getJobUpdateStore().getLockToken(UPDATE2));

        saveUpdate(update2, Optional.of("lock2"));
        assertEquals(
            Optional.of("lock1"),
            storeProvider.getJobUpdateStore().getLockToken(UPDATE1));
        assertEquals(
            Optional.of("lock2"),
            storeProvider.getJobUpdateStore().getLockToken(UPDATE2));

        storeProvider.getLockStore().removeLock(makeLock(update1, "lock1").getKey());
        assertEquals(NO_TOKEN, storeProvider.getJobUpdateStore().getLockToken(UPDATE1));
        assertEquals(
            Optional.of("lock2"),
            storeProvider.getJobUpdateStore().getLockToken(UPDATE2));

        storeProvider.getLockStore().removeLock(makeLock(update2, "lock2").getKey());
        assertEquals(NO_TOKEN, storeProvider.getJobUpdateStore().getLockToken(UPDATE1));
        assertEquals(NO_TOKEN, storeProvider.getJobUpdateStore().getLockToken(UPDATE2));
      }
    });
  }

  @Test
  public void testGetSummaries() {
    String role1 = "role1";
    JobKey job1 = JobKeys.from(role1, "env", "name1");
    JobKey job2 = JobKeys.from(role1, "env", "name2");
    JobKey job3 = JobKeys.from(role1, "env", "name3");
    JobKey job4 = JobKeys.from(role1, "env", "name4");
    JobKey job5 = JobKeys.from("role", "env", "name5");
    JobUpdateSummary s1 =
        saveSummary(makeKey(job1, "u1"), 1230L, ROLLED_BACK, "user", Optional.of("lock1"));
    JobUpdateSummary s2 =
        saveSummary(makeKey(job2, "u2"), 1231L, ABORTED, "user", Optional.of("lock2"));
    JobUpdateSummary s3 =
        saveSummary(makeKey(job3, "u3"), 1239L, ERROR, "user2", Optional.of("lock3"));
    JobUpdateSummary s4 =
        saveSummary(makeKey(job4, "u4"), 1234L, ROLL_BACK_PAUSED, "user3", Optional.of("lock4"));
    JobUpdateSummary s5 =
        saveSummary(makeKey(job5, "u5"), 1235L, ROLLING_FORWARD, "user4", Optional.of("lock5"));

    // Test empty query returns all.
    assertEquals(
        ImmutableList.of(s3, s5, s4, s2, s1),
        getSummaries(JobUpdateQuery.builder().build()));

    // Test query by updateId.
    assertEquals(
        ImmutableList.of(s1),
        getSummaries(JobUpdateQuery.builder().setKey(JobUpdateKey.create(job1, "u1")).build()));

    // Test query by role.
    assertEquals(
        ImmutableList.of(s3, s4, s2, s1),
        getSummaries(JobUpdateQuery.builder().setRole(role1).build()));

    // Test query by job key.
    assertEquals(
        ImmutableList.of(s5),
        getSummaries(JobUpdateQuery.builder().setJobKey(job5).build()));

    // Test querying by update key.
    assertEquals(
        ImmutableList.of(s5),
        getSummaries(
            JobUpdateQuery.builder()
                .setKey(JobUpdateKey.create(job5, s5.getKey().getId()))
                .build()));

    // Test querying by incorrect update keys.
    assertEquals(
        ImmutableList.of(),
        getSummaries(
            JobUpdateQuery.builder()
                .setKey(JobUpdateKey.create(job5, s4.getKey().getId()))
                .build()));
    assertEquals(
        ImmutableList.of(),
        getSummaries(
            JobUpdateQuery.builder()
                .setKey(JobUpdateKey.create(job4, s5.getKey().getId()))
                .build()));

    // Test query by user.
    assertEquals(
        ImmutableList.of(s2, s1),
        getSummaries(JobUpdateQuery.builder().setUser("user").build()));

    // Test query by one status.
    assertEquals(
        ImmutableList.of(s3),
        getSummaries(JobUpdateQuery.builder().setUpdateStatuses(ERROR).build()));

    // Test query by multiple statuses.
    assertEquals(
        ImmutableList.of(s3, s2, s1),
        getSummaries(
            JobUpdateQuery.builder().setUpdateStatuses(ERROR, ABORTED, ROLLED_BACK).build()));

    // Test query by empty statuses.
    assertEquals(
        ImmutableList.of(s3, s5, s4, s2, s1),
        getSummaries(JobUpdateQuery.builder().setUpdateStatuses().build()));

    // Test paging.
    assertEquals(
        ImmutableList.of(s3, s5),
        getSummaries(JobUpdateQuery.builder().setLimit(2).setOffset(0).build()));
    assertEquals(
        ImmutableList.of(s4, s2),
        getSummaries(JobUpdateQuery.builder().setLimit(2).setOffset(2).build()));
    assertEquals(
        ImmutableList.of(s1),
        getSummaries(JobUpdateQuery.builder().setLimit(2).setOffset(4).build()));

    // Test no match.
    assertEquals(
        ImmutableList.of(),
        getSummaries(JobUpdateQuery.builder().setRole("no_match").build()));
  }

  @Test
  public void testQueryDetails() {
    JobKey jobKey1 = JobKeys.from("role1", "env", "name1");
    JobUpdateKey updateId1 = makeKey(jobKey1, "u1");
    JobKey jobKey2 = JobKeys.from("role2", "env", "name2");
    JobUpdateKey updateId2 = makeKey(jobKey2, "u2");

    JobUpdate update1 = makeJobUpdate(updateId1);
    JobUpdate update2 = makeJobUpdate(updateId2);

    assertEquals(ImmutableList.of(), getInstanceEvents(updateId2, 3));

    saveUpdate(update1, Optional.of("lock1"));
    saveUpdate(update2, Optional.of("lock2"));

    updateJobDetails(populateExpected(update1), FIRST_EVENT);
    updateJobDetails(populateExpected(update2), FIRST_EVENT);

    JobUpdateEvent jEvent11 = makeJobUpdateEvent(ROLLING_BACK, 450L);
    JobUpdateEvent jEvent12 = makeJobUpdateEvent(ROLLED_BACK, 500L);
    JobInstanceUpdateEvent iEvent11 = makeJobInstanceEvent(1, 451L, INSTANCE_ROLLING_BACK);
    JobInstanceUpdateEvent iEvent12 = makeJobInstanceEvent(2, 458L, INSTANCE_ROLLED_BACK);

    JobUpdateEvent jEvent21 = makeJobUpdateEvent(ROLL_FORWARD_PAUSED, 550L);
    JobUpdateEvent jEvent22 = makeJobUpdateEvent(ABORTED, 600L);
    JobInstanceUpdateEvent iEvent21 = makeJobInstanceEvent(3, 561L, INSTANCE_UPDATING);
    JobInstanceUpdateEvent iEvent22 = makeJobInstanceEvent(3, 570L, INSTANCE_UPDATED);

    saveJobEvent(jEvent11, updateId1);
    saveJobEvent(jEvent12, updateId1);
    saveJobInstanceEvent(iEvent11, updateId1);
    saveJobInstanceEvent(iEvent12, updateId1);

    saveJobEvent(jEvent21, updateId2);
    saveJobEvent(jEvent22, updateId2);

    saveJobInstanceEvent(iEvent21, updateId2);
    saveJobInstanceEvent(iEvent22, updateId2);

    JobUpdateDetails details1 = getUpdateDetails(updateId1).get();
    JobUpdateDetails details2 = getUpdateDetails(updateId2).get();

    // Test empty query returns all.
    assertEquals(
        ImmutableList.of(details2, details1),
        queryDetails(JobUpdateQuery.builder().build()));

    // Test query by update ID.
    assertEquals(
        ImmutableList.of(details1),
        queryDetails(JobUpdateQuery.builder().setKey(updateId1).build()));

    // Test query by role.
    assertEquals(
        ImmutableList.of(details2),
        queryDetails(JobUpdateQuery.builder().setRole(jobKey2.getRole()).build()));

    // Test query by job key.
    assertEquals(
        ImmutableList.of(details2),
        queryDetails(JobUpdateQuery.builder().setJobKey(jobKey2).build()));

    // Test query by status.
    assertEquals(
        ImmutableList.of(details2),
        queryDetails(JobUpdateQuery.builder().setUpdateStatuses(ABORTED).build()));

    // Test no match.
    assertEquals(
        ImmutableList.of(),
        queryDetails(JobUpdateQuery.builder().setRole("no match").build()));
  }

  private static JobUpdateKey makeKey(String id) {
    return makeKey(JOB, id);
  }

  private static JobUpdateKey makeKey(JobKey job, String id) {
    return JobUpdateKey.create(job, id);
  }

  private void assertUpdate(JobUpdate expected) {
    JobUpdateKey key = expected.getSummary().getKey();
    assertEquals(populateExpected(expected), getUpdate(key).get());
    assertEquals(getUpdate(key).get(), getUpdateDetails(key).get().getUpdate());
    assertEquals(getUpdateInstructions(key).get(), expected.getInstructions());
  }

  private Optional<JobUpdate> getUpdate(final JobUpdateKey key) {
    return storage.read(new Quiet<Optional<JobUpdate>>() {
      @Override
      public Optional<JobUpdate> apply(Storage.StoreProvider storeProvider) {
        return storeProvider.getJobUpdateStore().fetchJobUpdate(key);
      }
    });
  }

  private List<JobInstanceUpdateEvent> getInstanceEvents(final JobUpdateKey key, final int id) {
    return storage.read(new Quiet<List<JobInstanceUpdateEvent>>() {
      @Override
      public List<JobInstanceUpdateEvent> apply(Storage.StoreProvider storeProvider) {
        return storeProvider.getJobUpdateStore().fetchInstanceEvents(key, id);
      }
    });
  }

  private Optional<JobUpdateInstructions> getUpdateInstructions(final JobUpdateKey key) {
    return storage.read(new Quiet<Optional<JobUpdateInstructions>>() {
      @Override
      public Optional<JobUpdateInstructions> apply(Storage.StoreProvider storeProvider) {
        return storeProvider.getJobUpdateStore().fetchJobUpdateInstructions(key);
      }
    });
  }

  private Optional<JobUpdateDetails> getUpdateDetails(final JobUpdateKey key) {
    return storage.read(new Quiet<Optional<JobUpdateDetails>>() {
      @Override
      public Optional<JobUpdateDetails> apply(Storage.StoreProvider storeProvider) {
        return storeProvider.getJobUpdateStore().fetchJobUpdateDetails(key);
      }
    });
  }

  private Set<StoredJobUpdateDetails> getAllUpdateDetails() {
    return storage.read(new Quiet<Set<StoredJobUpdateDetails>>() {
      @Override
      public Set<StoredJobUpdateDetails> apply(Storage.StoreProvider storeProvider) {
        return storeProvider.getJobUpdateStore().fetchAllJobUpdateDetails();
      }
    });
  }

  private List<JobUpdateDetails> queryDetails(final JobUpdateQuery query) {
    return storage.read(new Quiet<List<JobUpdateDetails>>() {
      @Override
      public List<JobUpdateDetails> apply(Storage.StoreProvider storeProvider) {
        return storeProvider.getJobUpdateStore().fetchJobUpdateDetails(query);
      }
    });
  }

  private List<JobUpdateSummary> getSummaries(final JobUpdateQuery query) {
    return storage.read(new Quiet<List<JobUpdateSummary>>() {
      @Override
      public List<JobUpdateSummary> apply(Storage.StoreProvider storeProvider) {
        return storeProvider.getJobUpdateStore().fetchJobUpdateSummaries(query);
      }
    });
  }

  private static Lock makeLock(JobUpdate update, String lockToken) {
    return Lock.builder()
        .setKey(LockKey.job(update.getSummary().getKey().getJob()))
        .setToken(lockToken)
        .setTimestampMs(100)
        .setUser("fake user")
        .build();
  }

  private JobUpdate saveUpdate(final JobUpdate update, final Optional<String> lockToken) {
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        if (lockToken.isPresent()) {
          storeProvider.getLockStore().saveLock(makeLock(update, lockToken.get()));
        }
        storeProvider.getJobUpdateStore().saveJobUpdate(update, lockToken);
        storeProvider.getJobUpdateStore().saveJobUpdateEvent(
            update.getSummary().getKey(),
            FIRST_EVENT);
      }
    });

    return update;
  }

  private JobUpdate saveUpdateNoEvent(final JobUpdate update, final Optional<String> lockToken) {
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        if (lockToken.isPresent()) {
          storeProvider.getLockStore().saveLock(makeLock(update, lockToken.get()));
        }
        storeProvider.getJobUpdateStore().saveJobUpdate(update, lockToken);
      }
    });

    return update;
  }

  private void saveJobEvent(final JobUpdateEvent event, final JobUpdateKey key) {
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        storeProvider.getJobUpdateStore().saveJobUpdateEvent(key, event);
      }
    });
  }

  private void saveJobInstanceEvent(final JobInstanceUpdateEvent event, final JobUpdateKey key) {
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        storeProvider.getJobUpdateStore().saveJobInstanceUpdateEvent(key, event);
      }
    });
  }

  private void truncateUpdates() {
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        storeProvider.getJobUpdateStore().deleteAllUpdatesAndEvents();
      }
    });
  }

  private Set<JobUpdateKey> pruneHistory(final int retainCount, final long pruningThresholdMs) {
    return storage.write(new MutateWork.Quiet<Set<JobUpdateKey>>() {
      @Override
      public Set<JobUpdateKey> apply(MutableStoreProvider storeProvider) {
        return storeProvider.getJobUpdateStore().pruneHistory(retainCount, pruningThresholdMs);
      }
    });
  }

  private void removeLock(final JobUpdate update, final String lockToken) {
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        storeProvider.getLockStore().removeLock(makeLock(update, lockToken).getKey());
      }
    });
  }

  private JobUpdate populateExpected(JobUpdate update) {
    return populateExpected(update, ROLLING_FORWARD, CREATED_MS, CREATED_MS);
  }

  private JobUpdate populateExpected(
      JobUpdate update,
      JobUpdateStatus status,
      long createdMs,
      long lastMs) {

    return update.toBuilder()
        .setSummary(update.getSummary().toBuilder()
            .setState(
                JobUpdateState.builder()
                    .setCreatedTimestampMs(createdMs)
                    .setLastModifiedTimestampMs(lastMs)
                    .setStatus(status)
                    .build())
            .build())
        .build();
  }

  private static JobUpdateEvent makeJobUpdateEvent(JobUpdateStatus status, long timestampMs) {
    return JobUpdateEvent.builder()
        .setStatus(status)
        .setTimestampMs(timestampMs)
        .setUser("user")
        .setMessage("message")
        .build();
  }

  private JobInstanceUpdateEvent makeJobInstanceEvent(
      int instanceId,
      long timestampMs,
      JobUpdateAction action) {

    return JobInstanceUpdateEvent.create(instanceId, timestampMs, action);
  }

  private JobUpdateDetails makeJobDetails(JobUpdate update) {
    return updateJobDetails(
        update,
        ImmutableList.of(FIRST_EVENT),
        ImmutableList.of());
  }

  private JobUpdateDetails updateJobDetails(JobUpdate update, JobUpdateEvent event) {
    return updateJobDetails(
        update,
        ImmutableList.of(event),
        ImmutableList.of());
  }

  private JobUpdateDetails updateJobDetails(
      JobUpdate update,
      List<JobUpdateEvent> jobEvents,
      List<JobInstanceUpdateEvent> instanceEvents) {

    return JobUpdateDetails.builder()
        .setUpdate(update)
        .setUpdateEvents(jobEvents)
        .setInstanceEvents(instanceEvents)
        .build();
  }

  private static JobUpdateSummary makeSummary(JobUpdateKey key, String user) {
    return JobUpdateSummary.builder()
        .setKey(key)
        .setUser(user)
        .build();
  }

  private JobUpdateSummary saveSummary(
      JobUpdateKey key,
      Long modifiedTimestampMs,
      JobUpdateStatus status,
      String user,
      Optional<String> lockToken) {

    JobUpdateSummary summary = JobUpdateSummary.builder()
        .setKey(key)
        .setUser(user)
        .build();

    JobUpdate update = makeJobUpdate(summary);
    saveUpdate(update, lockToken);
    saveJobEvent(makeJobUpdateEvent(status, modifiedTimestampMs), key);
    return populateExpected(update, status, CREATED_MS, modifiedTimestampMs).getSummary();
  }

  private JobUpdate makeJobUpdate(JobUpdateSummary summary) {
    return makeJobUpdate().toBuilder().setSummary(summary).build();
  }

  private static JobUpdate makeJobUpdate(JobUpdateKey key) {
    return makeJobUpdate().toBuilder()
        .setSummary(makeSummary(key, "user"))
        .build();
  }

  private static JobUpdate makeJobUpdate() {
    return JobUpdate.builder()
        .setInstructions(makeJobUpdateInstructions())
        .build();
  }

  private static JobUpdateInstructions makeJobUpdateInstructions() {
    TaskConfig config = TaskTestUtil.makeConfig(JOB);
    return JobUpdateInstructions.builder()
        .setDesiredState(InstanceTaskConfig.builder()
            .setTask(config)
            .setInstances(Range.create(0, 7), Range.create(8, 9))
            .build())
        .setInitialState(
            InstanceTaskConfig.builder()
                .setInstances(Range.create(0, 1), Range.create(2, 3))
                .setTask(config)
                .build(),
            InstanceTaskConfig.builder()
                .setInstances(Range.create(4, 5), Range.create(6, 7))
                .setTask(config)
                .build())
        .setSettings(JobUpdateSettings.builder()
            .setBlockIfNoPulsesAfterMs(500)
            .setUpdateGroupSize(1)
            .setMaxPerInstanceFailures(1)
            .setMaxFailedInstances(1)
            .setMaxWaitToInstanceRunningMs(100)
            .setMinWaitInInstanceRunningMs(200)
            .setRollbackOnFailure(true)
            .setWaitForBatchCompletion(true)
            .setUpdateOnlyTheseInstances(Range.create(0, 0), Range.create(3, 5))
            .build())
        .build();
  }
}
