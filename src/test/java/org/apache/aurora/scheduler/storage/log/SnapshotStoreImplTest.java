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

import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.testing.FakeBuildInfo;
import org.apache.aurora.common.util.testing.FakeClock;
import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.storage.QuotaConfiguration;
import org.apache.aurora.gen.storage.SchedulerMetadata;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.gen.storage.StoredCronJob;
import org.apache.aurora.gen.storage.StoredJobUpdateDetails;
import org.apache.aurora.scheduler.ResourceAggregates;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.storage.SnapshotStore;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.common.util.testing.FakeBuildInfo.generateBuildInfo;
import static org.apache.aurora.gen.Constants.CURRENT_API_VERSION;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class SnapshotStoreImplTest extends EasyMockTest {

  private static final long NOW = 10335463456L;

  private StorageTestUtil storageUtil;
  private SnapshotStore<Snapshot> snapshotStore;

  @Before
  public void setUp() {
    FakeClock clock = new FakeClock();
    clock.setNowMillis(NOW);
    storageUtil = new StorageTestUtil(this);
    snapshotStore = new SnapshotStoreImpl(
        generateBuildInfo(),
        clock,
        storageUtil.storage);
  }

  private static JobUpdateKey makeKey(String id) {
    return JobUpdateKey.create(JobKeys.from("role", "env", "job"), id);
  }

  @Test
  public void testCreateAndRestoreNewSnapshot() {
    ImmutableSet<ScheduledTask> tasks = ImmutableSet.of(
        ScheduledTask.builder().setStatus(ScheduleStatus.PENDING).build());
    Set<QuotaConfiguration> quotas =
        ImmutableSet.of(
            QuotaConfiguration.create("steve", ResourceAggregates.EMPTY));
    HostAttributes attribute = HostAttributes.builder()
        .setHost("host")
        .setAttributes(Attribute.create("attr", ImmutableSet.of("value")))
        .setSlaveId("slave id")
        .build();
    // A legacy attribute that has a maintenance mode set, but nothing else.  These should be
    // dropped.
    HostAttributes legacyAttribute =
        HostAttributes.create("host", ImmutableSet.of());
    StoredCronJob job = StoredCronJob.create(
        JobConfiguration.builder().setKey(JobKey.create("owner", "env", "name")).build());
    String frameworkId = "framework_id";
    Lock lock = Lock.builder()
        .setKey(LockKey.job(JobKeys.from("testRole", "testEnv", "testJob")))
        .setToken("lockId")
        .setUser("testUser")
        .setTimestampMs(12345L)
        .build();
    SchedulerMetadata metadata = SchedulerMetadata.builder()
        .setFrameworkId(frameworkId)
        .setVersion(CURRENT_API_VERSION)
        .setDetails(ImmutableMap.of(
            FakeBuildInfo.DATE, FakeBuildInfo.DATE,
            FakeBuildInfo.GIT_REVISION, FakeBuildInfo.GIT_REVISION,
            FakeBuildInfo.GIT_TAG, FakeBuildInfo.GIT_TAG))
        .build();
    JobUpdateKey updateId1 =  makeKey("updateId1");
    JobUpdateKey updateId2 = makeKey("updateId2");
    JobUpdateDetails updateDetails1 = JobUpdateDetails.builder()
        .setUpdate(JobUpdate.builder()
            .setSummary(JobUpdateSummary.builder()
                .setKey(updateId1)
                .build())
            .build())
        .setUpdateEvents(JobUpdateEvent.builder().setStatus(JobUpdateStatus.ERROR).build())
        .setInstanceEvents(JobInstanceUpdateEvent.builder().setTimestampMs(123L).build())
        .build();

    JobUpdateDetails updateDetails2 = JobUpdateDetails.builder()
        .setUpdate(JobUpdate.builder()
            .setSummary(JobUpdateSummary.builder()
                .setKey(updateId2)
                .build())
            .build())
        .build();

    storageUtil.expectOperations();
    expect(storageUtil.taskStore.fetchTasks(Query.unscoped())).andReturn(tasks);
    expect(storageUtil.quotaStore.fetchQuotas())
        .andReturn(ImmutableMap.of("steve", ResourceAggregates.EMPTY));
    expect(storageUtil.attributeStore.getHostAttributes())
        .andReturn(ImmutableSet.of(attribute, legacyAttribute));
    expect(storageUtil.jobStore.fetchJobs())
        .andReturn(ImmutableSet.of(job.getJobConfiguration()));
    expect(storageUtil.schedulerStore.fetchFrameworkId()).andReturn(Optional.of(frameworkId));
    expect(storageUtil.lockStore.fetchLocks()).andReturn(ImmutableSet.of(lock));
    String lockToken = "token";
    expect(storageUtil.jobUpdateStore.fetchAllJobUpdateDetails())
        .andReturn(ImmutableSet.of(
            StoredJobUpdateDetails.create(updateDetails1, lockToken),
            StoredJobUpdateDetails.create(updateDetails2, null)));

    expectDataWipe();
    storageUtil.taskStore.saveTasks(tasks);
    storageUtil.quotaStore.saveQuota("steve", ResourceAggregates.EMPTY);
    expect(storageUtil.attributeStore.saveHostAttributes(attribute)).andReturn(true);
    storageUtil.jobStore.saveAcceptedJob(job.getJobConfiguration());
    storageUtil.schedulerStore.saveFrameworkId(frameworkId);
    storageUtil.lockStore.saveLock(lock);
    storageUtil.jobUpdateStore.saveJobUpdate(
        updateDetails1.getUpdate(), Optional.fromNullable(lockToken));
    storageUtil.jobUpdateStore.saveJobUpdateEvent(
        updateId1,
        Iterables.getOnlyElement(updateDetails1.getUpdateEvents()));
    storageUtil.jobUpdateStore.saveJobInstanceUpdateEvent(
        updateId1,
        Iterables.getOnlyElement(updateDetails1.getInstanceEvents()));

    // The saved object for update2 should be backfilled.
    JobUpdate update2Expected = updateDetails2.getUpdate().toBuilder()
        .setSummary(updateDetails2.getUpdate().getSummary().toBuilder()
            .setKey(updateId2)
            .build())
        .build();
    storageUtil.jobUpdateStore.saveJobUpdate(
        update2Expected, Optional.absent());

    control.replay();

    Snapshot expected = Snapshot.builder()
        .setTimestamp(NOW)
        .setTasks(tasks)
        .setQuotaConfigurations(quotas)
        .setHostAttributes(ImmutableSet.of(attribute, legacyAttribute))
        .setCronJobs(ImmutableSet.of(job))
        .setSchedulerMetadata(metadata)
        .setLocks(ImmutableSet.of(lock))
        .setJobUpdateDetails(ImmutableSet.of(
            StoredJobUpdateDetails.create(updateDetails1, lockToken),
            StoredJobUpdateDetails.create(updateDetails2, null)))
        .build();

    Snapshot snapshot = snapshotStore.createSnapshot();
    assertEquals(expected, snapshot);

    snapshotStore.applySnapshot(expected);
  }

  private void expectDataWipe() {
    storageUtil.taskStore.deleteAllTasks();
    storageUtil.quotaStore.deleteQuotas();
    storageUtil.attributeStore.deleteHostAttributes();
    storageUtil.jobStore.deleteJobs();
    storageUtil.lockStore.deleteLocks();
    storageUtil.jobUpdateStore.deleteAllUpdatesAndEvents();
  }
}
