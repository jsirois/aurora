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

import java.util.Arrays;
import java.util.Map;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.common.util.BuildInfo;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.storage.QuotaConfiguration;
import org.apache.aurora.gen.storage.SchedulerMetadata;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.gen.storage.StoredCronJob;
import org.apache.aurora.gen.storage.StoredJobUpdateDetails;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.SnapshotStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.Storage.Volatile;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.gen.Constants.CURRENT_API_VERSION;

/**
 * Snapshot store implementation that delegates to underlying snapshot stores by
 * extracting/applying fields in a snapshot thrift struct.
 */
public class SnapshotStoreImpl implements SnapshotStore<Snapshot> {

  private static final Logger LOG = Logger.getLogger(SnapshotStoreImpl.class.getName());

  private static final Iterable<SnapshotField> SNAPSHOT_FIELDS = Arrays.asList(
      new SnapshotField() {
        // It's important for locks to be replayed first, since there are relations that expect
        // references to be valid on insertion.
        @Override
        public void saveToSnapshot(StoreProvider store, Snapshot.Builder snapshot) {
          snapshot.setLocks(store.getLockStore().fetchLocks());
        }

        @Override
        public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          store.getLockStore().deleteLocks();

          for (Lock lock : snapshot.getLocks()) {
            store.getLockStore().saveLock(lock);
          }
        }
      },
      new SnapshotField() {
        @Override
        public void saveToSnapshot(StoreProvider storeProvider, Snapshot.Builder snapshot) {
          snapshot.setHostAttributes(
              storeProvider.getAttributeStore().getHostAttributes());
        }

        @Override
        public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          store.getAttributeStore().deleteHostAttributes();

          for (HostAttributes attributes : snapshot.getHostAttributes()) {
            // Prior to commit 5cf760b, the store would persist maintenance mode changes for
            // unknown hosts.  5cf760b began rejecting these, but the replicated log may still
            // contain entries with a null slave ID.
            if (attributes.isSetSlaveId()) {
              store.getAttributeStore().saveHostAttributes(attributes);
            } else {
              LOG.info("Dropping host attributes with no slave ID: " + attributes);
            }
          }
        }
      },
      new SnapshotField() {
        @Override
        public void saveToSnapshot(StoreProvider store, Snapshot.Builder snapshot) {
          snapshot.setTasks(
              store.getTaskStore().fetchTasks(Query.unscoped()));
        }

        @Override
        public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          store.getUnsafeTaskStore().deleteAllTasks();

          if (!snapshot.getTasks().isEmpty()) {
            store.getUnsafeTaskStore().saveTasks(
                snapshot.getTasks());
          }
        }
      },
      new SnapshotField() {
        @Override
        public void saveToSnapshot(StoreProvider store, Snapshot.Builder snapshot) {
          ImmutableSet.Builder<StoredCronJob> jobs = ImmutableSet.builder();

          for (JobConfiguration config : store.getCronJobStore().fetchJobs()) {
            jobs.add(StoredCronJob.create(config));
          }
          snapshot.setCronJobs(jobs.build());
        }

        @Override
        public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          store.getCronJobStore().deleteJobs();

          for (StoredCronJob job : snapshot.getCronJobs()) {
            store.getCronJobStore().saveAcceptedJob(
                job.getJobConfiguration());
          }
        }
      },
      new SnapshotField() {
        @Override
        public void saveToSnapshot(StoreProvider store, Snapshot.Builder snapshot) {
          // SchedulerMetadata is updated outside of the static list of SnapshotFields
        }

        @Override
        public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          if (snapshot.isSetSchedulerMetadata()
              && snapshot.getSchedulerMetadata().isSetFrameworkId()) {
            // No delete necessary here since this is a single value.

            store.getSchedulerStore()
                .saveFrameworkId(snapshot.getSchedulerMetadata().getFrameworkId());
          }
        }
      },
      new SnapshotField() {
        @Override
        public void saveToSnapshot(StoreProvider store, Snapshot.Builder snapshot) {
          ImmutableSet.Builder<QuotaConfiguration> quotas = ImmutableSet.builder();
          for (Map.Entry<String, ResourceAggregate> entry
              : store.getQuotaStore().fetchQuotas().entrySet()) {

            quotas.add(QuotaConfiguration.create(entry.getKey(), entry.getValue()));
          }

          snapshot.setQuotaConfigurations(quotas.build());
        }

        @Override
        public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          store.getQuotaStore().deleteQuotas();

          for (QuotaConfiguration quota : snapshot.getQuotaConfigurations()) {
            store.getQuotaStore()
                .saveQuota(quota.getRole(), quota.getQuota());
          }
        }
      },
      new SnapshotField() {
        @Override
        public void saveToSnapshot(StoreProvider store, Snapshot.Builder snapshot) {
          snapshot.setJobUpdateDetails(store.getJobUpdateStore().fetchAllJobUpdateDetails());
        }

        @Override
        public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          JobUpdateStore.Mutable updateStore = store.getJobUpdateStore();
          updateStore.deleteAllUpdatesAndEvents();

          for (StoredJobUpdateDetails storedDetails : snapshot.getJobUpdateDetails()) {
            JobUpdateDetails details = storedDetails.getDetails();
            updateStore.saveJobUpdate(
                details.getUpdate(),
                Optional.fromNullable(storedDetails.getLockToken()));

            for (JobUpdateEvent updateEvent : details.getUpdateEvents()) {
              updateStore.saveJobUpdateEvent(
                  details.getUpdate().getSummary().getKey(),
                  updateEvent);
            }

            for (JobInstanceUpdateEvent instanceEvent : details.getInstanceEvents()) {
              updateStore.saveJobInstanceUpdateEvent(
                  details.getUpdate().getSummary().getKey(),
                  instanceEvent);
            }
          }
        }
      }
  );

  private final BuildInfo buildInfo;
  private final Clock clock;
  private final Storage storage;

  @Inject
  public SnapshotStoreImpl(BuildInfo buildInfo, Clock clock, @Volatile Storage storage) {
    this.buildInfo = requireNonNull(buildInfo);
    this.clock = requireNonNull(clock);
    this.storage = requireNonNull(storage);
  }

  @Timed("snapshot_create")
  @Override
  public Snapshot createSnapshot() {
    // It's important to perform snapshot creation in a write lock to ensure all upstream callers
    // are correctly synchronized (e.g. during backup creation).
    return storage.write(new MutateWork.Quiet<Snapshot>() {
      @Override
      public Snapshot apply(MutableStoreProvider storeProvider) {
        Snapshot.Builder snapshot = Snapshot.builder();

        // Capture timestamp to signify the beginning of a snapshot operation, apply after in case
        // one of the field closures is mean and tries to apply a timestamp.
        long timestamp = clock.nowMillis();
        for (SnapshotField field : SNAPSHOT_FIELDS) {
          field.saveToSnapshot(storeProvider, snapshot);
        }

        SchedulerMetadata metadata = SchedulerMetadata.builder()
            .setFrameworkId(storeProvider.getSchedulerStore().fetchFrameworkId().orNull())
            .setVersion(CURRENT_API_VERSION)
            .setDetails(buildInfo.getProperties())
            .build();

        snapshot.setSchedulerMetadata(metadata);
        snapshot.setTimestamp(timestamp);
        return snapshot.build();
      }
    });
  }

  @Timed("snapshot_apply")
  @Override
  public void applySnapshot(final Snapshot snapshot) {
    requireNonNull(snapshot);

    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        LOG.info("Restoring snapshot.");

        for (SnapshotField field : SNAPSHOT_FIELDS) {
          field.restoreFromSnapshot(storeProvider, snapshot);
        }
      }
    });
  }

  private interface SnapshotField {
    void saveToSnapshot(StoreProvider storeProvider, Snapshot.Builder snapshot);

    void restoreFromSnapshot(MutableStoreProvider storeProvider, Snapshot snapshot);
  }
}
