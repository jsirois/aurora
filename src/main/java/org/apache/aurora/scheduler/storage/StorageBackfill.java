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

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import org.apache.aurora.common.stats.Stats;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.TaskStore.Mutable.TaskMutation;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.ScheduledTask;
/**
 * Utility class to contain and perform storage backfill operations.
 */
public final class StorageBackfill {

  private static final Logger LOG = Logger.getLogger(StorageBackfill.class.getName());

  private static final AtomicLong BACKFILLED_TASK_CONFIG_KEYS =
      Stats.exportLong("task_config_keys_backfilled");

  private static final AtomicLong BACKFILLED_JOB_CONFIG_KEYS =
      Stats.exportLong("job_store_task_config_keys_backfilled");

  private StorageBackfill() {
    // Utility class.
  }

  private static void backfillJobDefaults(CronJobStore.Mutable jobStore) {
    for (JobConfiguration job : jobStore.fetchJobs()) {
      TaskConfig config = populateJobKey(job.getTaskConfig(), BACKFILLED_JOB_CONFIG_KEYS);
      jobStore.saveAcceptedJob(job.toBuilder().setTaskConfig(config).build());
    }
  }

  private static TaskConfig populateJobKey(TaskConfig config, AtomicLong counter) {
    if (!config.isSetJob() || !JobKeys.isValid(config.getJob())) {
      counter.incrementAndGet();
      return config.toBuilder()
          .setJob(JobKey.builder()
              .setRole(config.getOwner().getRole())
              .setEnvironment(config.getEnvironment())
              .setName(config.getJobName())
              .build())
          .build();
    } else {
      return config;
    }
  }

  /**
   * Backfills the storage to make it match any assumptions that may have changed since
   * the structs were first written.
   *
   * @param storeProvider Storage provider.
   */
  public static void backfill(final MutableStoreProvider storeProvider) {
    backfillJobDefaults(storeProvider.getCronJobStore());

    // Backfilling job keys has to be done in a separate transaction to ensure follow up scoped
    // Query calls work against upgraded MemTaskStore, which does not support deprecated fields.
    LOG.info("Backfilling task config job keys.");
    storeProvider.getUnsafeTaskStore().mutateTasks(Query.unscoped(), new TaskMutation() {
      @Override
      public ScheduledTask apply(ScheduledTask task) {
        AssignedTask assignedTask = task.getAssignedTask();
        TaskConfig config = populateJobKey(assignedTask.getTask(), BACKFILLED_TASK_CONFIG_KEYS);
        return task.toBuilder()
            .setAssignedTask(assignedTask.toBuilder()
                .setTask(config)
                .build())
            .build();
      }
    });
  }
}
