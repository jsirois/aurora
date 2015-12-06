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

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.logging.Logger;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

import org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.storage.DeduplicatedScheduledTask;
import org.apache.aurora.gen.storage.DeduplicatedSnapshot;
import org.apache.aurora.gen.storage.Snapshot;

/**
 * Converter between denormalized storage Snapshots and de-duplicated snapshots.
 *
 * <p>
 * For information on the difference in the two formats see the documentation in storage.thrift.
 */
public interface SnapshotDeduplicator {
  /**
   * Convert a Snapshot to the deduplicated format.
   *
   * @param snapshot Snapshot to convert.
   * @return deduplicated snapshot.
   */
  DeduplicatedSnapshot deduplicate(Snapshot snapshot);

  /**
   * Restore a deduplicated snapshot to its original denormalized form.
   *
   * @param snapshot Deduplicated snapshot to restore.
   * @return A full snapshot.
   * @throws CodingException when the input data is corrupt.
   */
  Snapshot reduplicate(DeduplicatedSnapshot snapshot) throws CodingException;

  class SnapshotDeduplicatorImpl implements SnapshotDeduplicator {
    private static final Logger LOG = Logger.getLogger(SnapshotDeduplicatorImpl.class.getName());

    private static final Function<ScheduledTask, TaskConfig> SCHEDULED_TO_CONFIG =
        new Function<ScheduledTask, TaskConfig>() {
          @Override
          public TaskConfig apply(ScheduledTask task) {
            return task.getAssignedTask().getTask();
          }
        };

    private static ScheduledTask withoutTaskConfig(ScheduledTask scheduledTask) {
      return scheduledTask.toBuilder()
          .setAssignedTask(scheduledTask.getAssignedTask().toBuilder()
              .setTask(null)
              .build())
          .build();
    }

    private static Snapshot withoutTasks(Snapshot snapshot) {
      return snapshot.toBuilder().setTasks().build();
    }

    @Override
    @Timed("snapshot_deduplicate")
    public DeduplicatedSnapshot deduplicate(Snapshot snapshot) {
      int numInputTasks = snapshot.getTasks().size();
      LOG.info(String.format("Starting deduplication of a snapshot with %d tasks.", numInputTasks));

      DeduplicatedSnapshot.Builder deduplicatedSnapshot = DeduplicatedSnapshot.builder()
          .setPartialSnapshot(withoutTasks(snapshot));

      // Nothing to do if we don't have any input tasks.
      if (numInputTasks == 0) {
        LOG.warning("Got snapshot with unset tasks field.");
        return deduplicatedSnapshot.build();
      }

      // Match each unique TaskConfig to its hopefully-multiple ScheduledTask owners.
      ListMultimap<TaskConfig, ScheduledTask> index = Multimaps.index(
          snapshot.getTasks(),
          SCHEDULED_TO_CONFIG);

      ImmutableList.Builder<DeduplicatedScheduledTask> deduplicatedTasks = ImmutableList.builder();
      List<TaskConfig> indexedConfigs = new ArrayList<>(index.keySet().size());
      for (Entry<TaskConfig, List<ScheduledTask>> entry : Multimaps.asMap(index).entrySet()) {
        indexedConfigs.add(entry.getKey());
        for (ScheduledTask scheduledTask : entry.getValue()) {
          deduplicatedTasks.add(DeduplicatedScheduledTask.builder()
              .setPartialScheduledTask(withoutTaskConfig(scheduledTask))
              .setTaskConfigId(indexedConfigs.size() - 1)
              .build());
        }
      }

      DeduplicatedSnapshot built =
          deduplicatedSnapshot
              .setTaskConfigs(indexedConfigs)
              .setPartialTasks(deduplicatedTasks.build())
              .build();

      int numOutputTasks = built.getTaskConfigs().size();

      LOG.info(String.format(
          "Finished deduplicating snapshot. Deduplication ratio: %d/%d = %.2f%%.",
          numInputTasks,
          numOutputTasks,
          100.0 * numInputTasks / numOutputTasks));


      return built;
    }

    @Override
    @Timed("snapshot_reduplicate")
    public Snapshot reduplicate(DeduplicatedSnapshot deduplicatedSnapshot) throws CodingException {
      LOG.info("Starting reduplication.");
      int numInputTasks = deduplicatedSnapshot.getTaskConfigs().size();
      Snapshot partialSnapshot = deduplicatedSnapshot.getPartialSnapshot();
      if (numInputTasks == 0) {
        LOG.warning("Got deduplicated snapshot with unset task configs.");
        return partialSnapshot;
      }

      Snapshot.Builder snapshot = partialSnapshot.toBuilder();
      ImmutableSet.Builder<ScheduledTask> scheduledTasks = ImmutableSet.builder();
      scheduledTasks.addAll(partialSnapshot.getTasks());
      for (DeduplicatedScheduledTask partialTask : deduplicatedSnapshot.getPartialTasks()) {
        int taskConfigId = partialTask.getTaskConfigId();
        TaskConfig config;
        try {
          config = deduplicatedSnapshot.getTaskConfigs().get(taskConfigId);
        } catch (IndexOutOfBoundsException e) {
          throw new CodingException(
              "DeduplicatedScheduledTask referenced invalid task index " + taskConfigId, e);
        }
        ScheduledTask partialScheduledTask = partialTask.getPartialScheduledTask();
        ScheduledTask scheduledTask = partialScheduledTask.toBuilder()
            .setAssignedTask(partialScheduledTask.getAssignedTask().toBuilder()
                .setTask(config)
                .build())
            .build();
        scheduledTasks.add(scheduledTask);
      }
      snapshot.setTasks(scheduledTasks.build());

      Snapshot built = snapshot.build();
      int numOutputTasks = built.getTasks().size();
      LOG.info(String.format(
          "Finished reduplicating snapshot. Compression ratio: %d/%d = %.2f%%.",
          numInputTasks,
          numOutputTasks,
          100.0 * numInputTasks / numOutputTasks));

      return built;
    }
  }
}
