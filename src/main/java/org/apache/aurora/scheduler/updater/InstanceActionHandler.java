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
package org.apache.aurora.scheduler.updater;

import java.util.logging.Logger;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.gen.InstanceKey;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.state.StateManager;

import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_FORWARD;
import static org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;

interface InstanceActionHandler {

  Amount<Long, Time> getReevaluationDelay(
      InstanceKey instance,
      JobUpdateInstructions instructions,
      MutableStoreProvider storeProvider,
      StateManager stateManager,
      JobUpdateStatus status);

  Logger LOG = Logger.getLogger(InstanceActionHandler.class.getName());

  class AddTask implements InstanceActionHandler {
    private static TaskConfig getTargetConfig(
        JobUpdateInstructions instructions,
        boolean rollingForward,
        int instanceId) {

      if (rollingForward) {
        // Desired state is assumed to be non-null when AddTask is used.
        return instructions.getDesiredState().getTask();
      } else {
        for (InstanceTaskConfig config : instructions.getInitialState()) {
          for (Range range : config.getInstances()) {
            if (com.google.common.collect.Range.closed(range.getFirst(), range.getLast())
                .contains(instanceId)) {
              return config.getTask();
            }
          }
        }

        throw new IllegalStateException("Failed to find instance " + instanceId);
      }
    }

    @Override
    public Amount<Long, Time> getReevaluationDelay(
        InstanceKey instance,
        JobUpdateInstructions instructions,
        MutableStoreProvider storeProvider,
        StateManager stateManager,
        JobUpdateStatus status) {

      LOG.info("Adding instance " + instance + " while " + status);
      TaskConfig replacement = getTargetConfig(
          instructions,
          status == ROLLING_FORWARD,
          instance.getInstanceId());
      stateManager.insertPendingTasks(
          storeProvider,
          replacement,
          ImmutableSet.of(instance.getInstanceId()));
      return  Amount.of(
          (long) instructions.getSettings().getMaxWaitToInstanceRunningMs(),
          Time.MILLISECONDS);
    }
  }

  class KillTask implements InstanceActionHandler {
    @Override
    public Amount<Long, Time> getReevaluationDelay(
        InstanceKey instance,
        JobUpdateInstructions instructions,
        MutableStoreProvider storeProvider,
        StateManager stateManager,
        JobUpdateStatus status) {

      String taskId = Tasks.id(Iterables.getOnlyElement(
          storeProvider.getTaskStore().fetchTasks(Query.instanceScoped(instance).active())));
      LOG.info("Killing " + instance + " while " + status);
      stateManager.changeState(
          storeProvider,
          taskId,
          Optional.absent(),
          ScheduleStatus.KILLING,
          Optional.of("Killed for job update."));
      return Amount.of(
          (long) instructions.getSettings().getMaxWaitToInstanceRunningMs(),
          Time.MILLISECONDS);
    }
  }

  class WatchRunningTask implements InstanceActionHandler {
    @Override
    public Amount<Long, Time> getReevaluationDelay(
        InstanceKey instance,
        JobUpdateInstructions instructions,
        MutableStoreProvider storeProvider,
        StateManager stateManager,
        JobUpdateStatus status) {

      return Amount.of(
          (long) instructions.getSettings().getMinWaitInInstanceRunningMs(),
          Time.MILLISECONDS);
    }
  }
}
