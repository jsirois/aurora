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
package org.apache.aurora.benchmark;

import java.util.Arrays;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateAction;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.TaskTestUtil;

/**
 * Job update factory.
 */
final class JobUpdates {

  static final class Builder {
    private static final String USER = "user";
    private int numEvents = 1;
    private int numInstanceEvents = 5000;

    Builder setNumEvents(int newCount) {
      numEvents = newCount;
      return this;
    }

    Builder setNumInstanceEvents(int newCount) {
      numInstanceEvents = newCount;
      return this;
    }

    Set<JobUpdateDetails> build(int count) {
      ImmutableSet.Builder<JobUpdateDetails> result = ImmutableSet.builder();
      for (int i = 0; i < count; i++) {
        JobKey job = JobKey.create("role", "env", UUID.randomUUID().toString());
        JobUpdateKey key = JobUpdateKey.create(job, UUID.randomUUID().toString());

        TaskConfig taskConfig = TaskTestUtil.makeConfig(job);
        TaskConfig task = taskConfig.toBuilder()
            .setExecutorConfig(taskConfig.getExecutorConfig().toBuilder()
                .setData(string(10000))
                .build())
            .build();

        JobUpdate update = JobUpdate.builder()
            .setSummary(JobUpdateSummary.builder()
                .setKey(key)
                .setUser(USER)
                .build())
            .setInstructions(JobUpdateInstructions.builder()
                .setSettings(JobUpdateSettings.builder()
                    .setUpdateGroupSize(100)
                    .setMaxFailedInstances(1)
                    .setMaxPerInstanceFailures(1)
                    .setMaxWaitToInstanceRunningMs(1)
                    .setMinWaitInInstanceRunningMs(1)
                    .setRollbackOnFailure(true)
                    .setWaitForBatchCompletion(false)
                    .build())
                .setInitialState(InstanceTaskConfig.builder()
                    .setTask(task)
                    .setInstances(Range.create(0, 10))
                    .build())
                .setDesiredState(InstanceTaskConfig.builder()
                    .setTask(task)
                    .setInstances(Range.create(0, numInstanceEvents))
                    .build())
                .build())
            .build();

        ImmutableList.Builder<JobUpdateEvent> events = ImmutableList.builder();
        for (int j = 0; j < numEvents; j++) {
          events.add(JobUpdateEvent.builder()
              .setStatus(JobUpdateStatus.ROLLING_FORWARD)
              .setTimestampMs(j)
              .setUser(USER)
              .setMessage("message")
              .build());
        }

        ImmutableList.Builder<JobInstanceUpdateEvent> instances = ImmutableList.builder();
        for (int k = 0; k < numInstanceEvents; k++) {
          instances.add(JobInstanceUpdateEvent.create(k, 0L, JobUpdateAction.INSTANCE_UPDATING));
        }

        result.add(JobUpdateDetails.builder()
            .setUpdate(update)
            .setUpdateEvents(events.build())
            .setInstanceEvents(instances.build())
            .build());
      }

      return result.build();
    }

    private static String string(int numChars) {
      char[] chars = new char[numChars];
      Arrays.fill(chars, 'a');
      return new String(chars);
    }
  }
}
