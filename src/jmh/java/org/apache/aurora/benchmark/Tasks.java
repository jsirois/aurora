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

import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.LimitConstraint;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.gen.ValueConstraint;
import org.apache.aurora.scheduler.base.TaskTestUtil;

/**
 * Task factory.
 */
final class Tasks {

  private Tasks() {
    // Utility class.
  }

  /**
   * Builds tasks for the specified configuration.
   */
  static final class Builder {
    private JobKey.Builder jobKeyBuilder = JobKey.create("jmh", "dev", "benchmark").toBuilder();
    private int uuidStart = 0;
    private boolean isProduction = false;
    private double cpu = 6.0;
    private Amount<Long, Data> ram = Amount.of(8L, Data.GB);
    private Amount<Long, Data> disk = Amount.of(128L, Data.GB);
    private ScheduleStatus scheduleStatus = ScheduleStatus.PENDING;
    private ImmutableSet.Builder<Constraint> constraints = ImmutableSet.builder();

    Builder setRole(String newRole) {
      jobKeyBuilder.setRole(newRole);
      return this;
    }

    Builder setEnv(String env) {
      jobKeyBuilder.setEnvironment(env);
      return this;
    }

    Builder setJob(String job) {
      jobKeyBuilder.setName(job);
      return this;
    }

    Builder setUuidStart(int uuidStart) {
      this.uuidStart = uuidStart;
      return this;
    }

    Builder setCpu(double newCpu) {
      cpu = newCpu;
      return this;
    }

    Builder setRam(Amount<Long, Data> newRam) {
      ram = newRam;
      return this;
    }

    Builder setDisk(Amount<Long, Data> newDisk) {
      disk = newDisk;
      return this;
    }

    Builder setScheduleStatus(ScheduleStatus newScheduleStatus) {
      scheduleStatus = newScheduleStatus;
      return this;
    }

    Builder setProduction(boolean newProduction) {
      isProduction = newProduction;
      return this;
    }

    Builder addValueConstraint(String name, String value) {
      constraints.add(Constraint.builder()
          .setName(name)
          .setConstraint(TaskConstraint.value(ValueConstraint.builder()
              .setNegated(false)
              .setValues(value)
              .build()))
          .build());

      return this;
    }

    Builder addLimitConstraint(String name, int limit) {
      constraints.add(Constraint.builder()
          .setName(name)
          .setConstraint(TaskConstraint.limit(LimitConstraint.create(limit)))
          .build());

      return this;
    }

    /**
     * Builds a set of {@link ScheduledTask} for the current configuration.
     *
     * @param count Number of tasks to build.
     * @return Set of tasks.
     */
    Set<ScheduledTask> build(int count) {
      JobKey jobKey = jobKeyBuilder.build();
      ImmutableSet.Builder<ScheduledTask> tasks = ImmutableSet.builder();

      for (int i = 0; i < count; i++) {
        int instanceId = i; // For closure capture of i.
        String taskId =
            jobKey.getRole() + "-" + jobKey.getEnvironment() + "-" + i + "-" + (uuidStart + i);

        tasks.add(
            TaskTestUtil.makeTask(taskId, jobKey)
              .withStatus(scheduleStatus)
              .withTaskEvents(ImmutableList.of(
                  TaskEvent.create(0, ScheduleStatus.PENDING),
                  TaskEvent.create(1, scheduleStatus)))
              .withAssignedTask(at -> at.toBuilder()
                  .setInstanceId(instanceId)
                  .setTaskId(taskId)
                  .setTask(at.getTask().toBuilder()
                      .setConstraints(constraints.build())
                      .setNumCpus(cpu)
                      .setRamMb(ram.as(Data.MB))
                      .setDiskMb(disk.as(Data.MB))
                      .setProduction(isProduction)
                      .setRequestedPorts()
                      .build())
                  .build()));
      }

      return tasks.build();
    }
  }
}
