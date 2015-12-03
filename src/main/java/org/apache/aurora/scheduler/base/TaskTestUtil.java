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
package org.apache.aurora.scheduler.base;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.DockerContainer;
import org.apache.aurora.gen.DockerParameter;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.LimitConstraint;
import org.apache.aurora.gen.Metadata;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.gen.ValueConstraint;
import org.apache.aurora.scheduler.TierInfo;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;

/**
 * Convenience methods for working with tasks.
 * <p>
 * TODO(wfarner): This lives in under src/main only so benchmarks can access it.  Reconsider the
 * project layout so this is not necessary.
 */
public final class TaskTestUtil {

  public static final JobKey JOB = JobKeys.from("role", "env", "job");
  public static final TierInfo REVOCABLE_TIER = new TierInfo(true);

  private TaskTestUtil() {
    // Utility class.
  }

  public static TaskConfig makeConfig(JobKey job) {
    return TaskConfig.builder()
        .setJob(job)
        .setJobName(job.getName())
        .setEnvironment(job.getEnvironment())
        .setOwner(Identity.create(job.getRole(), job.getRole() + "-user"))
        .setIsService(true)
        .setNumCpus(1.0)
        .setRamMb(1024)
        .setDiskMb(1024)
        .setPriority(1)
        .setMaxTaskFailures(-1)
        .setProduction(true)
        .setTier("tier-" + job.getEnvironment())
        .setConstraints(ImmutableSet.of(
            Constraint.create(
                "valueConstraint",
                TaskConstraint.value(
                    ValueConstraint.create(true, ImmutableSet.of("value1", "value2")))),
            Constraint.create(
                "limitConstraint",
                TaskConstraint.limit(LimitConstraint.create(5)))))
        .setRequestedPorts(ImmutableSet.of("http"))
        .setTaskLinks(ImmutableMap.of("http", "link", "admin", "otherLink"))
        .setContactEmail("foo@bar.com")
        .setMetadata(ImmutableSet.of(Metadata.create("key", "value")))
        .setExecutorConfig(ExecutorConfig.create("name", "config"))
        .setContainer(Container.docker(
            DockerContainer.builder()
                .setImage("imagename")
                .setParameters(
                    DockerParameter.create("a", "b"),
                    DockerParameter.create("c", "d"))
                .build()))
        .build();
  }

  public static ScheduledTask makeTask(String id, JobKey job) {
    return makeTask(id, makeConfig(job));
  }

  public static ScheduledTask makeTask(String id, TaskConfig config) {
    return ScheduledTask.builder()
        .setStatus(ScheduleStatus.PENDING)
        .setTaskEvents(ImmutableList.of(
            TaskEvent.builder()
                .setTimestamp(100L)
                .setStatus(ScheduleStatus.PENDING)
                .setMessage("message")
                .setScheduler("scheduler")
                .build(),
            TaskEvent.builder()
                .setTimestamp(101L)
                .setStatus(ScheduleStatus.ASSIGNED)
                .setMessage("message")
                .setScheduler("scheduler2")
                .build()))
        .setAncestorId("ancestor")
        .setFailureCount(3)
        .setAssignedTask(AssignedTask.builder()
            .setInstanceId(2)
            .setTaskId(id)
            .setAssignedPorts(ImmutableMap.of("http", 1000))
            .setTask(config)
            .build())
        .build();
  }

  public static ScheduledTask addStateTransition(
      ScheduledTask task,
      ScheduleStatus status,
      long timestamp) {

    return task.toBuilder()
        .setStatus(status)
        .addToTaskEvents(TaskEvent.builder()
            .setTimestamp(timestamp)
            .setStatus(status)
            .setScheduler("scheduler")
            .build())
        .build();
  }
}
