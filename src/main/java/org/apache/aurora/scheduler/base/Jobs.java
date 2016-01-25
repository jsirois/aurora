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

import org.apache.aurora.gen.JobStats;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;

/**
 * Convenience methods related to jobs.
 */
public final class Jobs {

  private Jobs() {
    // Utility class.
  }

  /**
   * For a given collection of tasks compute statistics based on the state of the task.
   *
   * @param tasks a collection of tasks for which statistics are sought
   * @return an JobStats object containing the statistics about the tasks.
   */
  public static JobStats getJobStats(Iterable<ScheduledTask> tasks) {
    int pendingTaskCount = 0;
    int activeTaskCount = 0;
    int finishedTaskCount = 0;
    int failedTaskCount = 0;
    for (ScheduledTask task : tasks) {
      ScheduleStatus status = task.getStatus();
      switch (status) {
        case INIT:
        case PENDING:
        case THROTTLED:
          pendingTaskCount++;
          break;

        case ASSIGNED:
        case STARTING:
        case RESTARTING:
        case RUNNING:
        case KILLING:
        case DRAINING:
        case PREEMPTING:
          activeTaskCount++;
          break;

        case KILLED:
        case FINISHED:
          finishedTaskCount++;
          break;

        case LOST:
        case FAILED:
          failedTaskCount++;
          break;

        default:
          throw new IllegalArgumentException("Unsupported status: " + status);
      }
    }
    return JobStats.builder()
        .setPendingTaskCount(pendingTaskCount)
        .setActiveTaskCount(activeTaskCount)
        .setFinishedTaskCount(finishedTaskCount)
        .setFailedTaskCount(failedTaskCount)
        .build();
  }
}
