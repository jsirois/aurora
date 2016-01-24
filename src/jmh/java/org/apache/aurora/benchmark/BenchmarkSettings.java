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

import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.ScheduledTask;

import static java.util.Objects.requireNonNull;

/**
 * Benchmark test settings.
 */
final class BenchmarkSettings {
  private final Set<HostAttributes> hostAttributes;
  private final double clusterUtilization;
  private final boolean allVictimsEligibleForPreemption;
  private final Set<ScheduledTask> tasks;

  private BenchmarkSettings(
      double clusterUtilization,
      boolean allVictimsEligibleForPreemption,
      Set<HostAttributes> hostAttributes,
      Set<ScheduledTask> tasks) {

    this.clusterUtilization = clusterUtilization;
    this.allVictimsEligibleForPreemption = allVictimsEligibleForPreemption;
    this.hostAttributes = requireNonNull(hostAttributes);
    this.tasks = requireNonNull(tasks);
  }

  /**
   * Gets the cluster utilization factor specifying what percentage of hosts in the cluster
   * already have tasks assigned to them.
   *
   * @return Cluster utilization (default: 0.9).
   */
  double getClusterUtilization() {
    return clusterUtilization;
  }

  /**
   * Flag indicating whether all existing assigned tasks are available for preemption.
   *
   * @return Victim preemption eligibility (default: false).
   */
  boolean areAllVictimsEligibleForPreemption() {
    return allVictimsEligibleForPreemption;
  }

  /**
   * Gets a set of cluster host attributes.
   *
   * @return Set of {@link HostAttributes}.
   */
  Set<HostAttributes> getHostAttributes() {
    return hostAttributes;
  }

  /**
   * Gets a benchmark task.
   *
   * @return Task to run a benchmark for.
   */
  Set<ScheduledTask> getTasks() {
    return tasks;
  }

  static class Builder {
    private double clusterUtilization = 0.9;
    private boolean allVictimsEligibleForPreemption;
    private Set<HostAttributes> hostAttributes;
    private Set<ScheduledTask> tasks;

    Builder setClusterUtilization(double newClusterUtilization) {
      clusterUtilization = newClusterUtilization;
      return this;
    }

    Builder setVictimPreemptionEligibilty(boolean newPreemptionEligibility) {
      allVictimsEligibleForPreemption = newPreemptionEligibility;
      return this;
    }

    Builder setHostAttributes(Set<HostAttributes> newHostAttributes) {
      hostAttributes = newHostAttributes;
      return this;
    }

    Builder setTasks(Set<ScheduledTask> newTasks) {
      tasks = newTasks;
      return this;
    }

    BenchmarkSettings build() {
      return new BenchmarkSettings(
          clusterUtilization,
          allVictimsEligibleForPreemption,
          hostAttributes,
          tasks);
    }
  }
}
