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

import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;

import static com.google.common.base.CharMatcher.WHITESPACE;

/**
 * Stores all tasks configured with the scheduler.
 */
public interface TaskStore {

  /**
   * Fetches a read-only view of tasks matching a query and filters. Intended for use with a
   * {@link org.apache.aurora.scheduler.base.Query.Builder}.
   *
   * @param query Builder of the query to identify tasks with.
   * @return A read-only view of matching tasks.
   */
  Iterable<ScheduledTask> fetchTasks(Query.Builder query);

  /**
   * Fetches all job keys represented in the task store.
   *
   * @return Job keys of stored tasks.
   */
  Set<JobKey> getJobKeys();

  interface Mutable extends TaskStore {

    /**
     * A convenience interface to allow callers to more concisely implement a task mutation.
     */
    interface TaskMutation extends Function<ScheduledTask, ScheduledTask> {
    }

    /**
     * Saves tasks to the store.  Tasks are copied internally, meaning that the tasks are stored in
     * the state they were in when the method is called, and further object modifications will not
     * affect the tasks.  If any of the tasks already exist in the store, they will be overwritten
     * by the supplied {@code newTasks}.
     *
     * @param tasks Tasks to add.
     */
    void saveTasks(Set<ScheduledTask> tasks);

    /**
     * Removes all tasks from the store.
     * TODO(wfarner): Move this and other mass-delete methods to an interface that is only
     * accessible by SnapshotStoreImpl.
     */
    void deleteAllTasks();

    /**
     * Deletes specific tasks from the store.
     *
     * @param taskIds IDs of tasks to delete.
     */
    void deleteTasks(Set<String> taskIds);

    /**
     * Offers temporary mutable access to tasks.  If a task ID is not found, it will be silently
     * skipped, and no corresponding task will be returned.
     * TODO(wfarner): Consider a non-batch variant of this, since that's a more common use case,
     * and it prevents the caller from worrying about a bad query having broad impact.
     *
     * @param query Query to match tasks against.
     * @param mutator The mutate operation.
     * @return Immutable copies of only the tasks that were mutated.
     */
    ImmutableSet<ScheduledTask> mutateTasks(
        Query.Builder query,
        Function<ScheduledTask, ScheduledTask> mutator);

    /**
     * Rewrites a task's configuration in-place.
     * <p>
     * <b>WARNING</b>: this is a dangerous operation, and should not be used without exercising
     * great care.  This feature should be used as a last-ditch effort to rewrite things that
     * the scheduler otherwise can't (e.g. {@link TaskConfig#getExecutorConfig()}) rewrite in a
     * controlled/tested backfill operation.
     *
     * @param taskId ID of the task to alter.
     * @param taskConfiguration Configuration object to swap with the existing task's
     *                          configuration.
     * @return {@code true} if the modification took effect, or {@code false} if the task does not
     *         exist in the store.
     */
    boolean unsafeModifyInPlace(String taskId, TaskConfig taskConfiguration);
  }

  final class Util {
    private Util() {
      // Utility class.
    }

    public static Predicate<ScheduledTask> queryFilter(final Query.Builder queryBuilder) {
      return new Predicate<ScheduledTask>() {
        @Override
        public boolean apply(ScheduledTask task) {
          TaskQuery query = queryBuilder.get();
          TaskConfig config = task.getAssignedTask().getTask();
          // TODO(wfarner): Investigate why blank inputs are treated specially for the role field.
          if (query.getRole() != null
              && !WHITESPACE.matchesAllOf(query.getRole())
              && !query.getRole().equals(config.getJob().getRole())) {
            return false;
          }
          if (query.getEnvironment() != null
              && !query.getEnvironment().equals(config.getEnvironment())) {
            return false;
          }
          if (query.getJobName() != null && !query.getJobName().equals(config.getJobName())) {
            return false;
          }

          if (!query.getJobKeys().isEmpty() && !query.getJobKeys().contains(config.getJob())) {
            return false;
          }
          if (!query.getTaskIds().isEmpty() && !query.getTaskIds().contains(Tasks.id(task))) {
            return false;
          }

          if (!query.getStatuses().isEmpty() && !query.getStatuses().contains(task.getStatus())) {
            return false;
          }
          if (!query.getSlaveHosts().isEmpty()
              && !query.getSlaveHosts().contains(task.getAssignedTask().getSlaveHost())) {
            return false;
          }
          if (!query.getInstanceIds().isEmpty()
              && !query.getInstanceIds().contains(task.getAssignedTask().getInstanceId())) {
            return false;
          }

          return true;
        }
      };
    }
  }
}
