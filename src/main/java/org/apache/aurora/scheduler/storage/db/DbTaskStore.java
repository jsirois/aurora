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
package org.apache.aurora.scheduler.storage.db;

import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

import org.apache.aurora.GuavaUtils;
import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.peer.MutableJobKey;
import org.apache.aurora.gen.peer.MutableScheduledTask;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Query.Builder;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.storage.db.views.Pairs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A task store implementation based on a relational database.
 * <p>
 * TODO(wfarner): Consider modifying code generator to support directly producing TaskConfig, etc
 * from myBatis (it will set private final fields just fine).  This would reduce memory and time
 * spent translating and copying objects.
 */
class DbTaskStore implements TaskStore.Mutable {

  private static final Logger LOG = LoggerFactory.getLogger(DbTaskStore.class);

  private final TaskMapper taskMapper;
  private final TaskConfigManager configManager;
  private final Clock clock;
  private final long slowQueryThresholdNanos;

  @Inject
  DbTaskStore(
      TaskMapper taskMapper,
      TaskConfigManager configManager,
      Clock clock,
      Amount<Long, Time> slowQueryThreshold) {

    LOG.warn("DbTaskStore is experimental, and should not be used in production clusters!");
    this.taskMapper = requireNonNull(taskMapper);
    this.configManager = requireNonNull(configManager);
    this.clock = requireNonNull(clock);
    this.slowQueryThresholdNanos =  slowQueryThreshold.as(Time.NANOSECONDS);
  }

  @Timed("db_storage_fetch_task")
  @Override
  public Optional<ScheduledTask> fetchTask(String taskId) {
    requireNonNull(taskId);
    return Optional.fromNullable(taskMapper.selectById(taskId))
        .transform(MutableScheduledTask::toThrift);
  }

  @Timed("db_storage_fetch_tasks")
  @Override
  public ImmutableSet<ScheduledTask> fetchTasks(Builder query) {
    requireNonNull(query);

    // TODO(wfarner): Consider making slow query logging more reusable, or pushing it down into the
    //                database.
    long start = clock.nowNanos();
    ImmutableSet<ScheduledTask> result = matches(query).toSet();
    long durationNanos = clock.nowNanos() - start;
    boolean infoLevel = durationNanos >= slowQueryThresholdNanos;
    long time = Amount.of(durationNanos, Time.NANOSECONDS).as(Time.MILLISECONDS);
    String message = "Query took {} ms: {}";
    if (infoLevel) {
      LOG.info(message, time, query.get());
    } else {
      LOG.debug(message, time, query.get());
    }

    return result;
  }

  @Timed("db_storage_get_job_keys")
  @Override
  public ImmutableSet<JobKey> getJobKeys() {
    return taskMapper.selectJobKeys().stream()
        .map(MutableJobKey::toThrift)
        .collect(GuavaUtils.toImmutableSet());
  }

  @Timed("db_storage_save_tasks")
  @Override
  public void saveTasks(Set<ScheduledTask> tasks) {
    if (tasks.isEmpty()) {
      return;
    }

    // TODO(wfarner): Restrict the TaskStore.Mutable methods to more specific mutations.  It would
    //                simplify this code if we did not have to handle full object tree mutations.

    deleteTasks(Tasks.ids(tasks));

    // Maintain a cache of all task configs that exist for a job key so that identical entities
    LoadingCache<TaskConfig, Long> configCache = CacheBuilder.newBuilder()
        .build(new CacheLoader<TaskConfig, Long>() {
          @Override
          public Long load(TaskConfig config) {
            return configManager.insert(config);
          }
        });

    for (ScheduledTask task : tasks) {
      InsertResult result = new InsertResult();
      taskMapper.insertScheduledTask(
          task,
          configCache.getUnchecked(task.getAssignedTask().getTask()),
          result);

      if (!task.getTaskEvents().isEmpty()) {
        taskMapper.insertTaskEvents(result.getId(), task.getTaskEvents());
      }
      if (!task.getAssignedTask().getAssignedPorts().isEmpty()) {
        taskMapper.insertPorts(
            result.getId(),
            Pairs.fromMap(task.getAssignedTask().getAssignedPorts()));
      }
    }
  }

  @Timed("db_storage_delete_all_tasks")
  @Override
  public void deleteAllTasks() {
    taskMapper.truncate();
  }

  @Timed("db_storage_delete_tasks")
  @Override
  public void deleteTasks(Set<String> taskIds) {
    if (!taskIds.isEmpty()) {
      taskMapper.deleteTasks(taskIds);
    }
  }

  @Timed("db_storage_mutate_task")
  @Override
  public Optional<ScheduledTask> mutateTask(
      String taskId,
      Function<ScheduledTask, ScheduledTask> mutator) {

    requireNonNull(taskId);
    requireNonNull(mutator);

    return fetchTask(taskId).transform(original -> {
      ScheduledTask maybeMutated = mutator.apply(original);
      requireNonNull(maybeMutated);
      if (!original.equals(maybeMutated)) {
        Preconditions.checkState(
            Tasks.id(original).equals(Tasks.id(maybeMutated)),
            "A task's ID may not be mutated.");
        saveTasks(ImmutableSet.of(maybeMutated));
      }
      return maybeMutated;
    });
  }

  @Timed("db_storage_unsafe_modify_in_place")
  @Override
  public boolean unsafeModifyInPlace(String taskId, TaskConfig taskConfiguration) {
    checkNotNull(taskId);
    checkNotNull(taskConfiguration);
    Optional<ScheduledTask> task = fetchTask(taskId);
    if (task.isPresent()) {
      deleteTasks(ImmutableSet.of(taskId));
      ScheduledTask modified = task.get().withAssignedTask(at -> at.withTask(taskConfiguration));
      saveTasks(ImmutableSet.of(modified));
      return true;
    }
    return false;
  }

  private FluentIterable<ScheduledTask> matches(Query.Builder query) {
    Iterable<MutableScheduledTask> results;
    Predicate<ScheduledTask> filter;
    if (query.get().getTaskIds().size() == 1) {
      // Optimize queries that are scoped to a single task, as the dynamic SQL used for arbitrary
      // queries comes with a performance penalty.
      results = Optional.fromNullable(
          taskMapper.selectById(Iterables.getOnlyElement(query.get().getTaskIds())))
          .asSet();
      filter = Util.queryFilter(query);
    } else {
      results = taskMapper.select(query.get());
      // Additional filtering is not necessary in this case, since the query does it for us.
      filter = Predicates.alwaysTrue();
    }

    return FluentIterable.from(results)
        .transform(MutableScheduledTask::toThrift)
        .filter(filter);
  }
}
