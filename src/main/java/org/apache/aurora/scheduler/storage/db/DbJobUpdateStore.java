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

import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.base.MorePreconditions;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.storage.StoredJobUpdateDetails;
import org.apache.aurora.scheduler.stats.CachedCounters;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.db.views.DbJobUpdate;
import org.apache.aurora.scheduler.storage.db.views.DbJobUpdateInstructions;
import org.apache.aurora.scheduler.storage.db.views.DbStoredJobUpdateDetails;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.Range;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.common.inject.TimedInterceptor.Timed;

/**
 * A relational database-backed job update store.
 */
public class DbJobUpdateStore implements JobUpdateStore.Mutable {

  private final JobKeyMapper jobKeyMapper;
  private final JobUpdateDetailsMapper detailsMapper;
  private final JobUpdateEventMapper jobEventMapper;
  private final JobInstanceUpdateEventMapper instanceEventMapper;
  private final TaskConfigManager taskConfigManager;
  private final CachedCounters stats;

  @Inject
  DbJobUpdateStore(
      JobKeyMapper jobKeyMapper,
      JobUpdateDetailsMapper detailsMapper,
      JobUpdateEventMapper jobEventMapper,
      JobInstanceUpdateEventMapper instanceEventMapper,
      TaskConfigManager taskConfigManager,
      CachedCounters stats) {

    this.jobKeyMapper = requireNonNull(jobKeyMapper);
    this.detailsMapper = requireNonNull(detailsMapper);
    this.jobEventMapper = requireNonNull(jobEventMapper);
    this.instanceEventMapper = requireNonNull(instanceEventMapper);
    this.taskConfigManager = requireNonNull(taskConfigManager);
    this.stats = requireNonNull(stats);
  }

  @Timed("job_update_store_save_update")
  @Override
  public void saveJobUpdate(JobUpdate update, Optional<String> lockToken) {
    requireNonNull(update);
    if (!update.getInstructions().isSetDesiredState()
        && update.getInstructions().getInitialState().isEmpty()) {
      throw new IllegalArgumentException(
          "Missing both initial and desired states. At least one is required.");
    }

    JobUpdateKey key = update.getSummary().getKey();
    jobKeyMapper.merge(key.getJob());
    detailsMapper.insert(update);

    if (lockToken.isPresent()) {
      detailsMapper.insertLockToken(key, lockToken.get());
    }

    // Insert optional instance update overrides.
    Set<Range> instanceOverrides =
        update.getInstructions().getSettings().getUpdateOnlyTheseInstances();

    if (!instanceOverrides.isEmpty()) {
      detailsMapper.insertInstanceOverrides(key, instanceOverrides);
    }

    // Insert desired state task config and instance mappings.
    if (update.getInstructions().isSetDesiredState()) {
      InstanceTaskConfig desired = update.getInstructions().getDesiredState();
      detailsMapper.insertTaskConfig(
          key,
          taskConfigManager.insert(desired.getTask()),
          true,
          new InsertResult());

      detailsMapper.insertDesiredInstances(
          key,
          MorePreconditions.checkNotBlank(desired.getInstances()));
    }

    // Insert initial state task configs and instance mappings.
    if (!update.getInstructions().getInitialState().isEmpty()) {
      for (InstanceTaskConfig config : update.getInstructions().getInitialState()) {
        InsertResult result = new InsertResult();
        detailsMapper.insertTaskConfig(
            key,
            taskConfigManager.insert(config.getTask()),
            false,
            result);

        detailsMapper.insertTaskConfigInstances(
            result.getId(),
            MorePreconditions.checkNotBlank(config.getInstances()));
      }
    }
  }

  @VisibleForTesting
  static String statName(JobUpdateStatus status) {
    return "update_transition_" + status;
  }

  @Timed("job_update_store_save_event")
  @Override
  public void saveJobUpdateEvent(JobUpdateKey key, JobUpdateEvent event) {
    stats.get(statName(event.getStatus())).incrementAndGet();
    jobEventMapper.insert(key, event);
  }

  @Timed("job_update_store_save_instance_event")
  @Override
  public void saveJobInstanceUpdateEvent(JobUpdateKey key, JobInstanceUpdateEvent event) {
    instanceEventMapper.insert(key, event);
  }

  @Timed("job_update_store_delete_all")
  @Override
  public void deleteAllUpdatesAndEvents() {
    detailsMapper.truncate();
  }

  private static final Function<PruneVictim, JobUpdateKey> GET_UPDATE_KEY =
      new Function<PruneVictim, JobUpdateKey>() {
        @Override
        public JobUpdateKey apply(PruneVictim victim) {
          return victim.getUpdate();
        }
      };

  @Timed("job_update_store_prune_history")
  @Override
  public Set<JobUpdateKey> pruneHistory(int perJobRetainCount, long historyPruneThresholdMs) {
    ImmutableSet.Builder<JobUpdateKey> pruned = ImmutableSet.builder();

    Set<Long> jobKeyIdsToPrune = detailsMapper.selectJobKeysForPruning(
        perJobRetainCount,
        historyPruneThresholdMs);

    for (long jobKeyId : jobKeyIdsToPrune) {
      Set<PruneVictim> pruneVictims = detailsMapper.selectPruneVictims(
          jobKeyId,
          perJobRetainCount,
          historyPruneThresholdMs);

      detailsMapper.deleteCompletedUpdates(
          FluentIterable.from(pruneVictims).transform(PruneVictim::getRowId).toSet());
      pruned.addAll(FluentIterable.from(pruneVictims).transform(GET_UPDATE_KEY));
    }

    return pruned.build();
  }

  @Timed("job_update_store_fetch_summaries")
  @Override
  public List<JobUpdateSummary> fetchJobUpdateSummaries(JobUpdateQuery query) {
    return detailsMapper.selectSummaries(query);
  }

  @Timed("job_update_store_fetch_details_list")
  @Override
  public List<JobUpdateDetails> fetchJobUpdateDetails(JobUpdateQuery query) {
    return FluentIterable
        .from(detailsMapper.selectDetailsList(query))
        .transform(DbStoredJobUpdateDetails::toThrift)
        .transform(StoredJobUpdateDetails::getDetails)
        .toList();
  }

  @Timed("job_update_store_fetch_details")
  @Override
  public Optional<JobUpdateDetails> fetchJobUpdateDetails(JobUpdateKey key) {
    return Optional.fromNullable(detailsMapper.selectDetails(key))
        .transform(DbStoredJobUpdateDetails::toThrift)
        .transform(StoredJobUpdateDetails::getDetails);
  }

  @Timed("job_update_store_fetch_update")
  @Override
  public Optional<JobUpdate> fetchJobUpdate(JobUpdateKey key) {
    return Optional.fromNullable(detailsMapper.selectUpdate(key))
        .transform(DbJobUpdate::toImmutable);
  }

  @Timed("job_update_store_fetch_instructions")
  @Override
  public Optional<JobUpdateInstructions> fetchJobUpdateInstructions(JobUpdateKey key) {
    return Optional.fromNullable(detailsMapper.selectInstructions(key))
        .transform(DbJobUpdateInstructions::toImmutable);
  }

  @Timed("job_update_store_fetch_all_details")
  @Override
  public Set<StoredJobUpdateDetails> fetchAllJobUpdateDetails() {
    return FluentIterable.from(detailsMapper.selectAllDetails())
        .transform(DbStoredJobUpdateDetails::toThrift)
        .toSet();
  }

  @Timed("job_update_store_get_lock_token")
  @Override
  public Optional<String> getLockToken(JobUpdateKey key) {
    // We assume here that cascading deletes will cause a lock-update associative row to disappear
    // when the lock is invalidated.  This further assumes that a lock row is deleted when a lock
    // is no longer valid.
    return Optional.fromNullable(detailsMapper.selectLockToken(key));
  }

  @Timed("job_update_store_fetch_instance_events")
  @Override
  public List<JobInstanceUpdateEvent> fetchInstanceEvents(JobUpdateKey key, int instanceId) {
    return detailsMapper.selectInstanceUpdateEvents(key, instanceId);
  }
}
