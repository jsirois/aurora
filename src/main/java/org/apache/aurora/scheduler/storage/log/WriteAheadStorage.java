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

import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.PruneJobUpdateHistory;
import org.apache.aurora.gen.storage.RemoveJob;
import org.apache.aurora.gen.storage.RemoveLock;
import org.apache.aurora.gen.storage.RemoveQuota;
import org.apache.aurora.gen.storage.RemoveTasks;
import org.apache.aurora.gen.storage.RewriteTask;
import org.apache.aurora.gen.storage.SaveCronJob;
import org.apache.aurora.gen.storage.SaveFrameworkId;
import org.apache.aurora.gen.storage.SaveHostAttributes;
import org.apache.aurora.gen.storage.SaveJobInstanceUpdateEvent;
import org.apache.aurora.gen.storage.SaveJobUpdate;
import org.apache.aurora.gen.storage.SaveJobUpdateEvent;
import org.apache.aurora.gen.storage.SaveLock;
import org.apache.aurora.gen.storage.SaveQuota;
import org.apache.aurora.gen.storage.SaveTasks;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.CronJobStore;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.LockStore;
import org.apache.aurora.scheduler.storage.QuotaStore;
import org.apache.aurora.scheduler.storage.SchedulerStore;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.slf4j.Logger;

import uno.perk.forward.Forward;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.storage.log.LogStorage.TransactionManager;

/**
 * Mutable stores implementation that translates all operations to {@link Op}s (which are passed
 * to a provided {@link TransactionManager}) before forwarding the operations to delegate mutable
 * stores.
 */
@Forward({
    SchedulerStore.class,
    CronJobStore.class,
    TaskStore.class,
    LockStore.class,
    QuotaStore.class,
    AttributeStore.class,
    JobUpdateStore.class})
class WriteAheadStorage extends WriteAheadStorageForwarder implements
    MutableStoreProvider,
    SchedulerStore.Mutable,
    CronJobStore.Mutable,
    TaskStore.Mutable,
    LockStore.Mutable,
    QuotaStore.Mutable,
    AttributeStore.Mutable,
    JobUpdateStore.Mutable {

  private final TransactionManager transactionManager;
  private final SchedulerStore.Mutable schedulerStore;
  private final CronJobStore.Mutable jobStore;
  private final TaskStore.Mutable taskStore;
  private final LockStore.Mutable lockStore;
  private final QuotaStore.Mutable quotaStore;
  private final AttributeStore.Mutable attributeStore;
  private final JobUpdateStore.Mutable jobUpdateStore;
  private final Logger log;
  private final EventSink eventSink;

  /**
   * Creates a new write-ahead storage that delegates to the providing default stores.
   *
   * @param transactionManager External controller for transaction operations.
   * @param schedulerStore Delegate.
   * @param jobStore       Delegate.
   * @param taskStore      Delegate.
   * @param lockStore      Delegate.
   * @param quotaStore     Delegate.
   * @param attributeStore Delegate.
   * @param jobUpdateStore Delegate.
   */
  WriteAheadStorage(
      TransactionManager transactionManager,
      SchedulerStore.Mutable schedulerStore,
      CronJobStore.Mutable jobStore,
      TaskStore.Mutable taskStore,
      LockStore.Mutable lockStore,
      QuotaStore.Mutable quotaStore,
      AttributeStore.Mutable attributeStore,
      JobUpdateStore.Mutable jobUpdateStore,
      Logger log,
      EventSink eventSink) {

    super(
        schedulerStore,
        jobStore,
        taskStore,
        lockStore,
        quotaStore,
        attributeStore,
        jobUpdateStore);

    this.transactionManager = requireNonNull(transactionManager);
    this.schedulerStore = requireNonNull(schedulerStore);
    this.jobStore = requireNonNull(jobStore);
    this.taskStore = requireNonNull(taskStore);
    this.lockStore = requireNonNull(lockStore);
    this.quotaStore = requireNonNull(quotaStore);
    this.attributeStore = requireNonNull(attributeStore);
    this.jobUpdateStore = requireNonNull(jobUpdateStore);
    this.log = requireNonNull(log);
    this.eventSink = requireNonNull(eventSink);
  }

  private void write(Op op) {
    Preconditions.checkState(
        transactionManager.hasActiveTransaction(),
        "Mutating operations must be within a transaction.");
    transactionManager.log(op);
  }

  @Override
  public void saveFrameworkId(final String frameworkId) {
    requireNonNull(frameworkId);

    write(Op.saveFrameworkId(SaveFrameworkId.create(frameworkId)));
    schedulerStore.saveFrameworkId(frameworkId);
  }

  @Override
  public boolean unsafeModifyInPlace(final String taskId, final TaskConfig taskConfiguration) {
    requireNonNull(taskId);
    requireNonNull(taskConfiguration);

    boolean mutated = taskStore.unsafeModifyInPlace(taskId, taskConfiguration);
    if (mutated) {
      write(Op.rewriteTask(RewriteTask.create(taskId, taskConfiguration)));
    }
    return mutated;
  }

  @Override
  public void deleteTasks(final Set<String> taskIds) {
    requireNonNull(taskIds);

    write(Op.removeTasks(RemoveTasks.create(taskIds)));
    taskStore.deleteTasks(taskIds);
  }

  @Override
  public void saveTasks(final Set<ScheduledTask> newTasks) {
    requireNonNull(newTasks);

    write(Op.saveTasks(SaveTasks.create(newTasks)));
    taskStore.saveTasks(newTasks);
  }

  @Override
  public Optional<ScheduledTask> mutateTask(
      String taskId,
      Function<ScheduledTask, ScheduledTask> mutator) {

    Optional<ScheduledTask> mutated = taskStore.mutateTask(taskId, mutator);
    log.debug("Storing updated task to log: {}={}", taskId, mutated.get().getStatus());
    write(Op.saveTasks(new SaveTasks(ImmutableSet.of(mutated.get().newBuilder()))));

    return mutated;
  }

  @Override
  public void saveQuota(final String role, final ResourceAggregate quota) {
    requireNonNull(role);
    requireNonNull(quota);

    write(Op.saveQuota(SaveQuota.create(role, quota)));
    quotaStore.saveQuota(role, quota);
  }

  @Override
  public boolean saveHostAttributes(final HostAttributes attrs) {
    requireNonNull(attrs);

    boolean changed = attributeStore.saveHostAttributes(attrs);
    if (changed) {
      write(Op.saveHostAttributes(SaveHostAttributes.create(attrs)));
      eventSink.post(new PubsubEvent.HostAttributesChanged(attrs));
    }
    return changed;
  }

  @Override
  public void removeJob(final JobKey jobKey) {
    requireNonNull(jobKey);

    write(Op.removeJob(RemoveJob.create(jobKey)));
    jobStore.removeJob(jobKey);
  }

  @Override
  public void saveAcceptedJob(final JobConfiguration jobConfig) {
    requireNonNull(jobConfig);

    write(Op.saveCronJob(SaveCronJob.create(jobConfig)));
    jobStore.saveAcceptedJob(jobConfig);
  }

  @Override
  public void removeQuota(final String role) {
    requireNonNull(role);

    write(Op.removeQuota(RemoveQuota.create(role)));
    quotaStore.removeQuota(role);
  }

  @Override
  public void saveLock(final Lock lock) {
    requireNonNull(lock);

    write(Op.saveLock(SaveLock.create(lock)));
    lockStore.saveLock(lock);
  }

  @Override
  public void removeLock(final LockKey lockKey) {
    requireNonNull(lockKey);

    write(Op.removeLock(RemoveLock.create(lockKey)));
    lockStore.removeLock(lockKey);
  }

  @Override
  public void saveJobUpdate(JobUpdate update, Optional<String> lockToken) {
    requireNonNull(update);

    write(Op.saveJobUpdate(SaveJobUpdate.create(update, lockToken.orNull())));
    jobUpdateStore.saveJobUpdate(update, lockToken);
  }

  @Override
  public void saveJobUpdateEvent(JobUpdateKey key, JobUpdateEvent event) {
    requireNonNull(key);
    requireNonNull(event);

    write(Op.saveJobUpdateEvent(SaveJobUpdateEvent.create(event, key)));
    jobUpdateStore.saveJobUpdateEvent(key, event);
  }

  @Override
  public void saveJobInstanceUpdateEvent(JobUpdateKey key, JobInstanceUpdateEvent event) {
    requireNonNull(key);
    requireNonNull(event);

    write(Op.saveJobInstanceUpdateEvent(SaveJobInstanceUpdateEvent.create(event, key)));
    jobUpdateStore.saveJobInstanceUpdateEvent(key, event);
  }

  @Override
  public Set<JobUpdateKey> pruneHistory(int perJobRetainCount, long historyPruneThresholdMs) {
    Set<JobUpdateKey> prunedUpdates = jobUpdateStore.pruneHistory(
        perJobRetainCount,
        historyPruneThresholdMs);

    if (!prunedUpdates.isEmpty()) {
      // Pruned updates will eventually go away from persisted storage when a new snapshot is cut.
      // So, persisting pruning attempts is not strictly necessary as the periodic pruner will
      // provide eventual consistency between volatile and persistent storage upon scheduler
      // restart. By generating an out of band pruning during log replay the consistency is
      // achieved sooner without potentially exposing pruned but not yet persisted data.
      write(Op.pruneJobUpdateHistory(
          PruneJobUpdateHistory.create(perJobRetainCount, historyPruneThresholdMs)));
    }
    return prunedUpdates;
  }

  @Override
  public void deleteAllTasks() {
    throw new UnsupportedOperationException(
        "Unsupported since casual storage users should never be doing this.");
  }

  @Override
  public void deleteHostAttributes() {
    throw new UnsupportedOperationException(
        "Unsupported since casual storage users should never be doing this.");
  }

  @Override
  public void deleteJobs() {
    throw new UnsupportedOperationException(
        "Unsupported since casual storage users should never be doing this.");
  }

  @Override
  public void deleteQuotas() {
    throw new UnsupportedOperationException(
        "Unsupported since casual storage users should never be doing this.");
  }

  @Override
  public void deleteLocks() {
    throw new UnsupportedOperationException(
        "Unsupported since casual storage users should never be doing this.");
  }

  @Override
  public void deleteAllUpdatesAndEvents() {
    throw new UnsupportedOperationException(
        "Unsupported since casual storage users should never be doing this.");
  }

  @Override
  public SchedulerStore.Mutable getSchedulerStore() {
    return this;
  }

  @Override
  public CronJobStore.Mutable getCronJobStore() {
    return this;
  }

  @Override
  public TaskStore.Mutable getUnsafeTaskStore() {
    return this;
  }

  @Override
  public LockStore.Mutable getLockStore() {
    return this;
  }

  @Override
  public QuotaStore.Mutable getQuotaStore() {
    return this;
  }

  @Override
  public AttributeStore.Mutable getAttributeStore() {
    return this;
  }

  @Override
  public TaskStore getTaskStore() {
    return this;
  }

  @Override
  public JobUpdateStore.Mutable getJobUpdateStore() {
    return this;
  }
}
