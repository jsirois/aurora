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

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;

import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.storage.StoredJobUpdateDetails;
import org.apache.aurora.scheduler.base.Query;

import static java.util.Objects.requireNonNull;

/**
 * A store that forwards all its operations to underlying storage systems.  Useful for decorating
 * an existing storage system.
 */
public class ForwardingStore implements
    SchedulerStore,
    CronJobStore,
    TaskStore,
    LockStore,
    QuotaStore,
    AttributeStore,
    JobUpdateStore {

  private final SchedulerStore schedulerStore;
  private final CronJobStore cronJobStore;
  private final TaskStore taskStore;
  private final LockStore lockStore;
  private final QuotaStore quotaStore;
  private final AttributeStore attributeStore;
  private final JobUpdateStore jobUpdateStore;

  /**
   * Creates a new forwarding store that delegates to the providing default stores.
   *
   * @param schedulerStore Delegate.
   * @param cronJobStore Delegate.
   * @param taskStore Delegate.
   * @param lockStore Delegate.
   * @param quotaStore Delegate.
   * @param attributeStore Delegate.
   * @param jobUpdateStore Delegate.
   */
  public ForwardingStore(
      SchedulerStore schedulerStore,
      CronJobStore cronJobStore,
      TaskStore taskStore,
      LockStore lockStore,
      QuotaStore quotaStore,
      AttributeStore attributeStore,
      JobUpdateStore jobUpdateStore) {

    this.schedulerStore = requireNonNull(schedulerStore);
    this.cronJobStore = requireNonNull(cronJobStore);
    this.taskStore = requireNonNull(taskStore);
    this.lockStore = requireNonNull(lockStore);
    this.quotaStore = requireNonNull(quotaStore);
    this.attributeStore = requireNonNull(attributeStore);
    this.jobUpdateStore = requireNonNull(jobUpdateStore);
  }

  @Override
  public Optional<String> fetchFrameworkId() {
    return schedulerStore.fetchFrameworkId();
  }

  @Override
  public Iterable<JobConfiguration> fetchJobs() {
    return cronJobStore.fetchJobs();
  }

  @Override
  public Optional<JobConfiguration> fetchJob(JobKey jobKey) {
    return cronJobStore.fetchJob(jobKey);
  }

  @Override
  public Iterable<ScheduledTask> fetchTasks(Query.Builder querySupplier) {
    return taskStore.fetchTasks(querySupplier);
  }

  @Override
  public Set<JobKey> getJobKeys() {
    return taskStore.getJobKeys();
  }

  @Override
  public Set<Lock> fetchLocks() {
    return lockStore.fetchLocks();
  }

  @Override
  public java.util.Optional<Lock> fetchLock(LockKey lockKey) {
    return lockStore.fetchLock(lockKey);
  }

  @Override
  public Map<String, ResourceAggregate> fetchQuotas() {
    return quotaStore.fetchQuotas();
  }

  @Override
  public Optional<ResourceAggregate> fetchQuota(String role) {
    return quotaStore.fetchQuota(role);
  }

  @Override
  public Optional<HostAttributes> getHostAttributes(String host) {
    return attributeStore.getHostAttributes(host);
  }

  @Override
  public Set<HostAttributes> getHostAttributes() {
    return attributeStore.getHostAttributes();
  }

  @Override
  public List<JobUpdateSummary> fetchJobUpdateSummaries(JobUpdateQuery query) {
    return jobUpdateStore.fetchJobUpdateSummaries(query);
  }

  @Override
  public Optional<JobUpdateDetails> fetchJobUpdateDetails(JobUpdateKey key) {
    return jobUpdateStore.fetchJobUpdateDetails(key);
  }

  @Override
  public List<JobUpdateDetails> fetchJobUpdateDetails(JobUpdateQuery query) {
    return jobUpdateStore.fetchJobUpdateDetails(query);
  }

  @Override
  public Optional<JobUpdate> fetchJobUpdate(JobUpdateKey key) {
    return jobUpdateStore.fetchJobUpdate(key);
  }

  @Override
  public Optional<JobUpdateInstructions> fetchJobUpdateInstructions(JobUpdateKey key) {
    return jobUpdateStore.fetchJobUpdateInstructions(key);
  }

  @Override
  public Set<StoredJobUpdateDetails> fetchAllJobUpdateDetails() {
    return jobUpdateStore.fetchAllJobUpdateDetails();
  }

  @Override
  public Optional<String> getLockToken(JobUpdateKey key) {
    return jobUpdateStore.getLockToken(key);
  }

  @Override
  public List<JobInstanceUpdateEvent> fetchInstanceEvents(JobUpdateKey key, int instanceId) {
    return jobUpdateStore.fetchInstanceEvents(key, instanceId);
  }
}
