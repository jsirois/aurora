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
package org.apache.aurora.scheduler.thrift.aop;

import java.util.Set;

import org.apache.aurora.gen.AddInstancesConfig;
import org.apache.aurora.gen.Hosts;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.JobUpdateRequest;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.gen.LockValidation;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.RewriteConfigsRequest;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.TaskQuery;

import static java.util.Objects.requireNonNull;

/**
 * A forwarding scheduler controller to make it easy to override specific behavior in an
 * implementation class.
 */
abstract class ForwardingThrift implements AnnotatedAuroraAdmin {

  private final AnnotatedAuroraAdmin delegate;

  ForwardingThrift(AnnotatedAuroraAdmin delegate) {
    this.delegate = requireNonNull(delegate);
  }

  @Override
  public Response setQuota(
      String ownerRole,
      ResourceAggregate resourceAggregate) {

    return delegate.setQuota(ownerRole, resourceAggregate);
  }

  @Override
  public Response forceTaskState(String taskId, ScheduleStatus status) {
    return delegate.forceTaskState(taskId, status);
  }

  @Override
  public Response performBackup() {
    return delegate.performBackup();
  }

  @Override
  public Response listBackups() {
    return delegate.listBackups();
  }

  @Override
  public Response stageRecovery(String backupId) {
    return delegate.stageRecovery(backupId);
  }

  @Override
  public Response queryRecovery(TaskQuery query) {
    return delegate.queryRecovery(query);
  }

  @Override
  public Response deleteRecoveryTasks(TaskQuery query) {
    return delegate.deleteRecoveryTasks(query);
  }

  @Override
  public Response commitRecovery() {
    return delegate.commitRecovery();
  }

  @Override
  public Response unloadRecovery() {
    return delegate.unloadRecovery();
  }

  @Override
  public Response getRoleSummary() {
    return delegate.getRoleSummary();
  }

  @Override
  public Response getJobSummary(String role) {
    return delegate.getJobSummary(role);
  }

  @Override
  public Response getConfigSummary(JobKey key) {
    return delegate.getConfigSummary(key);
  }

  @Override
  public Response createJob(JobConfiguration description, Lock lock) {
    return delegate.createJob(description, lock);
  }

  @Override
  public Response scheduleCronJob(JobConfiguration description, Lock lock) {
    return delegate.scheduleCronJob(description, lock);
  }

  @Override
  public Response descheduleCronJob(JobKey job, Lock lock) {
    return delegate.descheduleCronJob(job, lock);
  }

  @Override
  public Response replaceCronTemplate(JobConfiguration config, Lock lock) {
    return delegate.replaceCronTemplate(config, lock);
  }

  @Override
  public Response populateJobConfig(JobConfiguration description) {
    return delegate.populateJobConfig(description);
  }

  @Override
  public Response startCronJob(JobKey job) {
    return delegate.startCronJob(job);
  }

  @Override
  public Response restartShards(JobKey job, Set<Integer> shardIds, Lock lock) {
    return delegate.restartShards(job, shardIds, lock);
  }

  @Override
  public Response getTasksStatus(TaskQuery query) {
    return delegate.getTasksStatus(query);
  }

  @Override
  public Response getTasksWithoutConfigs(TaskQuery query) {
    return delegate.getTasksStatus(query);
  }

  @Override
  public Response getJobs(String ownerRole) {
    return delegate.getJobs(ownerRole);
  }

  @Override
  public Response killTasks(TaskQuery query, Lock lock) {
    return delegate.killTasks(query, lock);
  }

  @Override
  public Response getQuota(String ownerRole) {
    return delegate.getQuota(ownerRole);
  }

  @Override
  public Response startMaintenance(Hosts hosts) {
    return delegate.startMaintenance(hosts);
  }

  @Override
  public Response drainHosts(Hosts hosts) {
    return delegate.drainHosts(hosts);
  }

  @Override
  public Response maintenanceStatus(Hosts hosts) {
    return delegate.maintenanceStatus(hosts);
  }

  @Override
  public Response endMaintenance(Hosts hosts) {
    return delegate.endMaintenance(hosts);
  }

  @Override
  public Response snapshot() {
    return delegate.snapshot();
  }

  @Override
  public Response rewriteConfigs(RewriteConfigsRequest request) {
    return delegate.rewriteConfigs(request);
  }

  @Override
  public Response acquireLock(LockKey lockKey) {
    return delegate.acquireLock(lockKey);
  }

  @Override
  public Response releaseLock(Lock lock, LockValidation validation) {
    return delegate.releaseLock(lock, validation);
  }

  @Override
  public Response getLocks() {
    return delegate.getLocks();
  }

  @Override
  public Response addInstances(AddInstancesConfig config, Lock lock) {
    return delegate.addInstances(config, lock);
  }

  @Override
  public Response getPendingReason(TaskQuery query) {
    return delegate.getPendingReason(query);
  }

  @Override
  public Response startJobUpdate(JobUpdateRequest request, String message) {
    return delegate.startJobUpdate(request, message);
  }

  @Override
  public Response pauseJobUpdate(JobUpdateKey key, String message) {
    return delegate.pauseJobUpdate(key, message);
  }

  @Override
  public Response resumeJobUpdate(JobUpdateKey key, String message) {
    return delegate.resumeJobUpdate(key, message);
  }

  @Override
  public Response abortJobUpdate(JobUpdateKey key, String message) {
    return delegate.abortJobUpdate(key, message);
  }

  @Override
  public Response pulseJobUpdate(JobUpdateKey key) {
    return delegate.pulseJobUpdate(key);
  }

  @Override
  public Response getJobUpdateSummaries(JobUpdateQuery updateQuery) {
    return delegate.getJobUpdateSummaries(updateQuery);
  }

  @Override
  public Response getJobUpdateDetails(JobUpdateKey key) {
    return delegate.getJobUpdateDetails(key);
  }

  @Override
  public Response getJobUpdateDiff(JobUpdateRequest request) {
    return delegate.getJobUpdateDiff(request);
  }
}
