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
package org.apache.aurora.scheduler.thrift;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.CmdLine;
import org.apache.aurora.common.args.constraints.Positive;
import org.apache.aurora.gen.AcquireLockResult;
import org.apache.aurora.gen.AddInstancesConfig;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.ConfigRewrite;
import org.apache.aurora.gen.DrainHostsResult;
import org.apache.aurora.gen.EndMaintenanceResult;
import org.apache.aurora.gen.Hosts;
import org.apache.aurora.gen.InstanceConfigRewrite;
import org.apache.aurora.gen.InstanceKey;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobConfigRewrite;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdatePulseStatus;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.JobUpdateRequest;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.ListBackupsResult;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.gen.LockValidation;
import org.apache.aurora.gen.MaintenanceStatusResult;
import org.apache.aurora.gen.PulseJobUpdateResult;
import org.apache.aurora.gen.QueryRecoveryResult;
import org.apache.aurora.gen.ReadOnlyScheduler;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.ResponseDetail;
import org.apache.aurora.gen.Result;
import org.apache.aurora.gen.RewriteConfigsRequest;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.StartJobUpdateResult;
import org.apache.aurora.gen.StartMaintenanceResult;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.scheduler.TaskIdGenerator;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Numbers;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import org.apache.aurora.scheduler.configuration.SanitizedConfiguration;
import org.apache.aurora.scheduler.cron.CronException;
import org.apache.aurora.scheduler.cron.CronJobManager;
import org.apache.aurora.scheduler.cron.SanitizedCronJob;
import org.apache.aurora.scheduler.quota.QuotaCheckResult;
import org.apache.aurora.scheduler.quota.QuotaManager;
import org.apache.aurora.scheduler.quota.QuotaManager.QuotaException;
import org.apache.aurora.scheduler.state.LockManager;
import org.apache.aurora.scheduler.state.LockManager.LockException;
import org.apache.aurora.scheduler.state.MaintenanceController;
import org.apache.aurora.scheduler.state.StateChangeResult;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.state.UUIDGenerator;
import org.apache.aurora.scheduler.storage.CronJobStore;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.Storage.NonVolatileStorage;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.backup.Recovery;
import org.apache.aurora.scheduler.storage.backup.StorageBackup;
import org.apache.aurora.scheduler.thrift.auth.DecoratedThrift;
import org.apache.aurora.scheduler.updater.JobDiff;
import org.apache.aurora.scheduler.updater.JobUpdateController;
import org.apache.aurora.scheduler.updater.JobUpdateController.AuditData;
import org.apache.aurora.scheduler.updater.UpdateStateException;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.CharMatcher.WHITESPACE;

import static org.apache.aurora.common.base.MorePreconditions.checkNotBlank;
import static org.apache.aurora.gen.ResponseCode.INVALID_REQUEST;
import static org.apache.aurora.gen.ResponseCode.LOCK_ERROR;
import static org.apache.aurora.gen.ResponseCode.OK;
import static org.apache.aurora.gen.ResponseCode.WARNING;
import static org.apache.aurora.scheduler.base.Numbers.convertRanges;
import static org.apache.aurora.scheduler.base.Numbers.toRanges;
import static org.apache.aurora.scheduler.base.Tasks.ACTIVE_STATES;
import static org.apache.aurora.scheduler.quota.QuotaCheckResult.Result.INSUFFICIENT_QUOTA;
import static org.apache.aurora.scheduler.thrift.Responses.error;
import static org.apache.aurora.scheduler.thrift.Responses.invalidRequest;
import static org.apache.aurora.scheduler.thrift.Responses.ok;

/**
 * Aurora scheduler thrift server implementation.
 * <p/>
 * Interfaces between users and the scheduler to access/modify jobs and perform cluster
 * administration tasks.
 */
@DecoratedThrift
class SchedulerThriftInterface implements AuroraAdmin.Sync {
  private static final int DEFAULT_MAX_TASKS_PER_JOB = 4000;
  private static final int DEFAULT_MAX_UPDATE_INSTANCE_FAILURES =
      DEFAULT_MAX_TASKS_PER_JOB * 5;

  @Positive
  @CmdLine(name = "max_tasks_per_job", help = "Maximum number of allowed tasks in a single job.")
  public static final Arg<Integer> MAX_TASKS_PER_JOB = Arg.create(DEFAULT_MAX_TASKS_PER_JOB);

  @Positive
  @CmdLine(name = "max_update_instance_failures", help = "Upper limit on the number of "
      + "failures allowed during a job update. This helps cap potentially unbounded entries into "
      + "storage.")
  public static final Arg<Integer> MAX_UPDATE_INSTANCE_FAILURES = Arg.create(
      DEFAULT_MAX_UPDATE_INSTANCE_FAILURES);

  // This number is derived from the maximum file name length limit on most UNIX systems, less
  // the number of characters we've observed being added by mesos for the executor ID, prefix, and
  // delimiters.
  @VisibleForTesting
  static final int MAX_TASK_ID_LENGTH = 255 - 90;

  private static final Logger LOG = Logger.getLogger(SchedulerThriftInterface.class.getName());

  private final NonVolatileStorage storage;
  private final LockManager lockManager;
  private final StorageBackup backup;
  private final Recovery recovery;
  private final MaintenanceController maintenance;
  private final CronJobManager cronJobManager;
  private final QuotaManager quotaManager;
  private final StateManager stateManager;
  private final TaskIdGenerator taskIdGenerator;
  private final UUIDGenerator uuidGenerator;
  private final JobUpdateController jobUpdateController;
  private final ReadOnlyScheduler.Sync readOnlyScheduler;
  private final AuditMessages auditMessages;

  @Inject
  SchedulerThriftInterface(
      NonVolatileStorage storage,
      LockManager lockManager,
      StorageBackup backup,
      Recovery recovery,
      CronJobManager cronJobManager,
      MaintenanceController maintenance,
      QuotaManager quotaManager,
      StateManager stateManager,
      TaskIdGenerator taskIdGenerator,
      UUIDGenerator uuidGenerator,
      JobUpdateController jobUpdateController,
      ReadOnlyScheduler.Sync readOnlyScheduler,
      AuditMessages auditMessages) {

    this.storage = requireNonNull(storage);
    this.lockManager = requireNonNull(lockManager);
    this.backup = requireNonNull(backup);
    this.recovery = requireNonNull(recovery);
    this.maintenance = requireNonNull(maintenance);
    this.cronJobManager = requireNonNull(cronJobManager);
    this.quotaManager = requireNonNull(quotaManager);
    this.stateManager = requireNonNull(stateManager);
    this.taskIdGenerator = requireNonNull(taskIdGenerator);
    this.uuidGenerator = requireNonNull(uuidGenerator);
    this.jobUpdateController = requireNonNull(jobUpdateController);
    this.readOnlyScheduler = requireNonNull(readOnlyScheduler);
    this.auditMessages = requireNonNull(auditMessages);
  }

  @Override
  public Response createJob(JobConfiguration job, @Nullable Lock lock) {
    SanitizedConfiguration sanitized;
    try {
      sanitized = SanitizedConfiguration.fromUnsanitized(job);
    } catch (TaskDescriptionException e) {
      return error(INVALID_REQUEST, e);
    }

    if (sanitized.isCron()) {
      return invalidRequest(NO_CRON);
    }

    return storage.write(new MutateWork.Quiet<Response>() {
      @Override
      public Response apply(MutableStoreProvider storeProvider) {
        JobConfiguration job = sanitized.getJobConfig();

        try {
          lockManager.validateIfLocked(
              LockKey.job(job.getKey()),
              java.util.Optional.ofNullable(lock));

          checkJobExists(storeProvider, job.getKey());

          TaskConfig template = sanitized.getJobConfig().getTaskConfig();
          int count = sanitized.getJobConfig().getInstanceCount();

          validateTaskLimits(
              template,
              count,
              quotaManager.checkInstanceAddition(template, count, storeProvider));

          LOG.info("Launching " + count + " tasks.");
          stateManager.insertPendingTasks(
              storeProvider,
              template,
              sanitized.getInstanceIds());

          return ok();
        } catch (LockException e) {
          return error(LOCK_ERROR, e);
        } catch (JobExistsException | TaskValidationException e) {
          return error(INVALID_REQUEST, e);
        }
      }
    });
  }

  private static class JobExistsException extends Exception {
    JobExistsException(String message) {
      super(message);
    }
  }

  private void checkJobExists(StoreProvider store, JobKey jobKey) throws JobExistsException {
    if (!Iterables.isEmpty(store.getTaskStore().fetchTasks(Query.jobScoped(jobKey).active()))
        || getCronJob(store, jobKey).isPresent()) {

      throw new JobExistsException(jobAlreadyExistsMessage(jobKey));
    }
  }

  private Response createOrUpdateCronTemplate(
      JobConfiguration job,
      @Nullable Lock lock,
      boolean updateOnly) {

    JobKey jobKey = JobKeys.assertValid(job.getKey());

    SanitizedConfiguration sanitized;
    try {
      sanitized = SanitizedConfiguration.fromUnsanitized(job);
    } catch (TaskDescriptionException e) {
      return error(INVALID_REQUEST, e);
    }

    if (!sanitized.isCron()) {
      return invalidRequest(noCronScheduleMessage(jobKey));
    }

    return storage.write(new MutateWork.Quiet<Response>() {
      @Override
      public Response apply(MutableStoreProvider storeProvider) {
        try {
          lockManager.validateIfLocked(
              LockKey.job(jobKey),
              java.util.Optional.ofNullable(lock));

          TaskConfig template = sanitized.getJobConfig().getTaskConfig();
          int count = sanitized.getJobConfig().getInstanceCount();

          validateTaskLimits(
              template,
              count,
              quotaManager.checkCronUpdate(sanitized.getJobConfig(), storeProvider));

          // TODO(mchucarroll): Merge CronJobManager.createJob/updateJob
          if (updateOnly || getCronJob(storeProvider, jobKey).isPresent()) {
            // The job already has a schedule: so update it.
            cronJobManager.updateJob(SanitizedCronJob.from(sanitized));
          } else {
            checkJobExists(storeProvider, jobKey);
            cronJobManager.createJob(SanitizedCronJob.from(sanitized));
          }

          return ok();
        } catch (LockException e) {
          return error(LOCK_ERROR, e);
        } catch (JobExistsException | TaskValidationException | CronException e) {
          return error(INVALID_REQUEST, e);
        }
      }
    });
  }

  @Override
  public Response scheduleCronJob(JobConfiguration job, @Nullable Lock lock) {
    return createOrUpdateCronTemplate(job, lock, false);
  }

  @Override
  public Response replaceCronTemplate(JobConfiguration job, @Nullable Lock lock) {
    return createOrUpdateCronTemplate(job, lock, true);
  }

  @Override
  public Response descheduleCronJob(JobKey jobKey, @Nullable Lock lock) {
    try {
      lockManager.validateIfLocked(
          LockKey.job(jobKey),
          java.util.Optional.ofNullable(lock));

      if (!cronJobManager.deleteJob(jobKey)) {
        return invalidRequest(notScheduledCronMessage(jobKey));
      }
      return ok();
    } catch (LockException e) {
      return error(LOCK_ERROR, e);
    }
  }

  @Override
  public Response populateJobConfig(JobConfiguration description) {
    return readOnlyScheduler.populateJobConfig(description);
  }

  @Override
  public Response startCronJob(JobKey rawJobKey) {
    JobKey jobKey = JobKeys.assertValid(rawJobKey);

    try {
      cronJobManager.startJobNow(jobKey);
      return ok();
    } catch (CronException e) {
      return invalidRequest("Failed to start cron job - " + e.getMessage());
    }
  }

  // TODO(William Farner): Provide status information about cron jobs here.
  @Override
  public Response getTasksStatus(TaskQuery query) {
    return readOnlyScheduler.getTasksStatus(query);
  }

  @Override
  public Response getTasksWithoutConfigs(TaskQuery query) {
    return readOnlyScheduler.getTasksWithoutConfigs(query);
  }

  @Override
  public Response getPendingReason(TaskQuery query) {
    return readOnlyScheduler.getPendingReason(query);
  }

  @Override
  public Response getConfigSummary(JobKey job) {
    return readOnlyScheduler.getConfigSummary(job);
  }

  @Override
  public Response getRoleSummary() {
    return readOnlyScheduler.getRoleSummary();
  }

  @Override
  public Response getJobSummary(@Nullable String maybeNullRole) {
    return readOnlyScheduler.getJobSummary(maybeNullRole);
  }

  @Override
  public Response getJobs(@Nullable String maybeNullRole) {
    return readOnlyScheduler.getJobs(maybeNullRole);
  }

  @Override
  public Response getJobUpdateDiff(JobUpdateRequest request) {
    return readOnlyScheduler.getJobUpdateDiff(request);
  }

  private void validateLockForTasks(java.util.Optional<Lock> lock, Iterable<ScheduledTask> tasks)
      throws LockException {

    ImmutableSet<JobKey> uniqueKeys = FluentIterable.from(tasks)
        .transform(Tasks::getJob)
        .toSet();

    // Validate lock against every unique job key derived from the tasks.
    for (JobKey key : uniqueKeys) {
      lockManager.validateIfLocked(LockKey.job(key), lock);
    }
  }

  private static Query.Builder implicitKillQuery(TaskQuery mutableQuery) {
    Query.Builder query = Query.arbitrary(mutableQuery);
    // Unless statuses were specifically supplied, only attempt to kill active tasks.
    return query.get().getStatuses().isEmpty() ? query.byStatus(ACTIVE_STATES) : query;
  }

  @Override
  public Response killTasks(TaskQuery query, Lock lock) {
    requireNonNull(query);

    if (query.getJobName() != null && WHITESPACE.matchesAllOf(query.getJobName())) {
      return invalidRequest(String.format("Invalid job name: '%s'", query.getJobName()));
    }

    Query.Builder killQuery = implicitKillQuery(query);
    Preconditions.checkState(
        !killQuery.get().isSetOwner(),
        "The owner field in a query should have been unset by Query.Builder.");

    return storage.write(new MutateWork.Quiet<Response>() {
      @Override
      public Response apply(MutableStoreProvider storeProvider) {
        Iterable<ScheduledTask> tasks = storeProvider.getTaskStore().fetchTasks(killQuery);
        try {
          validateLockForTasks(
              java.util.Optional.ofNullable(lock),
              tasks);
        } catch (LockException e) {
          return error(LOCK_ERROR, e);
        }

        LOG.info("Killing tasks matching " + killQuery);

        boolean tasksKilled = false;
        for (String taskId : Tasks.ids(tasks)) {
          tasksKilled |= StateChangeResult.SUCCESS == stateManager.changeState(
              storeProvider,
              taskId,
              Optional.absent(),
              ScheduleStatus.KILLING,
              auditMessages.killedByRemoteUser());
        }

        return tasksKilled
            ? ok()
            : Responses.message(OK, NO_TASKS_TO_KILL_MESSAGE);
      }
    });
  }

  @Override
  public Response restartShards(
      JobKey rawJobKey,
      Set<Integer> shardIds,
      @Nullable Lock lock) {

    JobKey jobKey = JobKeys.assertValid(rawJobKey);
    checkNotBlank(shardIds);

    return storage.write(new MutateWork.Quiet<Response>() {
      @Override
      public Response apply(MutableStoreProvider storeProvider) {
        try {
          lockManager.validateIfLocked(
              LockKey.job(jobKey),
              java.util.Optional.ofNullable(lock));
        } catch (LockException e) {
          return error(LOCK_ERROR, e);
        }

        Query.Builder query = Query.instanceScoped(jobKey, shardIds).active();
        Iterable<ScheduledTask> matchingTasks = storeProvider.getTaskStore().fetchTasks(query);
        if (Iterables.size(matchingTasks) != shardIds.size()) {
          return invalidRequest("Not all requested shards are active.");
        }

        LOG.info("Restarting shards matching " + query);
        storage.write(new MutateWork.NoResult.Quiet() {
          @Override
          public void execute(MutableStoreProvider storeProvider) {
            for (String taskId : Tasks.ids(matchingTasks)) {
              stateManager.changeState(
                  storeProvider,
                  taskId,
                  Optional.absent(),
                  ScheduleStatus.RESTARTING,
                  auditMessages.restartedByRemoteUser());
            }
          }
        });
        return ok();
      }
    });
  }

  @Override
  public Response getQuota(String ownerRole) {
    return readOnlyScheduler.getQuota(ownerRole);
  }

  @Override
  public Response setQuota(String ownerRole, ResourceAggregate resourceAggregate) {
    checkNotBlank(ownerRole);
    requireNonNull(resourceAggregate);

    try {
      storage.write(new MutateWork.NoResult<QuotaException>() {
        @Override
        public void execute(MutableStoreProvider store) throws QuotaException {
          quotaManager.saveQuota(
              ownerRole,
              resourceAggregate,
              store);
        }
      });
      return ok();
    } catch (QuotaException e) {
      return error(INVALID_REQUEST, e);
    }
  }

  @Override
  public Response startMaintenance(Hosts hosts) {
    return ok(Result.startMaintenanceResult(
        StartMaintenanceResult.create(maintenance.startMaintenance(hosts.getHostNames()))));
  }

  @Override
  public Response drainHosts(Hosts hosts) {
    return ok(Result.drainHostsResult(
        DrainHostsResult.create(maintenance.drain(hosts.getHostNames()))));
  }

  @Override
  public Response maintenanceStatus(Hosts hosts) {
    return ok(Result.maintenanceStatusResult(
        MaintenanceStatusResult.create(maintenance.getStatus(hosts.getHostNames()))));
  }

  @Override
  public Response endMaintenance(Hosts hosts) {
    return ok(Result.endMaintenanceResult(
        EndMaintenanceResult.create(maintenance.endMaintenance(hosts.getHostNames()))));
  }

  @Override
  public Response forceTaskState(String taskId, ScheduleStatus status) {
    checkNotBlank(taskId);
    requireNonNull(status);

    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        stateManager.changeState(
            storeProvider,
            taskId,
            Optional.absent(),
            status,
            auditMessages.transitionedBy());
      }
    });

    return ok();
  }

  @Override
  public Response performBackup() {
    backup.backupNow();
    return ok();
  }

  @Override
  public Response listBackups() {
    return ok(Result.listBackupsResult(ListBackupsResult.create(recovery.listBackups())));
  }

  @Override
  public Response stageRecovery(String backupId) {
    recovery.stage(backupId);
    return ok();
  }

  @Override
  public Response queryRecovery(TaskQuery query) {
    return ok(Result.queryRecoveryResult(QueryRecoveryResult.builder()
        .setTasks(recovery.query(Query.arbitrary(query)))
        .build()));
  }

  @Override
  public Response deleteRecoveryTasks(TaskQuery query) {
    recovery.deleteTasks(Query.arbitrary(query));
    return ok();
  }

  @Override
  public Response commitRecovery() {
    recovery.commit();
    return ok();
  }

  @Override
  public Response unloadRecovery() {
    recovery.unload();
    return ok();
  }

  @Override
  public Response snapshot() {
    storage.snapshot();
    return ok();
  }

  @Override
  public Response rewriteConfigs(RewriteConfigsRequest request) {
    if (request.getRewriteCommands().isEmpty()) {
      return Responses.message(INVALID_REQUEST, "No rewrite commands provided.");
    }

    return storage.write(new MutateWork.Quiet<Response>() {
      @Override
      public Response apply(MutableStoreProvider storeProvider) {
        List<String> errors = Lists.newArrayList();

        for (ConfigRewrite command : request.getRewriteCommands()) {
          Optional<String> error = rewriteConfig(command, storeProvider);
          if (error.isPresent()) {
            errors.add(error.get());
          }
        }

        if (errors.isEmpty()) {
          return Responses.ok();
        } else {
          return Response.builder()
              .setResponseCode(WARNING)
              .setDetails(errors.stream().map(ResponseDetail::create).collect(Collectors.toList()))
              .build();
        }
      }
    });
  }

  private Optional<String> rewriteJob(JobConfigRewrite jobRewrite, CronJobStore.Mutable jobStore) {
    JobConfiguration existingJob = jobRewrite.getOldJob();
    JobConfiguration rewrittenJob;
    Optional<String> error = Optional.absent();
    try {
      rewrittenJob = ConfigurationManager.validateAndPopulate(jobRewrite.getRewrittenJob());
    } catch (TaskDescriptionException e) {
      // We could add an error here, but this is probably a hint of something wrong in
      // the client that's causing a bad configuration to be applied.
      throw new RuntimeException(e);
    }

    if (existingJob.getKey().equals(rewrittenJob.getKey())) {
      Optional<JobConfiguration> job = jobStore.fetchJob(existingJob.getKey());
      if (job.isPresent()) {
        JobConfiguration storedJob = job.get();
        if (storedJob.equals(existingJob)) {
          jobStore.saveAcceptedJob(rewrittenJob);
        } else {
          error = Optional.of(
              "CAS compare failed for " + JobKeys.canonicalString(storedJob.getKey()));
        }
      } else {
        error = Optional.of(
            "No jobs found for key " + JobKeys.canonicalString(existingJob.getKey()));
      }
    } else {
      error = Optional.of("Disallowing rewrite attempting to change job key.");
    }

    return error;
  }

  private Optional<String> rewriteInstance(
      InstanceConfigRewrite instanceRewrite,
      MutableStoreProvider storeProvider) {

    InstanceKey instanceKey = instanceRewrite.getInstanceKey();
    Optional<String> error = Optional.absent();
    Iterable<ScheduledTask> tasks = storeProvider.getTaskStore().fetchTasks(
        Query.instanceScoped(instanceKey.getJobKey(),
            instanceKey.getInstanceId())
            .active());
    Optional<AssignedTask> task =
        Optional.fromNullable(Iterables.getOnlyElement(tasks, null))
            .transform(ScheduledTask::getAssignedTask);

    if (task.isPresent()) {
      if (task.get().getTask().equals(instanceRewrite.getOldTask())) {
        TaskConfig newConfiguration = instanceRewrite.getRewrittenTask();
        boolean changed = storeProvider.getUnsafeTaskStore().unsafeModifyInPlace(
            task.get().getTaskId(), newConfiguration);
        if (!changed) {
          error = Optional.of("Did not change " + task.get().getTaskId());
        }
      } else {
        error = Optional.of("CAS compare failed for " + instanceKey);
      }
    } else {
      error = Optional.of("No active task found for " + instanceKey);
    }

    return error;
  }

  private Optional<String> rewriteConfig(
      ConfigRewrite command,
      MutableStoreProvider storeProvider) {

    Optional<String> error;
    switch (command.getSetField()) {
      case JOB_REWRITE:
        error = rewriteJob(command.getJobRewrite(), storeProvider.getCronJobStore());
        break;

      case INSTANCE_REWRITE:
        error = rewriteInstance(command.getInstanceRewrite(), storeProvider);
        break;

      default:
        throw new IllegalArgumentException("Unhandled command type " + command.getSetField());
    }

    return error;
  }

  @Override
  public Response addInstances(AddInstancesConfig config, @Nullable Lock lock) {
    requireNonNull(config);
    checkNotBlank(config.getInstanceIds());
    JobKey jobKey = JobKeys.assertValid(config.getKey());

    TaskConfig task;
    try {
      task = ConfigurationManager.validateAndPopulate(config.getTaskConfig());
    } catch (TaskDescriptionException e) {
      return error(INVALID_REQUEST, e);
    }

    return storage.write(new MutateWork.Quiet<Response>() {
      @Override
      public Response apply(MutableStoreProvider storeProvider) {
        try {
          if (getCronJob(storeProvider, jobKey).isPresent()) {
            return invalidRequest("Instances may not be added to cron jobs.");
          }

          lockManager.validateIfLocked(
              LockKey.job(jobKey),
              java.util.Optional.ofNullable(lock));

          Iterable<ScheduledTask> currentTasks = storeProvider.getTaskStore().fetchTasks(
              Query.jobScoped(task.getJob()).active());

          validateTaskLimits(
              task,
              Iterables.size(currentTasks) + config.getInstanceIds().size(),
              quotaManager.checkInstanceAddition(
                  task, config.getInstanceIds().size(), storeProvider));

          storage.write(new NoResult.Quiet() {
            @Override
            public void execute(MutableStoreProvider storeProvider) {
              stateManager.insertPendingTasks(
                  storeProvider,
                  task,
                  ImmutableSet.copyOf(config.getInstanceIds()));
            }
          });

          return ok();
        } catch (LockException e) {
          return error(LOCK_ERROR, e);
        } catch (TaskValidationException | IllegalArgumentException e) {
          return error(INVALID_REQUEST, e);
        }
      }
    });
  }

  public Optional<JobConfiguration> getCronJob(StoreProvider storeProvider, JobKey jobKey) {
    requireNonNull(jobKey);
    return storeProvider.getCronJobStore().fetchJob(jobKey);
  }

  @Override
  public Response acquireLock(LockKey lockKey) {
    requireNonNull(lockKey);

    try {
      Lock lock = lockManager.acquireLock(lockKey, auditMessages.getRemoteUserName());
      return ok(Result.acquireLockResult(AcquireLockResult.create(lock)));
    } catch (LockException e) {
      return error(LOCK_ERROR, e);
    }
  }

  @Override
  public Response releaseLock(Lock lock, LockValidation validation) {
    requireNonNull(lock);
    requireNonNull(validation);

    try {
      if (validation == LockValidation.CHECKED) {
        lockManager.validateIfLocked(lock.getKey(), java.util.Optional.of(lock));
      }
      lockManager.releaseLock(lock);
      return ok();
    } catch (LockException e) {
      return error(LOCK_ERROR, e);
    }
  }

  @Override
  public Response getLocks() {
    return readOnlyScheduler.getLocks();
  }

  private static class TaskValidationException extends Exception {
    TaskValidationException(String message) {
      super(message);
    }
  }

  private void validateTaskLimits(
      TaskConfig task,
      int totalInstances,
      QuotaCheckResult quotaCheck) throws TaskValidationException {

    if (totalInstances <= 0 || totalInstances > MAX_TASKS_PER_JOB.get()) {
      throw new TaskValidationException(String.format(
          "Instance count must be between 1 and %d inclusive.",
          MAX_TASKS_PER_JOB.get()));
    }

    // TODO(maximk): This is a short-term hack to stop the bleeding from
    //               https://issues.apache.org/jira/browse/MESOS-691
    if (taskIdGenerator.generate(task, totalInstances).length() > MAX_TASK_ID_LENGTH) {
      throw new TaskValidationException(
          "Task ID is too long, please shorten your role or job name.");
    }

    if (quotaCheck.getResult() == INSUFFICIENT_QUOTA) {
      throw new TaskValidationException("Insufficient resource quota: "
          + quotaCheck.getDetails().or(""));
    }
  }

  private static Set<InstanceTaskConfig> buildInitialState(Map<Integer, TaskConfig> tasks) {
    // Translate tasks into instance IDs.
    Multimap<TaskConfig, Integer> instancesByConfig = HashMultimap.create();
    Multimaps.invertFrom(Multimaps.forMap(tasks), instancesByConfig);

    // Reduce instance IDs into contiguous ranges.
    Map<TaskConfig, Set<com.google.common.collect.Range<Integer>>> rangesByConfig =
        Maps.transformValues(instancesByConfig.asMap(), Numbers::toRanges);

    ImmutableSet.Builder<InstanceTaskConfig> builder = ImmutableSet.builder();
    for (Map.Entry<TaskConfig, Set<com.google.common.collect.Range<Integer>>> entry
        : rangesByConfig.entrySet()) {
      builder.add(InstanceTaskConfig.builder()
          .setTask(entry.getKey())
          .setInstances(convertRanges(entry.getValue()))
          .build());
    }

    return builder.build();
  }

  @Override
  public Response startJobUpdate(JobUpdateRequest rawRequest, @Nullable String message) {
    requireNonNull(rawRequest);

    // TODO(maxim): Switch to key field instead when AURORA-749 is fixed.
    JobKey job = JobKeys.assertValid(JobKey.builder()
        .setRole(rawRequest.getTaskConfig().getOwner().getRole())
        .setEnvironment(rawRequest.getTaskConfig().getEnvironment())
        .setName(rawRequest.getTaskConfig().getJobName())
        .build());

    if (!rawRequest.getTaskConfig().isIsService()) {
      return invalidRequest(NON_SERVICE_TASK);
    }

    JobUpdateSettings settings = requireNonNull(rawRequest.getSettings());
    if (settings.getUpdateGroupSize() <= 0) {
      return invalidRequest(INVALID_GROUP_SIZE);
    }

    if (settings.getMaxPerInstanceFailures() < 0) {
      return invalidRequest(INVALID_MAX_INSTANCE_FAILURES);
    }

    if (settings.getMaxFailedInstances() < 0) {
      return invalidRequest(INVALID_MAX_FAILED_INSTANCES);
    }

    if (settings.getMaxPerInstanceFailures() * rawRequest.getInstanceCount()
            > MAX_UPDATE_INSTANCE_FAILURES.get()) {
      return invalidRequest(TOO_MANY_POTENTIAL_FAILED_INSTANCES);
    }

    if (settings.getMinWaitInInstanceRunningMs() < 0) {
      return invalidRequest(INVALID_MIN_WAIT_TO_RUNNING);
    }

    if (settings.getBlockIfNoPulsesAfterMs() < 0) {
      return invalidRequest(INVALID_PULSE_TIMEOUT);
    }

    JobUpdateRequest request;
    try {
      request = rawRequest.toBuilder().setTaskConfig(
          ConfigurationManager.validateAndPopulate(rawRequest.getTaskConfig())).build();

    } catch (TaskDescriptionException e) {
      return error(INVALID_REQUEST, e);
    }

    return storage.write(new MutateWork.Quiet<Response>() {
      @Override
      public Response apply(MutableStoreProvider storeProvider) {
        if (getCronJob(storeProvider, job).isPresent()) {
          return invalidRequest(NO_CRON);
        }

        String updateId = uuidGenerator.createNew().toString();
        JobUpdateSettings settings = request.getSettings();

        JobDiff diff = JobDiff.compute(
            storeProvider.getTaskStore(),
            job,
            JobDiff.asMap(request.getTaskConfig(), request.getInstanceCount()),
            settings.getUpdateOnlyTheseInstances());

        Set<Integer> invalidScope = diff.getOutOfScopeInstances(
            Numbers.rangesToInstanceIds(settings.getUpdateOnlyTheseInstances()));
        if (!invalidScope.isEmpty()) {
          return invalidRequest(
              "The update request attempted to update specific instances,"
                  + " but some are irrelevant to the update and current job state: "
                  + invalidScope);
        }

        if (diff.isNoop()) {
          return Responses.message(OK, NOOP_JOB_UPDATE_MESSAGE);
        }

        JobUpdateInstructions.Builder instructions = JobUpdateInstructions.builder()
            .setSettings(settings)
            .setInitialState(buildInitialState(diff.getReplacedInstances()));

        Set<Integer> replacements = diff.getReplacementInstances();
        if (!replacements.isEmpty()) {
          instructions.setDesiredState(
              InstanceTaskConfig.builder()
                  .setTask(request.getTaskConfig())
                  .setInstances(convertRanges(toRanges(replacements)))
                  .build());
        }

        String remoteUserName = auditMessages.getRemoteUserName();
        JobUpdate update = JobUpdate.builder()
            .setSummary(JobUpdateSummary.builder()
                .setKey(JobUpdateKey.create(job, updateId))
                .setUser(remoteUserName)
                .build())
            .setInstructions(instructions.build())
            .build();
        try {
          validateTaskLimits(
              request.getTaskConfig(),
              request.getInstanceCount(),
              quotaManager.checkJobUpdate(update, storeProvider));

          jobUpdateController.start(
              update,
              new AuditData(remoteUserName, Optional.fromNullable(message)));
          return ok(Result.startJobUpdateResult(
              StartJobUpdateResult.create(update.getSummary().getKey())));
        } catch (UpdateStateException | TaskValidationException e) {
          return error(INVALID_REQUEST, e);
        }
      }
    });
  }

  private Response changeJobUpdateState(
      JobUpdateKey key,
      JobUpdateStateChange change,
      Optional<String> message) {

    JobKeys.assertValid(key.getJob());
    return storage.write(new MutateWork.Quiet<Response>() {
      @Override
      public Response apply(MutableStoreProvider storeProvider) {
        try {
          change.modifyUpdate(
              jobUpdateController,
              key,
              new AuditData(auditMessages.getRemoteUserName(), message));
          return ok();
        } catch (UpdateStateException e) {
          return error(INVALID_REQUEST, e);
        }
      }
    });
  }

  private interface JobUpdateStateChange {
    void modifyUpdate(JobUpdateController controller, JobUpdateKey key, AuditData auditData)
        throws UpdateStateException;
  }

  @Override
  public Response pauseJobUpdate(JobUpdateKey mutableKey, @Nullable String message) {
    return changeJobUpdateState(
        mutableKey,
        JobUpdateController::pause,
        Optional.fromNullable(message));
  }

  @Override
  public Response resumeJobUpdate(JobUpdateKey mutableKey, @Nullable String message) {
    return changeJobUpdateState(
        mutableKey,
        JobUpdateController::resume,
        Optional.fromNullable(message));
  }

  @Override
  public Response abortJobUpdate(JobUpdateKey mutableKey, @Nullable String message) {
    return changeJobUpdateState(
        mutableKey,
        JobUpdateController::abort,
        Optional.fromNullable(message));
  }

  @Override
  public Response pulseJobUpdate(JobUpdateKey rawUpdateKey) {
    JobUpdateKey updateKey = validateJobUpdateKey(rawUpdateKey);
    try {
      JobUpdatePulseStatus result = jobUpdateController.pulse(updateKey);
      return ok(Result.pulseJobUpdateResult(PulseJobUpdateResult.create(result)));
    } catch (UpdateStateException e) {
      return error(INVALID_REQUEST, e);
    }
  }

  @Override
  public Response getJobUpdateSummaries(JobUpdateQuery mutableQuery) {
    return readOnlyScheduler.getJobUpdateSummaries(mutableQuery);
  }

  @Override
  public Response getJobUpdateDetails(JobUpdateKey key) {
    return readOnlyScheduler.getJobUpdateDetails(key);
  }

  private static JobUpdateKey validateJobUpdateKey(JobUpdateKey key) {
    JobKeys.assertValid(key.getJob());
    checkNotBlank(key.getId());
    return key;
  }

  @VisibleForTesting
  static String noCronScheduleMessage(JobKey jobKey) {
    return String.format("Job %s has no cron schedule", JobKeys.canonicalString(jobKey));
  }

  @VisibleForTesting
  static String notScheduledCronMessage(JobKey jobKey) {
    return String.format("Job %s is not scheduled with cron", JobKeys.canonicalString(jobKey));
  }

  @VisibleForTesting
  static String jobAlreadyExistsMessage(JobKey jobKey) {
    return String.format("Job %s already exists", JobKeys.canonicalString(jobKey));
  }

  @VisibleForTesting
  static final String NO_TASKS_TO_KILL_MESSAGE = "No tasks to kill.";

  @VisibleForTesting
  static final String NOOP_JOB_UPDATE_MESSAGE = "Job is unchanged by proposed update.";

  @VisibleForTesting
  static final String NO_CRON = "Cron jobs may only be created/updated by calling scheduleCronJob.";

  @VisibleForTesting
  static final String NON_SERVICE_TASK = "Updates are not supported for non-service tasks.";

  @VisibleForTesting
  static final String INVALID_GROUP_SIZE = "updateGroupSize must be positive.";

  @VisibleForTesting
  static final String INVALID_MAX_FAILED_INSTANCES = "maxFailedInstances must be non-negative.";

  @VisibleForTesting
  static final String TOO_MANY_POTENTIAL_FAILED_INSTANCES = "Your update allows too many failures "
      + "to occur, consider decreasing the per-instance failures or maxFailedInstances.";

  @VisibleForTesting
  static final String INVALID_MAX_INSTANCE_FAILURES
      = "maxPerInstanceFailures must be non-negative.";

  @VisibleForTesting
  static final String INVALID_MIN_WAIT_TO_RUNNING =
      "minWaitInInstanceRunningMs must be non-negative.";

  @VisibleForTesting
  static final String INVALID_PULSE_TIMEOUT = "blockIfNoPulsesAfterMs must be positive.";
}
