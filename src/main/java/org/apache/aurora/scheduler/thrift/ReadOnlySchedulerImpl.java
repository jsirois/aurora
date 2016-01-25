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

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nullable;
import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

import org.apache.aurora.common.base.MorePreconditions;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.ConfigGroup;
import org.apache.aurora.gen.ConfigSummary;
import org.apache.aurora.gen.ConfigSummaryResult;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.GetJobUpdateDetailsResult;
import org.apache.aurora.gen.GetJobUpdateDiffResult;
import org.apache.aurora.gen.GetJobUpdateSummariesResult;
import org.apache.aurora.gen.GetJobsResult;
import org.apache.aurora.gen.GetLocksResult;
import org.apache.aurora.gen.GetPendingReasonResult;
import org.apache.aurora.gen.GetQuotaResult;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobSummary;
import org.apache.aurora.gen.JobSummaryResult;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.JobUpdateRequest;
import org.apache.aurora.gen.PendingReason;
import org.apache.aurora.gen.PopulateJobResult;
import org.apache.aurora.gen.ReadOnlyScheduler;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.Result;
import org.apache.aurora.gen.RoleSummary;
import org.apache.aurora.gen.RoleSummaryResult;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduleStatusResult;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Jobs;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import org.apache.aurora.scheduler.configuration.SanitizedConfiguration;
import org.apache.aurora.scheduler.cron.CronPredictor;
import org.apache.aurora.scheduler.cron.CrontabEntry;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.metadata.NearestFit;
import org.apache.aurora.scheduler.quota.QuotaInfo;
import org.apache.aurora.scheduler.quota.QuotaManager;
import org.apache.aurora.scheduler.state.LockManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.updater.JobDiff;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.gen.ResponseCode.INVALID_REQUEST;
import static org.apache.aurora.scheduler.base.Numbers.convertRanges;
import static org.apache.aurora.scheduler.base.Numbers.toRanges;
import static org.apache.aurora.scheduler.thrift.Responses.error;
import static org.apache.aurora.scheduler.thrift.Responses.invalidRequest;
import static org.apache.aurora.scheduler.thrift.Responses.ok;

class ReadOnlySchedulerImpl implements ReadOnlyScheduler.Sync {
  private static final Function<Entry<TaskConfig, Collection<Integer>>, ConfigGroup> TO_GROUP =
      input -> ConfigGroup.builder()
          .setConfig(input.getKey())
          .setInstances(convertRanges(toRanges(input.getValue())))
          .build();

  private final ConfigurationManager configurationManager;
  private final Storage storage;
  private final NearestFit nearestFit;
  private final CronPredictor cronPredictor;
  private final QuotaManager quotaManager;
  private final LockManager lockManager;

  @Inject
  ReadOnlySchedulerImpl(
      ConfigurationManager configurationManager,
      Storage storage,
      NearestFit nearestFit,
      CronPredictor cronPredictor,
      QuotaManager quotaManager,
      LockManager lockManager) {

    this.configurationManager = requireNonNull(configurationManager);
    this.storage = requireNonNull(storage);
    this.nearestFit = requireNonNull(nearestFit);
    this.cronPredictor = requireNonNull(cronPredictor);
    this.quotaManager = requireNonNull(quotaManager);
    this.lockManager = requireNonNull(lockManager);
  }

  @Override
  public Response populateJobConfig(JobConfiguration description) {
    requireNonNull(description);

    try {
      TaskConfig populatedTaskConfig = SanitizedConfiguration.fromUnsanitized(
          configurationManager,
          description).getJobConfig().getTaskConfig();
      return ok(Result.populateJobResult(PopulateJobResult.create(populatedTaskConfig)));
    } catch (TaskDescriptionException e) {
      return invalidRequest("Invalid configuration: " + e.getMessage());
    }
  }

  // TODO(William Farner): Provide status information about cron jobs here.
  @Override
  public Response getTasksStatus(TaskQuery query) {
    return ok(Result.scheduleStatusResult(ScheduleStatusResult.create(getTasks(query))));
  }

  @Override
  public Response getTasksWithoutConfigs(TaskQuery query) {
    List<ScheduledTask> tasks = Lists.transform(
        getTasks(query),
        task -> task.withAssignedTask(
            at -> at.withTask(
                tc -> tc.withExecutorConfig((ExecutorConfig) null))));

    return ok(Result.scheduleStatusResult(ScheduleStatusResult.create(tasks)));
  }

  @Override
  public Response getPendingReason(TaskQuery query) {
    requireNonNull(query);

    if (!query.getSlaveHosts().isEmpty() || !query.getStatuses().isEmpty()) {
      return invalidRequest(
          "Statuses or slaveHosts are not supported in " + query.toString());
    }

    // Only PENDING tasks should be considered.
    TaskQuery pendingQuery = query.withStatuses(ImmutableSet.of(ScheduleStatus.PENDING));

    Set<PendingReason> pendingReasons = FluentIterable.from(getTasks(pendingQuery))
        .transform(scheduledTask -> {
          TaskGroupKey groupKey = TaskGroupKey.from(scheduledTask.getAssignedTask().getTask());

          String reason = Joiner.on(',').join(Iterables.transform(
              nearestFit.getNearestFit(groupKey),
              Veto::getReason));

          return PendingReason.builder()
              .setTaskId(scheduledTask.getAssignedTask().getTaskId())
              .setReason(reason)
              .build();
        }).toSet();

    return ok(Result.getPendingReasonResult(GetPendingReasonResult.create(pendingReasons)));
  }

  @Override
  public Response getConfigSummary(JobKey jobKey) {
    JobKeys.assertValid(jobKey);

    Iterable<AssignedTask> assignedTasks = Iterables.transform(
        Storage.Util.fetchTasks(storage, Query.jobScoped(jobKey).active()),
        ScheduledTask::getAssignedTask);
    Map<Integer, TaskConfig> tasksByInstance = Maps.transformValues(
        Maps.uniqueIndex(assignedTasks, AssignedTask::getInstanceId),
        AssignedTask::getTask);
    Set<ConfigGroup> groups = instancesToConfigGroups(tasksByInstance);

    return ok(Result.configSummaryResult(
        ConfigSummaryResult.create(ConfigSummary.create(jobKey, groups))));
  }

  @Override
  public Response getRoleSummary() {
    Multimap<String, JobKey> jobsByRole = storage.read(
        storeProvider ->
            Multimaps.index(storeProvider.getTaskStore().getJobKeys(), JobKey::getRole));

    Multimap<String, JobKey> cronJobsByRole = Multimaps.index(
        Iterables.transform(Storage.Util.fetchCronJobs(storage), JobConfiguration::getKey),
        JobKey::getRole);

    Set<RoleSummary> summaries = FluentIterable.from(
        Sets.union(jobsByRole.keySet(), cronJobsByRole.keySet()))
        .transform(role -> RoleSummary.create(
            role,
            jobsByRole.get(role).size(),
            cronJobsByRole.get(role).size()))
        .toSet();

    return ok(Result.roleSummaryResult(RoleSummaryResult.create(summaries)));
  }

  @Override
  public Response getJobSummary(@Nullable String maybeNullRole) {
    Optional<String> ownerRole = Optional.fromNullable(maybeNullRole);

    Multimap<JobKey, ScheduledTask> tasks = getTasks(maybeRoleScoped(ownerRole));
    Map<JobKey, JobConfiguration> jobs = getJobs(ownerRole, tasks);

    Function<JobKey, JobSummary> makeJobSummary = jobKey -> {
      JobConfiguration job = jobs.get(jobKey);
      JobSummary summary = JobSummary.create(job, Jobs.getJobStats(tasks.get(jobKey)));
      if (job.isSetCronSchedule()) {
        CrontabEntry crontabEntry = CrontabEntry.parse(job.getCronSchedule());
        Optional<Date> nextRun = cronPredictor.predictNextRun(crontabEntry);
        return nextRun.transform(date -> summary.withNextCronRunMs(date.getTime())).or(summary);
      } else {
        return summary;
      }
    };

    ImmutableSet<JobSummary> jobSummaries =
        FluentIterable.from(jobs.keySet()).transform(makeJobSummary).toSet();

    return ok(Result.jobSummaryResult(JobSummaryResult.create(jobSummaries)));
  }

  @Override
  public Response getJobs(@Nullable String maybeNullRole) {
    Optional<String> ownerRole = Optional.fromNullable(maybeNullRole);

    return ok(Result.getJobsResult(
        GetJobsResult.create(
            ImmutableSet.copyOf(
                getJobs(ownerRole, getTasks(maybeRoleScoped(ownerRole).active())).values()))));
  }

  @Override
  public Response getQuota(String ownerRole) {
    MorePreconditions.checkNotBlank(ownerRole);
    return storage.read(storeProvider -> {
      QuotaInfo quotaInfo = quotaManager.getQuotaInfo(ownerRole, storeProvider);
      GetQuotaResult result = GetQuotaResult.builder()
          .setQuota(quotaInfo.getQuota())
          .setProdSharedConsumption(quotaInfo.getProdSharedConsumption())
          .setProdDedicatedConsumption(quotaInfo.getProdDedicatedConsumption())
          .setNonProdSharedConsumption(quotaInfo.getNonProdSharedConsumption())
          .setNonProdDedicatedConsumption(quotaInfo.getNonProdDedicatedConsumption())
          .build();

      return ok(Result.getQuotaResult(result));
    });
  }

  @Override
  public Response getLocks() {
    return ok(Result.getLocksResult(
        GetLocksResult.create(ImmutableSet.copyOf(lockManager.getLocks()))));
  }

  @Override
  public Response getJobUpdateSummaries(JobUpdateQuery query) {
    requireNonNull(query);
    return ok(Result.getJobUpdateSummariesResult(
        GetJobUpdateSummariesResult.create(
            storage.read(
                storeProvider ->
                    storeProvider.getJobUpdateStore().fetchJobUpdateSummaries(query)))));
  }

  @Override
  public Response getJobUpdateDetails(JobUpdateKey key) {
    Optional<JobUpdateDetails> details =
        storage.read(storeProvider -> storeProvider.getJobUpdateStore().fetchJobUpdateDetails(key));

    if (details.isPresent()) {
      return ok(Result.getJobUpdateDetailsResult(GetJobUpdateDetailsResult.create(details.get())));
    } else {
      return invalidRequest("Invalid update: " + key);
    }
  }

  @Override
  public Response getJobUpdateDiff(JobUpdateRequest request) {
    JobUpdateRequest validatedRequest;
    try {
      validatedRequest = request.withTaskConfig(
          configurationManager.validateAndPopulate(request.getTaskConfig()));
    } catch (TaskDescriptionException e) {
      return error(INVALID_REQUEST, e);
    }

    JobKey job = validatedRequest.getTaskConfig().getJob();

    return storage.read(storeProvider -> {
      if (storeProvider.getCronJobStore().fetchJob(job).isPresent()) {
        return invalidRequest(NO_CRON);
      }

      JobDiff diff = JobDiff.compute(
          storeProvider.getTaskStore(),
          job,
          JobDiff.asMap(validatedRequest.getTaskConfig(), validatedRequest.getInstanceCount()),
          validatedRequest.getSettings().getUpdateOnlyTheseInstances());

      Map<Integer, TaskConfig> replaced = diff.getReplacedInstances();
      Map<Integer, TaskConfig> replacements = Maps.asMap(
          diff.getReplacementInstances(),
          Functions.constant(validatedRequest.getTaskConfig()));

      Map<Integer, TaskConfig> add = Maps.filterKeys(
          replacements,
          Predicates.in(Sets.difference(replacements.keySet(), replaced.keySet())));
      Map<Integer, TaskConfig> remove = Maps.filterKeys(
          replaced,
          Predicates.in(Sets.difference(replaced.keySet(), replacements.keySet())));
      Map<Integer, TaskConfig> update = Maps.filterKeys(
          replaced,
          Predicates.in(Sets.intersection(replaced.keySet(), replacements.keySet())));

      return ok(Result.getJobUpdateDiffResult(GetJobUpdateDiffResult.builder()
          .setAdd(instancesToConfigGroups(add))
          .setRemove(instancesToConfigGroups(remove))
          .setUpdate(instancesToConfigGroups(update))
          .setUnchanged(instancesToConfigGroups(diff.getUnchangedInstances()))
          .build()));
    });
  }

  private static Set<ConfigGroup> instancesToConfigGroups(Map<Integer, TaskConfig> tasks) {
    Multimap<TaskConfig, Integer> instancesByDetails = Multimaps.invertFrom(
        Multimaps.forMap(tasks),
        HashMultimap.create());
    return ImmutableSet.copyOf(
        Iterables.transform(instancesByDetails.asMap().entrySet(), TO_GROUP));
  }

  private List<ScheduledTask> getTasks(TaskQuery query) {
    requireNonNull(query);

    Iterable<ScheduledTask> tasks = Storage.Util.fetchTasks(storage, Query.arbitrary(query));
    if (query.getOffset() > 0) {
      tasks = Iterables.skip(tasks, query.getOffset());
    }
    if (query.getLimit() > 0) {
      tasks = Iterables.limit(tasks, query.getLimit());
    }

    return ImmutableList.copyOf(tasks);
  }

  private Query.Builder maybeRoleScoped(Optional<String> ownerRole) {
    return ownerRole.isPresent()
        ? Query.roleScoped(ownerRole.get())
        : Query.unscoped();
  }

  private Map<JobKey, JobConfiguration> getJobs(
      Optional<String> ownerRole,
      Multimap<JobKey, ScheduledTask> tasks) {

    // We need to synthesize the JobConfiguration from the the current tasks because the
    // ImmediateJobManager doesn't store jobs directly and ImmediateJobManager#getJobs always
    // returns an empty Collection.
    Map<JobKey, JobConfiguration> jobs = Maps.newHashMap();

    jobs.putAll(Maps.transformEntries(tasks.asMap(),
        (jobKey, tasks1) -> {

          // Pick the latest transitioned task for each immediate job since the job can be in the
          // middle of an update or some shards have been selectively created.
          TaskConfig mostRecentTaskConfig =
              Tasks.getLatestActiveTask(tasks1).getAssignedTask().getTask();

          return JobConfiguration.builder()
              .setKey(jobKey)
              .setOwner(mostRecentTaskConfig.getOwner())
              .setTaskConfig(mostRecentTaskConfig)
              .setInstanceCount(tasks1.size())
              .build();
        }));

    // Get cron jobs directly from the manager. Do this after querying the task store so the real
    // template JobConfiguration for a cron job will overwrite the synthesized one that could have
    // been created above.
    Predicate<JobConfiguration> configFilter = ownerRole.isPresent()
        ? Predicates.compose(Predicates.equalTo(ownerRole.get()), JobKeys::getRole)
        : Predicates.alwaysTrue();
    jobs.putAll(Maps.uniqueIndex(
        FluentIterable.from(Storage.Util.fetchCronJobs(storage)).filter(configFilter),
        JobConfiguration::getKey));

    return jobs;
  }

  private Multimap<JobKey, ScheduledTask> getTasks(Query.Builder query) {
    return Tasks.byJobKey(Storage.Util.fetchTasks(storage, query));
  }

  @VisibleForTesting
  static final String NO_CRON = "Cron jobs are not supported.";
}
