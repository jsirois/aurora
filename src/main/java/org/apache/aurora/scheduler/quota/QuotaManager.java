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
package org.apache.aurora.scheduler.quota;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.RangeSet;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.ResourceAggregates;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.updater.Updates;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Predicates.and;
import static com.google.common.base.Predicates.compose;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Predicates.or;

import static org.apache.aurora.scheduler.ResourceAggregates.EMPTY;
import static org.apache.aurora.scheduler.quota.QuotaCheckResult.Result.SUFFICIENT_QUOTA;
import static org.apache.aurora.scheduler.updater.Updates.getInstanceIds;

/**
 * Allows access to resource quotas, and tracks quota consumption.
 */
public interface QuotaManager {
  Predicate<TaskConfig> PROD = TaskConfig::isProduction;
  Predicate<TaskConfig> DEDICATED =
      e -> ConfigurationManager.isDedicated(e.getConstraints());
  Predicate<TaskConfig> PROD_SHARED = and(PROD, not(DEDICATED));
  Predicate<TaskConfig> PROD_DEDICATED = and(PROD, DEDICATED);
  Predicate<TaskConfig> NON_PROD_SHARED = and(not(PROD), not(DEDICATED));
  Predicate<TaskConfig> NON_PROD_DEDICATED = and(not(PROD), DEDICATED);

  /**
   * Saves a new quota for the provided role or overrides the existing one.
   *
   * @param role Quota owner.
   * @param quota Quota to save.
   * @param storeProvider A store provider to access quota and other data.
   * @throws QuotaException If provided quota specification is invalid.
   */
  void saveQuota(
      String role,
      ResourceAggregate quota,
      MutableStoreProvider storeProvider) throws QuotaException;

  /**
   * Gets {@code QuotaInfo} for the specified role.
   *
   * @param role Quota owner.
   * @param storeProvider A store provider to access quota data.
   * @return quota usage information for the given role.
   */
  QuotaInfo getQuotaInfo(String role, StoreProvider storeProvider);

  /**
   * Checks if there is enough resource quota available for adding {@code instances} of
   * {@code template} tasks provided resources consumed by {@code releasedTemplates} tasks
   * are released. The quota is defined at the task owner (role) level.
   *
   * @param template Task resource requirement.
   * @param instances Number of additional instances requested.
   * @param storeProvider A store provider to access quota data.
   * @return quota check result details.
   */
  QuotaCheckResult checkInstanceAddition(
      TaskConfig template,
      int instances,
      StoreProvider storeProvider);

  /**
   * Checks if there is enough resource quota available for performing a job update represented
   * by the {@code jobUpdate}. The quota is defined at the task owner (role) level.
   *
   * @param jobUpdate Job update to check quota for.
   * @param storeProvider A store provider to access quota data.
   * @return quota check result details.
   */
  QuotaCheckResult checkJobUpdate(JobUpdate jobUpdate, StoreProvider storeProvider);

  /**
   * Check if there is enough resource quota available for creating or updating a cron job
   * represented by the {@code cronConfig}. The quota is defined at the task owner (role) level.
   *
   * @param cronConfig Cron job configuration.
   * @param storeProvider A store provider to access quota data.
   * @return quota check result details.
   */
  QuotaCheckResult checkCronUpdate(JobConfiguration cronConfig, StoreProvider storeProvider);

  /**
   * Thrown when quota related operation failed.
   */
  class QuotaException extends Exception {
    public QuotaException(String msg) {
      super(msg);
    }
  }

  /**
   * Quota provider that stores quotas in the canonical store.
   */
  class QuotaManagerImpl implements QuotaManager {
    private static final Predicate<TaskConfig> NO_QUOTA_CHECK = or(PROD_DEDICATED, not(PROD));

    @Override
    public void saveQuota(
        final String ownerRole,
        final ResourceAggregate quota,
        MutableStoreProvider storeProvider) throws QuotaException {

      if (quota.getNumCpus() < 0.0 || quota.getRamMb() < 0 || quota.getDiskMb() < 0) {
        throw new QuotaException("Negative values in: " + quota.toString());
      }

      QuotaInfo info = getQuotaInfo(ownerRole, Optional.absent(), storeProvider);
      ResourceAggregate prodConsumption = info.getProdSharedConsumption();
      if (quota.getNumCpus() < prodConsumption.getNumCpus()
          || quota.getRamMb() < prodConsumption.getRamMb()
          || quota.getDiskMb() < prodConsumption.getDiskMb()) {
        throw new QuotaException(String.format(
            "Quota: %s is less then current prod reservation: %s",
            quota.toString(),
            prodConsumption.toString()));
      }

      storeProvider.getQuotaStore().saveQuota(ownerRole, quota);
    }

    @Override
    public QuotaInfo getQuotaInfo(String role, StoreProvider storeProvider) {
      return getQuotaInfo(role, Optional.absent(), storeProvider);
    }

    @Override
    public QuotaCheckResult checkInstanceAddition(
        TaskConfig template,
        int instances,
        StoreProvider storeProvider) {

      Preconditions.checkArgument(instances >= 0);
      if (NO_QUOTA_CHECK.apply(template)) {
        return new QuotaCheckResult(SUFFICIENT_QUOTA);
      }

      QuotaInfo quotaInfo = getQuotaInfo(template.getJob().getRole(), storeProvider);
      ResourceAggregate requestedTotal =
          add(quotaInfo.getProdSharedConsumption(), scale(template, instances));

      return QuotaCheckResult.greaterOrEqual(quotaInfo.getQuota(), requestedTotal);
    }

    @Override
    public QuotaCheckResult checkJobUpdate(JobUpdate jobUpdate, StoreProvider storeProvider) {
      requireNonNull(jobUpdate);
      if (!jobUpdate.getInstructions().isSetDesiredState()
          || NO_QUOTA_CHECK.apply(jobUpdate.getInstructions().getDesiredState().getTask())) {

        return new QuotaCheckResult(SUFFICIENT_QUOTA);
      }

      QuotaInfo quotaInfo = getQuotaInfo(
          jobUpdate.getSummary().getKey().getJob().getRole(),
          Optional.of(jobUpdate),
          storeProvider);

      return QuotaCheckResult.greaterOrEqual(
          quotaInfo.getQuota(),
          quotaInfo.getProdSharedConsumption());
    }

    @Override
    public QuotaCheckResult checkCronUpdate(
        JobConfiguration cronConfig,
        StoreProvider storeProvider) {

      if (!cronConfig.getTaskConfig().isProduction()) {
        return new QuotaCheckResult(SUFFICIENT_QUOTA);
      }

      QuotaInfo quotaInfo =
          getQuotaInfo(cronConfig.getKey().getRole(), Optional.absent(), storeProvider);

      Optional<JobConfiguration> oldCron =
          storeProvider.getCronJobStore().fetchJob(cronConfig.getKey());

      ResourceAggregate oldResource = oldCron.isPresent() ? scale(oldCron.get()) : EMPTY;

      // Calculate requested total as a sum of current prod consumption and a delta between
      // new and old cron templates.
      ResourceAggregate requestedTotal = add(
          quotaInfo.getProdSharedConsumption(),
          subtract(scale(cronConfig), oldResource));

      return QuotaCheckResult.greaterOrEqual(quotaInfo.getQuota(), requestedTotal);
    }

    /**
     * Gets QuotaInfo with currently allocated quota and actual consumption data.
     * <p>
     * In case an optional {@code requestedUpdate} is specified, the consumption returned also
     * includes an estimated resources share of that update as if it was already in progress.
     *
     * @param role Role to get quota info for.
     * @param requestedUpdate An optional {@code JobUpdate} to forecast the consumption.
     * @param storeProvider A store provider to access quota data.
     * @return {@code QuotaInfo} with quota and consumption details.
     */
    private QuotaInfo getQuotaInfo(
        String role,
        Optional<JobUpdate> requestedUpdate,
        StoreProvider storeProvider) {

      FluentIterable<AssignedTask> tasks = FluentIterable
          .from(storeProvider.getTaskStore().fetchTasks(Query.roleScoped(role).active()))
          .transform(ScheduledTask::getAssignedTask);

      Map<JobKey, JobUpdateInstructions> updates = Maps.newHashMap(
          fetchActiveJobUpdates(storeProvider.getJobUpdateStore(), role));

      // Mix in a requested job update (if present) to correctly calculate consumption.
      // This would be an update that is not saved in the store yet (i.e. the one quota is
      // checked for).
      if (requestedUpdate.isPresent()) {
        updates.put(
            requestedUpdate.get().getSummary().getKey().getJob(),
            requestedUpdate.get().getInstructions());
      }

      Map<JobKey, JobConfiguration> cronTemplates =
          FluentIterable.from(storeProvider.getCronJobStore().fetchJobs())
              .filter(compose(equalTo(role), JobKeys::getRole))
              .uniqueIndex(JobConfiguration::getKey);

      return new QuotaInfo(
          storeProvider.getQuotaStore().fetchQuota(role).or(EMPTY),
          getConsumption(tasks, updates, cronTemplates, PROD_SHARED),
          getConsumption(tasks, updates, cronTemplates, PROD_DEDICATED),
          getConsumption(tasks, updates, cronTemplates, NON_PROD_SHARED),
          getConsumption(tasks, updates, cronTemplates, NON_PROD_DEDICATED));
    }

    private ResourceAggregate getConsumption(
        FluentIterable<AssignedTask> tasks,
        Map<JobKey, JobUpdateInstructions> updatesByKey,
        Map<JobKey, JobConfiguration> cronTemplatesByKey,
        Predicate<TaskConfig> filter) {

      FluentIterable<AssignedTask> filteredTasks =
          tasks.filter(compose(filter, AssignedTask::getTask));

      Predicate<AssignedTask> excludeCron = compose(
          not(in(cronTemplatesByKey.keySet())),
          Tasks::getJob);

      ResourceAggregate nonCronConsumption = getNonCronConsumption(
          updatesByKey,
          filteredTasks.filter(excludeCron),
          filter);

      ResourceAggregate cronConsumption = getCronConsumption(
          Iterables.filter(
              cronTemplatesByKey.values(),
              compose(filter, JobConfiguration::getTaskConfig)),
          filteredTasks.transform(AssignedTask::getTask));

      return add(nonCronConsumption, cronConsumption);
    }

    private static ResourceAggregate getNonCronConsumption(
        Map<JobKey, JobUpdateInstructions> updatesByKey,
        FluentIterable<AssignedTask> tasks,
        final Predicate<TaskConfig> configFilter) {

      // 1. Get all active tasks that belong to jobs without active updates OR unaffected by an
      //    active update working set. An example of the latter would be instances not updated by
      //    the update due to being already in desired state or outside of update range (e.g.
      //    not in JobUpdateInstructions.updateOnlyTheseInstances). Calculate consumed resources
      //    as "nonUpdateConsumption".
      //
      // 2. Calculate consumed resources from instances affected by the active job updates as
      //    "updateConsumption".
      //
      // 3. Add up the two to yield total consumption.

      ResourceAggregate nonUpdateConsumption = fromTasks(tasks
          .filter(buildNonUpdatingTasksFilter(updatesByKey))
          .transform(AssignedTask::getTask));

      final Predicate<InstanceTaskConfig> instanceFilter =
          compose(configFilter, InstanceTaskConfig::getTask);

      ResourceAggregate updateConsumption =
          addAll(Iterables.transform(updatesByKey.values(), updateResources(instanceFilter)));

      return add(nonUpdateConsumption, updateConsumption);
    }

    private static ResourceAggregate getCronConsumption(
        Iterable<JobConfiguration> cronTemplates,
        FluentIterable<TaskConfig> tasks) {

      // Calculate the overall cron consumption as MAX between cron template resources and active
      // cron tasks. This is required to account for a case when a running cron task has higher
      // resource requirements than its updated template.
      //
      // While this is the "worst case" calculation that does not account for a possible "staggered"
      // cron scheduling, it's the simplest approach possible given the system constraints (e.g.:
      // lack of enforcement on a cron job run duration).

      final Multimap<JobKey, TaskConfig> taskConfigsByKey = tasks.index(TaskConfig::getJob);
      return addAll(Iterables.transform(
          cronTemplates,
          new Function<JobConfiguration, ResourceAggregate>() {
            @Override
            public ResourceAggregate apply(JobConfiguration config) {
              return max(
                  scale(config.getTaskConfig(), config.getInstanceCount()),
                  fromTasks(taskConfigsByKey.get(config.getKey())));
            }
          }));
    }

    private static Predicate<AssignedTask> buildNonUpdatingTasksFilter(
        final Map<JobKey, JobUpdateInstructions> roleJobUpdates) {

      return new Predicate<AssignedTask>() {
        @Override
        public boolean apply(AssignedTask task) {
          Optional<JobUpdateInstructions> update = Optional.fromNullable(
              roleJobUpdates.get(task.getTask().getJob()));

          if (update.isPresent()) {
            JobUpdateInstructions instructions = update.get();
            RangeSet<Integer> initialInstances = getInstanceIds(instructions.getInitialState());
            RangeSet<Integer> desiredInstances = getInstanceIds(instructions.isSetDesiredState()
                ? ImmutableSet.of(instructions.getDesiredState())
                : ImmutableSet.of());

            int instanceId = task.getInstanceId();
            return !initialInstances.contains(instanceId) && !desiredInstances.contains(instanceId);
          }
          return true;
        }
      };
    }

    private static Map<JobKey, JobUpdateInstructions> fetchActiveJobUpdates(
        final JobUpdateStore jobUpdateStore,
        String role) {

      Function<JobUpdateSummary, JobUpdate> fetchUpdate =
          new Function<JobUpdateSummary, JobUpdate>() {
            @Override
            public JobUpdate apply(JobUpdateSummary summary) {
              return jobUpdateStore.fetchJobUpdate(summary.getKey()).get();
            }
          };

      return Maps.transformValues(
          FluentIterable.from(jobUpdateStore.fetchJobUpdateSummaries(updateQuery(role)))
              .transform(fetchUpdate)
              .uniqueIndex(UPDATE_TO_JOB_KEY),
          JobUpdate::getInstructions);
    }

    @VisibleForTesting
    static JobUpdateQuery updateQuery(String role) {
      return JobUpdateQuery.builder()
          .setRole(role)
          .setUpdateStatuses(Updates.ACTIVE_JOB_UPDATE_STATES)
          .build();
    }

    private static final Function<TaskConfig, ResourceAggregate> CONFIG_RESOURCES =
        new Function<TaskConfig, ResourceAggregate>() {
          @Override
          public ResourceAggregate apply(TaskConfig config) {
            return ResourceAggregate.builder()
                .setNumCpus(config.getNumCpus())
                .setRamMb(config.getRamMb())
                .setDiskMb(config.getDiskMb())
                .build();
          }
        };

    private static final Function<InstanceTaskConfig, ResourceAggregate> INSTANCE_RESOURCES =
        new Function<InstanceTaskConfig, ResourceAggregate>() {
          @Override
          public ResourceAggregate apply(InstanceTaskConfig config) {
            return scale(config.getTask(), getUpdateInstanceCount(config.getInstances()));
          }
        };

    private static ResourceAggregate instructionsToResources(
        Iterable<InstanceTaskConfig> instructions) {

      return addAll(FluentIterable.from(instructions).transform(INSTANCE_RESOURCES));
    }

    /**
     * Calculates max aggregate resources consumed by the job update
     * {@code instructions}. The max is calculated between existing and desired task configs on per
     * resource basis. This means max CPU, RAM and DISK values are computed individually and may
     * come from different task configurations. While it may not be the most accurate
     * representation of job update resources during the update, it does guarantee none of the
     * individual resource values is exceeded during the forward/back roll.
     * <p/>
     * NOTE: In case of a job update converting the job production bit (i.e. prod -> non-prod or
     *       non-prod -> prod), only the matching state is counted towards consumption. For example,
     *       prod -> non-prod AND {@code prodSharedConsumption=True}: only the initial state
     *       is accounted.
     */
    private static Function<JobUpdateInstructions, ResourceAggregate> updateResources(
        final Predicate<InstanceTaskConfig> instanceFilter) {

      return new Function<JobUpdateInstructions, ResourceAggregate>() {
        @Override
        public ResourceAggregate apply(JobUpdateInstructions instructions) {
          Iterable<InstanceTaskConfig> initialState =
              Iterables.filter(instructions.getInitialState(), instanceFilter);
          Iterable<InstanceTaskConfig> desiredState = Iterables.filter(
              Optional.fromNullable(instructions.getDesiredState()).asSet(),
              instanceFilter);

          // Calculate result as max(existing, desired) per resource type.
          return max(
              instructionsToResources(initialState),
              instructionsToResources(desiredState));
        }
      };
    }

    private static ResourceAggregate add(ResourceAggregate a, ResourceAggregate b) {
      return addAll(Arrays.asList(a, b));
    }

    private static ResourceAggregate addAll(Iterable<ResourceAggregate> aggregates) {
      ResourceAggregate total = EMPTY;
      for (ResourceAggregate aggregate : aggregates) {
        total = ResourceAggregate.builder()
            .setNumCpus(total.getNumCpus() + aggregate.getNumCpus())
            .setRamMb(total.getRamMb() + aggregate.getRamMb())
            .setDiskMb(total.getDiskMb() + aggregate.getDiskMb())
            .build();
      }
      return total;
    }

    private static ResourceAggregate subtract(ResourceAggregate a, ResourceAggregate b) {
      return ResourceAggregate.builder()
          .setNumCpus(a.getNumCpus() - b.getNumCpus())
          .setRamMb(a.getRamMb() - b.getRamMb())
          .setDiskMb(a.getDiskMb() - b.getDiskMb())
          .build();
    }

    private static ResourceAggregate max(ResourceAggregate a, ResourceAggregate b) {
      return ResourceAggregate.builder()
          .setNumCpus(Math.max(a.getNumCpus(), b.getNumCpus()))
          .setRamMb(Math.max(a.getRamMb(), b.getRamMb()))
          .setDiskMb(Math.max(a.getDiskMb(), b.getDiskMb()))
          .build();
    }

    private static ResourceAggregate scale(TaskConfig taskConfig, int instanceCount) {
      return ResourceAggregates.scale(CONFIG_RESOURCES.apply(taskConfig), instanceCount);
    }

    private static ResourceAggregate scale(JobConfiguration jobConfiguration) {
      return scale(jobConfiguration.getTaskConfig(), jobConfiguration.getInstanceCount());
    }

    private static ResourceAggregate fromTasks(Iterable<TaskConfig> tasks) {
      return addAll(Iterables.transform(tasks, CONFIG_RESOURCES));
    }

    private static final Function<JobUpdate, JobKey> UPDATE_TO_JOB_KEY =
        new Function<JobUpdate, JobKey>() {
          @Override
          public JobKey apply(JobUpdate input) {
            return input.getSummary().getKey().getJob();
          }
        };

    private static int getUpdateInstanceCount(Set<Range> ranges) {
      int instanceCount = 0;
      for (Range range : ranges) {
        instanceCount += range.getLast() - range.getFirst() + 1;
      }

      return instanceCount;
    }
  }
}
