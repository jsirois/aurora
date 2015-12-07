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

import javax.inject.Inject;

import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;

import org.apache.aurora.scheduler.storage.CronJobStore;
import org.apache.aurora.scheduler.storage.db.views.DbJobConfiguration;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;

import static java.util.Objects.requireNonNull;

/**
 * Cron job store backed by a relational database.
 */
class DbCronJobStore implements CronJobStore.Mutable {
  private final CronJobMapper cronJobMapper;
  private final JobKeyMapper jobKeyMapper;
  private final TaskConfigManager taskConfigManager;

  @Inject
  DbCronJobStore(
      CronJobMapper cronJobMapper,
      JobKeyMapper jobKeyMapper,
      TaskConfigManager taskConfigManager) {

    this.cronJobMapper = requireNonNull(cronJobMapper);
    this.jobKeyMapper = requireNonNull(jobKeyMapper);
    this.taskConfigManager = requireNonNull(taskConfigManager);
  }

  @Override
  public void saveAcceptedJob(JobConfiguration jobConfig) {
    requireNonNull(jobConfig);
    jobKeyMapper.merge(jobConfig.getKey());
    cronJobMapper.merge(jobConfig, taskConfigManager.insert(jobConfig.getTaskConfig()));
  }

  @Override
  public void removeJob(JobKey jobKey) {
    requireNonNull(jobKey);
    cronJobMapper.delete(jobKey);
  }

  @Override
  public void deleteJobs() {
    cronJobMapper.truncate();
  }

  @Override
  public Iterable<JobConfiguration> fetchJobs() {
    return FluentIterable.from(cronJobMapper.selectAll())
        .transform(DbJobConfiguration::toThrift)
        .toList();
  }

  @Override
  public Optional<JobConfiguration> fetchJob(JobKey jobKey) {
    requireNonNull(jobKey);
    return Optional.fromNullable(cronJobMapper.select(jobKey))
        .transform(DbJobConfiguration::toThrift);
  }
}
