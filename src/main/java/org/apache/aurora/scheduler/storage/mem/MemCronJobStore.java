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
package org.apache.aurora.scheduler.storage.mem;

import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.storage.CronJobStore;

/**
 * An in-memory cron job store.
 */
class MemCronJobStore implements CronJobStore.Mutable {
  private final Map<JobKey, JobConfiguration> jobs = Maps.newConcurrentMap();

  @Override
  public void saveAcceptedJob(JobConfiguration jobConfig) {
    JobKey key = JobKeys.assertValid(jobConfig.getKey());
    jobs.put(key, jobConfig);
  }

  @Override
  public void removeJob(JobKey jobKey) {
    jobs.remove(jobKey);
  }

  @Override
  public void deleteJobs() {
    jobs.clear();
  }

  @Override
  public Iterable<JobConfiguration> fetchJobs() {
    return ImmutableSet.copyOf(jobs.values());
  }

  @Override
  public Optional<JobConfiguration> fetchJob(JobKey jobKey) {
    return Optional.fromNullable(jobs.get(jobKey));
  }
}
