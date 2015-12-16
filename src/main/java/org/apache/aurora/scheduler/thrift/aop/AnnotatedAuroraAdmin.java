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

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.AddInstancesConfig;
import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateRequest;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.gen.LockValidation;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.scheduler.http.api.security.AuthorizingParam;

/**
 * Extension of the generated thrift interface with Java annotations, for example {@link Nullable}
 * and {@link AuthorizingParam}.
 *
 * When adding new methods to api.thrift you should add, at the very least, {@link Nullable}
 * annotations for them here as well.
 *
 * TODO(ksweeney): Investigate adding other (JSR303) validation annotations here as well.
 */
public interface AnnotatedAuroraAdmin extends AuroraAdmin.Sync {
  @Override
  Response createJob(
      @AuthorizingParam @Nullable JobConfiguration description,
      @Nullable Lock lock);

  @Override
  Response scheduleCronJob(
      @AuthorizingParam @Nullable JobConfiguration description,
      @Nullable Lock lock);

  @Override
  Response descheduleCronJob(
      @AuthorizingParam @Nullable JobKey job,
      @Nullable Lock lock);

  @Override
  Response startCronJob(
      @AuthorizingParam @Nullable JobKey job);

  @Override
  Response restartShards(
      @AuthorizingParam @Nullable JobKey job,
      @Nullable Set<Integer> shardIds,
      @Nullable Lock lock);

  @Override
  Response killTasks(
      @AuthorizingParam @Nullable TaskQuery query,
      @Nullable Lock lock);

  @Override
  Response addInstances(
      @AuthorizingParam @Nullable AddInstancesConfig config,
      @Nullable Lock lock);

  @Override
  Response acquireLock(
      @AuthorizingParam @Nullable LockKey lockKey);

  @Override
  Response releaseLock(
      @AuthorizingParam @Nullable Lock lock,
      @Nullable LockValidation validation);

  @Override
  Response replaceCronTemplate(
      @AuthorizingParam @Nullable JobConfiguration config,
      @Nullable Lock lock);

  @Override
  Response startJobUpdate(
      @AuthorizingParam @Nullable JobUpdateRequest request,
      @Nullable String message);

  @Override
  Response pauseJobUpdate(
      @AuthorizingParam @Nullable JobUpdateKey key,
      @Nullable String message);

  @Override
  Response resumeJobUpdate(
      @AuthorizingParam @Nullable JobUpdateKey key,
      @Nullable String message);

  @Override
  Response abortJobUpdate(
      @AuthorizingParam @Nullable JobUpdateKey key,
      @Nullable String message);

  @Override
  Response pulseJobUpdate(
      @AuthorizingParam @Nullable JobUpdateKey key);
}
