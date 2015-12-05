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
package org.apache.aurora.scheduler.configuration;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.CronCollisionPolicy;
import org.apache.aurora.gen.DockerContainer;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.LimitConstraint;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.gen.ValueConstraint;
import org.apache.aurora.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import org.junit.Test;

import static org.apache.aurora.gen.test.Constants.INVALID_IDENTIFIERS;
import static org.apache.aurora.gen.test.Constants.VALID_IDENTIFIERS;
import static org.apache.aurora.scheduler.configuration.ConfigurationManager.DEDICATED_ATTRIBUTE;
import static org.apache.aurora.scheduler.configuration.ConfigurationManager.isGoodIdentifier;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

// TODO(kevints): Improve test coverage for this class.
public class ConfigurationManagerTest {
  private static final JobConfiguration UNSANITIZED_JOB_CONFIGURATION = JobConfiguration.builder()
      .setKey(JobKey.create("owner-role", "devel", "email_stats"))
      .setCronSchedule("0 2 * * *")
      .setCronCollisionPolicy(CronCollisionPolicy.KILL_EXISTING)
      .setInstanceCount(1)
      .setTaskConfig(
          TaskConfig.builder()
              .setIsService(false)
              .setTaskLinks(ImmutableMap.of())
              .setExecutorConfig(ExecutorConfig.create("aurora", "config"))
              .setEnvironment("devel")
              .setRequestedPorts(ImmutableSet.of())
              .setJobName(null)
              .setPriority(0)
              .setOwner(null)
              .setContactEmail("foo@twitter.com")
              .setProduction(false)
              .setDiskMb(1)
              .setMetadata(ImmutableSet.of())
              .setNumCpus(1.0)
              .setRamMb(1)
              .setMaxTaskFailures(0)
              .setConstraints(
                  ImmutableSet.of(
                      Constraint.builder()
                          .setName("executor")
                          .setConstraint(TaskConstraint
                              .value(ValueConstraint.builder()
                                  .setNegated(false)
                                  .setValues(ImmutableSet.of("legacy"))
                                  .build()))
                          .build(),
                      Constraint.builder()
                          .setName("host")
                          .setConstraint(TaskConstraint.limit(LimitConstraint.create(1)))
                          .build(),
                      Constraint.builder()
                          .setName(DEDICATED_ATTRIBUTE)
                          .setConstraint(TaskConstraint.value(
                              ValueConstraint.create(false, ImmutableSet.of("foo"))))
                          .build()))
              .setOwner(Identity.create("owner-role", "owner-user"))
              .build())
      .build();
  private static final TaskConfig CONFIG_WITH_CONTAINER = TaskConfig.builder()
      .setJobName("container-test")
      .setEnvironment("devel")
      .setExecutorConfig(ExecutorConfig.builder().build())
      .setOwner(Identity.create("role", "user"))
      .setNumCpus(1)
      .setRamMb(1)
      .setDiskMb(1)
      .setContainer(Container.docker(DockerContainer.create("testimage")))
      .build();

  @Test
  public void testIsGoodIdentifier() {
    for (String identifier : VALID_IDENTIFIERS) {
      assertTrue(isGoodIdentifier(identifier));
    }
    for (String identifier : INVALID_IDENTIFIERS) {
      assertFalse(isGoodIdentifier(identifier));
    }
  }

  @Test(expected = TaskDescriptionException.class)
  public void testBadContainerConfig() throws TaskDescriptionException {
    TaskConfig taskConfig = CONFIG_WITH_CONTAINER.toBuilder()
        .setContainer(Container.docker(DockerContainer.create(null))).build();

    ConfigurationManager.validateAndPopulate(taskConfig);
  }

  @Test(expected = TaskDescriptionException.class)
  public void testInvalidTier() throws TaskDescriptionException {
    TaskConfig config = UNSANITIZED_JOB_CONFIGURATION.getTaskConfig().toBuilder()
        .setJobName("job")
        .setEnvironment("env")
        .setTier("pr/d")
        .build();

    ConfigurationManager.validateAndPopulate(config);
  }
}
