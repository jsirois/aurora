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

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.CronCollisionPolicy;
import org.apache.aurora.gen.DockerContainer;
import org.apache.aurora.gen.DockerParameter;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.LimitConstraint;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.gen.ValueConstraint;
import org.apache.aurora.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.apache.aurora.gen.test.Constants.INVALID_IDENTIFIERS;
import static org.apache.aurora.gen.test.Constants.VALID_IDENTIFIERS;
import static org.apache.aurora.scheduler.base.UserProvidedStrings.isGoodIdentifier;
import static org.apache.aurora.scheduler.configuration.ConfigurationManager.DEDICATED_ATTRIBUTE;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

// TODO(kevints): Improve test coverage for this class.
public class ConfigurationManagerTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private static final ImmutableSet<Container.Fields> ALL_CONTAINER_TYPES =
      ImmutableSet.of(Container.Fields.DOCKER, Container.Fields.MESOS);

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
              .setRequestedPorts()
              .setJobName(null)
              .setPriority(0)
              .setOwner(null)
              .setContactEmail("foo@twitter.com")
              .setProduction(false)
              .setDiskMb(1)
              .setMetadata()
              .setNumCpus(1.0)
              .setRamMb(1)
              .setMaxTaskFailures(0)
              .setConstraints(
                  Constraint.create("executor",
                      TaskConstraint.value(
                          ValueConstraint.create(false, ImmutableSet.of("legacy")))),
                  Constraint.create("host", TaskConstraint.limit(LimitConstraint.create(1))),
                  Constraint.create(DEDICATED_ATTRIBUTE,
                      TaskConstraint.value(
                          ValueConstraint.create(false, ImmutableSet.of("foo")))))
              .build())
      .setOwner(Identity.create("owner-role", "owner-user"))
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

  private ConfigurationManager configurationManager;
  private ConfigurationManager dockerConfigurationManager;

  @Before
  public void setUp() {
    configurationManager = new ConfigurationManager(
        ALL_CONTAINER_TYPES, false, ImmutableMultimap.of());

    dockerConfigurationManager = new ConfigurationManager(
        ALL_CONTAINER_TYPES, true, ImmutableMultimap.of("foo", "bar"));
  }

  @Test
  public void testIsGoodIdentifier() {
    for (String identifier : VALID_IDENTIFIERS) {
      assertTrue(isGoodIdentifier(identifier));
    }
    for (String identifier : INVALID_IDENTIFIERS) {
      assertFalse(isGoodIdentifier(identifier));
    }
  }

  @Test
  public void testBadContainerConfig() throws TaskDescriptionException {
    TaskConfig invalidTaskConfig =
        CONFIG_WITH_CONTAINER.withContainer(
            c -> Container.docker(c.getDocker().withImage((String) null)));

    expectTaskDescriptionException("A container must specify an image");
    configurationManager.validateAndPopulate(invalidTaskConfig);
  }

  @Test
  public void testDisallowedDockerParameters() throws TaskDescriptionException {
    TaskConfig invalidTaskConfig =
        CONFIG_WITH_CONTAINER.withContainer(
            c -> Container.docker(c.getDocker().withParameters(
                ImmutableList.of(DockerParameter.create("foo", "bar")))));

    ConfigurationManager noParamsManager = new ConfigurationManager(
        ALL_CONTAINER_TYPES, false, ImmutableMultimap.of());

    expectTaskDescriptionException("Docker parameters not allowed");
    noParamsManager.validateAndPopulate(invalidTaskConfig);
  }

  @Test
  public void testInvalidTier() throws TaskDescriptionException {
    TaskConfig config = UNSANITIZED_JOB_CONFIGURATION.getTaskConfig().toBuilder()
        .setJobName("job")
        .setEnvironment("env")
        .setTier("pr/d")
        .build();

    expectTaskDescriptionException("Tier contains illegal characters");
    configurationManager.validateAndPopulate(config);
  }

  @Test
  public void testDefaultDockerParameters() throws TaskDescriptionException {
    TaskConfig result = dockerConfigurationManager.validateAndPopulate(CONFIG_WITH_CONTAINER);

    // The resulting task config should contain parameters supplied to the ConfigurationManager.
    List<DockerParameter> params = result.getContainer().getDocker().getParameters();
    assertThat(params, is(Arrays.asList(DockerParameter.create("foo", "bar"))));
  }

  @Test
  public void testPassthroughDockerParameters() throws TaskDescriptionException {
    DockerParameter userParameter = DockerParameter.create("bar", "baz");

    TaskConfig result = dockerConfigurationManager.validateAndPopulate(
        CONFIG_WITH_CONTAINER.withContainer(
            c -> Container.docker(c.getDocker().withParameters(ImmutableList.of(userParameter)))));

    // The resulting task config should contain parameters supplied from user config.
    List<DockerParameter> params = result.getContainer().getDocker().getParameters();
    assertThat(params, is(Arrays.asList(userParameter)));
  }

  private void expectTaskDescriptionException(String message) {
    expectedException.expect(TaskDescriptionException.class);
    expectedException.expectMessage(message);
  }
}
