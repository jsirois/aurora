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

  private static final ImmutableSet<Container._Fields> ALL_CONTAINER_TYPES =
      ImmutableSet.of(Container._Fields.DOCKER, Container._Fields.MESOS);

  private static final JobConfiguration UNSANITIZED_JOB_CONFIGURATION = new JobConfiguration()
      .setKey(new JobKey("owner-role", "devel", "email_stats"))
      .setCronSchedule("0 2 * * *")
      .setCronCollisionPolicy(CronCollisionPolicy.KILL_EXISTING)
      .setInstanceCount(1)
      .setTaskConfig(
          new TaskConfig()
              .setIsService(false)
              .setTaskLinks(ImmutableMap.of())
              .setExecutorConfig(new ExecutorConfig("aurora", "config"))
              .setEnvironment("devel")
              .setRequestedPorts(ImmutableSet.of())
              .setJobName(null)
              .setPriority(0)
              .setOwner(null)
              .setContactEmail("foo@twitter.com")
              .setProduction(false)
              .setDiskMb(1)
              .setMetadata(null)
              .setNumCpus(1.0)
              .setRamMb(1)
              .setMaxTaskFailures(0)
              .setConstraints(
                  ImmutableSet.of(
                      new Constraint()
                          .setName("executor")
                          .setConstraint(TaskConstraint
                              .value(new ValueConstraint()
                                  .setNegated(false)
                                  .setValues(ImmutableSet.of("legacy")))),
                      new Constraint()
                          .setName("host")
                          .setConstraint(TaskConstraint.limit(new LimitConstraint()
                              .setLimit(1))),
                      new Constraint()
                          .setName(DEDICATED_ATTRIBUTE)
                          .setConstraint(TaskConstraint.value(new ValueConstraint(
                              false, ImmutableSet.of("foo"))))))
              .setOwner(new Identity()
                  .setRole("owner-role")
                  .setUser("owner-user")));
  private static final TaskConfig CONFIG_WITH_CONTAINER = TaskConfig.build(new TaskConfig()
      .setJobName("container-test")
      .setEnvironment("devel")
      .setExecutorConfig(new ExecutorConfig())
      .setOwner(new Identity("role", "user"))
      .setNumCpus(1)
      .setRamMb(1)
      .setDiskMb(1)
      .setContainer(Container.docker(new DockerContainer("testimage"))))
      .newBuilder();

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
    TaskConfig taskConfig = CONFIG_WITH_CONTAINER.deepCopy();
    taskConfig.getContainer().getDocker().setImage(null);

    expectTaskDescriptionException("A container must specify an image");
    configurationManager.validateAndPopulate(TaskConfig.build(taskConfig));
  }

  @Test
  public void testDisallowedDockerParameters() throws TaskDescriptionException {
    TaskConfig taskConfig = CONFIG_WITH_CONTAINER.deepCopy();
    taskConfig.getContainer().getDocker().addToParameters(new DockerParameter("foo", "bar"));

    ConfigurationManager noParamsManager = new ConfigurationManager(
        ALL_CONTAINER_TYPES, false, ImmutableMultimap.of());

    expectTaskDescriptionException("Docker parameters not allowed");
    noParamsManager.validateAndPopulate(TaskConfig.build(taskConfig));
  }

  @Test
  public void testInvalidTier() throws TaskDescriptionException {
    TaskConfig config = TaskConfig.build(UNSANITIZED_JOB_CONFIGURATION.deepCopy().getTaskConfig()
        .setJobName("job")
        .setEnvironment("env")
        .setTier("pr/d"));

    expectTaskDescriptionException("Tier contains illegal characters");
    configurationManager.validateAndPopulate(config);
  }

  @Test
  public void testDefaultDockerParameters() throws TaskDescriptionException {
    TaskConfig result = dockerConfigurationManager.validateAndPopulate(
        TaskConfig.build(CONFIG_WITH_CONTAINER.deepCopy()));

    // The resulting task config should contain parameters supplied to the ConfigurationManager.
    List<DockerParameter> params = result.getContainer().getDocker().getParameters();
    assertThat(
        params, is(Arrays.asList(DockerParameter.build(new DockerParameter("foo", "bar")))));
  }

  @Test
  public void testPassthroughDockerParameters() throws TaskDescriptionException {
    TaskConfig taskConfig = CONFIG_WITH_CONTAINER.deepCopy();
    DockerParameter userParameter = new DockerParameter("bar", "baz");
    taskConfig.getContainer().getDocker().getParameters().clear();
    taskConfig.getContainer().getDocker().addToParameters(userParameter);

    TaskConfig result = dockerConfigurationManager.validateAndPopulate(
        TaskConfig.build(taskConfig));

    // The resulting task config should contain parameters supplied from user config.
    List<DockerParameter> params = result.getContainer().getDocker().getParameters();
    assertThat(
        params, is(Arrays.asList(DockerParameter.build(userParameter))));
  }

  private void expectTaskDescriptionException(String message) {
    expectedException.expect(TaskDescriptionException.class);
    expectedException.expectMessage(message);
  }
}
