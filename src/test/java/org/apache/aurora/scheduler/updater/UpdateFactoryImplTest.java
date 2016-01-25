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
package org.apache.aurora.scheduler.updater;

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.util.testing.FakeClock;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.TaskConfig;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.updater.UpdateFactory.Update;
import static org.junit.Assert.assertEquals;

/**
 * This test can't exercise much functionality of the output from the factory without duplicating
 * test behavior in the job updater integration test. So instead, we test only some basic behavior.
 */
public class UpdateFactoryImplTest {

  private static final JobUpdateInstructions INSTRUCTIONS = JobUpdateInstructions.builder()
      .setDesiredState(instanceConfig(Range.create(0, 2)))
      .setInitialState(ImmutableSet.of(instanceConfig(Range.create(1, 2))))
      .setSettings(JobUpdateSettings.builder()
          .setMaxFailedInstances(1)
          .setMaxPerInstanceFailures(1)
          .setMaxWaitToInstanceRunningMs(100)
          .setMinWaitInInstanceRunningMs(100)
          .setUpdateGroupSize(2)
          .setUpdateOnlyTheseInstances()
          .build())
      .build();

  private UpdateFactory factory;

  @Before
  public void setUp() {
    factory = new UpdateFactory.UpdateFactoryImpl(new FakeClock());
  }

  @Test
  public void testRollingForward() throws Exception  {
    Update update = factory.newUpdate(INSTRUCTIONS, true);
    assertEquals(ImmutableSet.of(0, 1, 2), update.getUpdater().getInstances());
  }

  @Test
  public void testRollingBack() throws Exception {
    Update update = factory.newUpdate(INSTRUCTIONS, false);
    assertEquals(ImmutableSet.of(0, 1, 2), update.getUpdater().getInstances());
  }

  @Test
  public void testRollForwardSpecificInstances() throws Exception {
    JobUpdateInstructions config = INSTRUCTIONS.toBuilder()
        .setInitialState(ImmutableSet.of())
        .setDesiredState(instanceConfig(Range.create(1, 1)))
        .setSettings(INSTRUCTIONS.getSettings()
            .withUpdateOnlyTheseInstances(ImmutableSet.of(Range.create(0, 1))))
        .build();

    Update update = factory.newUpdate(config, true);
    assertEquals(ImmutableSet.of(1), update.getUpdater().getInstances());
  }

  @Test
  public void testRollBackSpecificInstances() throws Exception {
    JobUpdateInstructions config = INSTRUCTIONS.toBuilder()
        .setInitialState()
        .setDesiredState(instanceConfig(Range.create(1, 1)))
        .setSettings(INSTRUCTIONS.getSettings()
            .withUpdateOnlyTheseInstances(ImmutableSet.of(Range.create(0, 1))))
        .build();

    Update update = factory.newUpdate(config, false);
    assertEquals(ImmutableSet.of(1), update.getUpdater().getInstances());
  }

  @Test
  public void testUpdateRemovesInstance() throws Exception {
    JobUpdateInstructions config = INSTRUCTIONS.withDesiredState(
        ds -> ds.withInstances(ImmutableSet.of(Range.create(0, 1))));

    Update update = factory.newUpdate(config, true);
    assertEquals(ImmutableSet.of(0, 1, 2), update.getUpdater().getInstances());
  }

  private static InstanceTaskConfig instanceConfig(Range instances) {
    return InstanceTaskConfig.builder()
        .setTask(TaskConfig.builder().build())
        .setInstances(instances)
        .build();
  }
}
