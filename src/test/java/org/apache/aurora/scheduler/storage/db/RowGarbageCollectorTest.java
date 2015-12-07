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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.common.util.testing.FakeClock;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RowGarbageCollectorTest {

  private static final JobKey JOB_A = JobKey.create("roleA", "envA", "jobA");
  private static final JobKey JOB_B = JobKey.create("roleB", "envB", "jobB");
  private static final ScheduledTask TASK_A2 = TaskTestUtil.makeTask("task_a2", JOB_A);
  private static final TaskConfig CONFIG_A =
      TASK_A2.getAssignedTask().getTask().toBuilder().setRamMb(124246).build();
  private static final TaskConfig CONFIG_B = TaskTestUtil.makeConfig(JOB_B);

  private JobKeyMapper jobKeyMapper;
  private TaskMapper taskMapper;
  private TaskConfigMapper taskConfigMapper;
  private RowGarbageCollector rowGc;

  @Before
  public void setUp() {
    Injector injector = Guice.createInjector(
        DbModule.testModule(),
        new DbModule.GarbageCollectorModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(StatsProvider.class).toInstance(new FakeStatsProvider());
            bind(Clock.class).toInstance(new FakeClock());
          }
        }
    );

    rowGc = injector.getInstance(RowGarbageCollector.class);
    injector.getInstance(Storage.class).prepare();
    taskMapper = injector.getInstance(TaskMapper.class);
    jobKeyMapper = injector.getInstance(JobKeyMapper.class);
    taskConfigMapper = injector.getInstance(TaskConfigMapper.class);
  }

  @Test
  public void testNoop() {
    rowGc.runOneIteration();
  }

  @Test
  public void testGarbageCollection() {
    rowGc.runOneIteration();
    assertEquals(ImmutableList.of(), jobKeyMapper.selectAll());
    assertEquals(ImmutableList.of(), taskConfigMapper.selectConfigsByJob(JOB_A));
    assertEquals(ImmutableList.of(), taskConfigMapper.selectConfigsByJob(JOB_B));

    jobKeyMapper.merge(JOB_A);
    rowGc.runOneIteration();
    assertEquals(ImmutableList.of(), jobKeyMapper.selectAll());
    assertEquals(ImmutableList.of(), taskConfigMapper.selectConfigsByJob(JOB_A));
    assertEquals(ImmutableList.of(), taskConfigMapper.selectConfigsByJob(JOB_B));

    jobKeyMapper.merge(JOB_A);
    taskConfigMapper.insert(CONFIG_A, new InsertResult());
    InsertResult a2Insert = new InsertResult();
    taskConfigMapper.insert(TASK_A2.getAssignedTask().getTask(), a2Insert);
    taskMapper.insertScheduledTask(TASK_A2, a2Insert.getId(), new InsertResult());
    jobKeyMapper.merge(JOB_B);
    taskConfigMapper.insert(CONFIG_B, new InsertResult());
    rowGc.runOneIteration();
    // Only job A and config A2 are still referenced, other rows are deleted.
    assertEquals(ImmutableList.of(JOB_A), jobKeyMapper.selectAll());
    // Note: Using the ramMb as a sentinel value, since relations in the TaskConfig are not
    // populated, therefore full object equivalence cannot easily be used.
    assertEquals(
        TASK_A2.getAssignedTask().getTask().getRamMb(),
        Iterables.getOnlyElement(taskConfigMapper.selectConfigsByJob(JOB_A)).toThrift()
            .getRamMb());
    assertEquals(ImmutableList.of(), taskConfigMapper.selectConfigsByJob(JOB_B));
  }
}
