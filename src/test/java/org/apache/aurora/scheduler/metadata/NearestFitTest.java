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
package org.apache.aurora.scheduler.metadata;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.util.testing.FakeTicker;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.events.PubsubEvent.TasksDeleted;
import org.apache.aurora.scheduler.events.PubsubEvent.Vetoed;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NearestFitTest {
  private static final int RESOURCE_MAX_SCORE = 1000;
  private static final Veto SEVERITY_1 = Veto.dedicatedHostConstraintMismatch();
  private static final Veto SEVERITY_2 = Veto.maintenance("maintenance");
  private static final Veto SEVERITY_3 = Veto.constraintMismatch("constraint");
  private static final Veto SEVERITY_4_CPU =
      Veto.insufficientResources("cpu", RESOURCE_MAX_SCORE);
  private static final Veto SEVERITY_4_RAM =
      Veto.insufficientResources("ram", RESOURCE_MAX_SCORE);
  private static final Veto SEVERITY_4_DISK =
      Veto.insufficientResources("disk", RESOURCE_MAX_SCORE);
  private static final Veto SEVERITY_4_PORTS =
      Veto.insufficientResources("ports", RESOURCE_MAX_SCORE);

  private static final TaskConfig TASK = TaskConfig.builder().setNumCpus(1.0).build();
  private static final TaskGroupKey GROUP_KEY = TaskGroupKey.from(TASK);

  private FakeTicker ticker;
  private NearestFit nearest;

  @Before
  public void setUp() {
    ticker = new FakeTicker();
    nearest = new NearestFit(ticker);
  }

  @Test
  public void testNoReason() {
    assertNearest();
  }

  @Test
  public void testScoring() {
    vetoed(SEVERITY_1);
    assertNearest(SEVERITY_1);
    vetoed(SEVERITY_2);
    assertNearest(SEVERITY_2);
    vetoed(SEVERITY_3);
    assertNearest(SEVERITY_3);
    vetoed(SEVERITY_4_CPU, SEVERITY_4_RAM, SEVERITY_4_DISK, SEVERITY_4_PORTS);
    assertNearest(SEVERITY_4_CPU, SEVERITY_4_RAM, SEVERITY_4_DISK, SEVERITY_4_PORTS);
  }

  @Test
  public void testRemove() {
    vetoed(SEVERITY_1);
    nearest.remove(new TasksDeleted(ImmutableSet.of(makeTask())));
    assertNearest();
  }

  private ScheduledTask makeTask() {
    return ScheduledTask.builder()
        .setAssignedTask(AssignedTask.builder().setTask(TASK).build())
        .build();
  }

  @Test
  public void testExpiration() {
    vetoed(SEVERITY_2);
    assertNearest(SEVERITY_2);
    ticker.advance(NearestFit.EXPIRATION);
    ticker.advance(Amount.of(1L, Time.SECONDS));
    assertNearest();
  }

  @Test
  public void testStateChanged() {
    vetoed(SEVERITY_2);
    assertNearest(SEVERITY_2);
    ScheduledTask task = makeTask().withStatus(ScheduleStatus.ASSIGNED);
    nearest.stateChanged(TaskStateChange.transition(task, ScheduleStatus.PENDING));
    assertNearest();
  }

  private Set<Veto> vetoes(Veto... vetoes) {
    return ImmutableSet.copyOf(vetoes);
  }

  private void vetoed(Veto... vetoes) {
    nearest.vetoed(new Vetoed(GROUP_KEY, ImmutableSet.copyOf(vetoes)));
  }

  private void assertNearest(Veto... vetoes) {
    assertEquals(vetoes(vetoes), nearest.getNearestFit(GROUP_KEY));
  }
}
