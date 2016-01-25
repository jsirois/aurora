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
package org.apache.aurora.scheduler.preemptor;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSetMultimap;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.ASSIGNED;
import static org.apache.aurora.gen.ScheduleStatus.FAILED;
import static org.apache.aurora.gen.ScheduleStatus.FINISHED;
import static org.apache.aurora.gen.ScheduleStatus.KILLED;
import static org.apache.aurora.gen.ScheduleStatus.KILLING;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.gen.ScheduleStatus.THROTTLED;
import static org.junit.Assert.assertEquals;

public class ClusterStateImplTest {

  private ClusterStateImpl state;

  @Before
  public void setUp() {
    state = new ClusterStateImpl();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testImmutable() {
    state.getSlavesToActiveTasks().clear();
  }

  @Test
  public void testTaskLifecycle() {
    AssignedTask a = makeTask("a", "s1");

    assertVictims();
    changeState(a, THROTTLED);
    assertVictims();
    changeState(a, PENDING);
    assertVictims();
    changeState(a, ASSIGNED);
    assertVictims(a);
    changeState(a, RUNNING);
    assertVictims(a);
    changeState(a, KILLING);
    assertVictims(a);
    changeState(a, FINISHED);
    assertVictims();
  }

  @Test
  public void testTaskChangesSlaves() {
    // We do not intend to handle the case of an external failure leading to the same task ID
    // on a different slave.
    AssignedTask a = makeTask("a", "s1");
    AssignedTask a1 = makeTask("a", "s2");
    changeState(a, RUNNING);
    changeState(a1, RUNNING);
    assertVictims(a, a1);
  }

  @Test
  public void testMultipleTasks() {
    AssignedTask a = makeTask("a", "s1");
    AssignedTask b = makeTask("b", "s1");
    AssignedTask c = makeTask("c", "s2");
    AssignedTask d = makeTask("d", "s3");
    AssignedTask e = makeTask("e", "s3");
    AssignedTask f = makeTask("f", "s1");
    changeState(a, RUNNING);
    assertVictims(a);
    changeState(b, RUNNING);
    assertVictims(a, b);
    changeState(c, RUNNING);
    assertVictims(a, b, c);
    changeState(d, RUNNING);
    assertVictims(a, b, c, d);
    changeState(e, RUNNING);
    assertVictims(a, b, c, d, e);
    changeState(c, FINISHED);
    assertVictims(a, b, d, e);
    changeState(a, FAILED);
    changeState(e, KILLED);
    assertVictims(b, d);
    changeState(f, RUNNING);
    assertVictims(b, d, f);
  }

  private void assertVictims(AssignedTask... tasks) {
    ImmutableMultimap.Builder<String, PreemptionVictim> victims = ImmutableSetMultimap.builder();
    for (AssignedTask task : tasks) {
      victims.put(task.getSlaveId(), PreemptionVictim.fromTask(task));
    }
    assertEquals(victims.build(), state.getSlavesToActiveTasks());
  }

  private AssignedTask makeTask(String taskId, String slaveId) {
    return AssignedTask.build(new AssignedTask()
        .setTaskId(taskId)
        .setSlaveId(slaveId)
        .setSlaveHost(slaveId + "host")
        .setTask(new TaskConfig().setJob(new JobKey("role", "env", "job"))));
  }

  private void changeState(AssignedTask assignedTask, ScheduleStatus status) {
    ScheduledTask task = ScheduledTask.build(new ScheduledTask()
        .setStatus(status)
        .setAssignedTask(assignedTask.newBuilder()));
    state.taskChangedState(TaskStateChange.transition(task, ScheduleStatus.INIT));
  }
}
