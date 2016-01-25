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
package org.apache.aurora.scheduler.sla;

import java.util.Map;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.base.TaskTestUtil;

final class SlaTestUtil {

  private SlaTestUtil() {
    // Utility class.
  }

  static ScheduledTask makeTask(Map<Long, ScheduleStatus> events, int instanceId) {
    return makeTask(events, instanceId, true);
  }

  static ScheduledTask makeTask(Map<Long, ScheduleStatus> events, int instanceId, boolean isProd) {
    ImmutableList<TaskEvent> taskEvents = makeEvents(events);
    return TaskTestUtil.makeTask("task_id", TaskTestUtil.JOB)
        .withStatus(Iterables.getLast(taskEvents).getStatus())
        .withTaskEvents(taskEvents)
        .withAssignedTask(at -> at.withInstanceId(instanceId).withTask(t -> t.withProduction(isProd)));
  }

  private static ImmutableList<TaskEvent> makeEvents(Map<Long, ScheduleStatus> events) {
    return FluentIterable.from(events.entrySet())
        .transform(e -> TaskEvent.create(e.getKey(), e.getValue()))
        .toList();
  }
}
