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
package org.apache.aurora.scheduler.storage.db.views;

import javax.annotation.Nullable;

import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.gen.peer.MutableLimitConstraint;
import org.apache.aurora.gen.peer.MutableValueConstraint;

public final class DbTaskConstraint {
  private MutableValueConstraint value;
  private MutableLimitConstraint limit;

  private DbTaskConstraint() {
  }

  private static boolean isSet(Object o) {
    return o != null;
  }

  @Nullable
  public TaskConstraint toThrift() {
    // Using the isSet shim to work around a well-intentioned PMD rule that prefers positive
    // branching (would trip if we did value != null directly here.
    if (isSet(value)) {
      return TaskConstraint.value(value.toThrift());
    } else if (isSet(limit)) {
      return TaskConstraint.limit(limit.toThrift());
    } else {
      return null;
    }
  }
}
