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

import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.MesosContainer;
import org.apache.aurora.gen.peer.MutableDockerContainer;

public final class DbContainer {
  private MutableDockerContainer docker;

  private DbContainer() {
  }

  public Container toThrift() {
    if (docker == null) {
      return Container.mesos(MesosContainer.create());
    } else {
      return Container.docker(docker.toThrift());
    }
  }
}
