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
package org.apache.aurora.scheduler.http.api.security;

import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobConfiguration._Fields;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.TaskConfig;
import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class ThriftFieldGetterTest {
  @Test
  public void testStructFieldGetter() {
    JobKey jobKey = JobKey.builder().build();
    FieldGetter<JobConfiguration, JobKey> fieldGetter =
        new ThriftFieldGetter<>(JobConfiguration.class, _Fields.KEY, JobKey.class);

    JobConfiguration jobConfiguration = JobConfiguration.builder().setKey(jobKey).build();

    assertSame(jobKey, fieldGetter.apply(jobConfiguration).orNull());
  }

  @Test
  public void testStructFieldGetterUnsetField() {
    FieldGetter<JobConfiguration, TaskConfig> fieldGetter =
        new ThriftFieldGetter<>(JobConfiguration.class, _Fields.TASK_CONFIG, TaskConfig.class);

    JobConfiguration jobConfiguration = JobConfiguration.builder().setInstanceCount(5).build();

    assertNull(fieldGetter.apply(jobConfiguration).orNull());
  }
}
