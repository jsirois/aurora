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

import java.io.IOException;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.MaintenanceMode.DRAINED;
import static org.junit.Assert.assertEquals;

public class DbAttributeStoreTest {

  private static final String HOST_A = "hostA";
  private static final String HOST_B = "hostB";
  private static final String SLAVE_A = "slaveA";
  private static final String SLAVE_B = "slaveB";
  private static final Attribute ATTR1 = new Attribute("attr1", ImmutableSet.of("a", "b", "c"));
  private static final Attribute ATTR2 = new Attribute("attr2", ImmutableSet.of("d", "e", "f"));
  private static final Attribute ATTR3 = new Attribute("attr3", ImmutableSet.of("a", "d", "g"));
  private static final HostAttributes HOST_A_ATTRS =
      HostAttributes.build(new HostAttributes(HOST_A, ImmutableSet.of(ATTR1, ATTR2))
          .setSlaveId(SLAVE_A)
          .setAttributes(ImmutableSet.of())
          .setMode(MaintenanceMode.NONE));
  private static final HostAttributes HOST_B_ATTRS =
      HostAttributes.build(new HostAttributes(HOST_B, ImmutableSet.of(ATTR2, ATTR3))
          .setSlaveId(SLAVE_B)
          .setAttributes(ImmutableSet.of())
          .setMode(MaintenanceMode.DRAINING));

  private Storage storage;

  @Before
  public void setUp() throws IOException {
    storage = DbUtil.createStorage();
  }

  @Test
  public void testCrud() {
    assertEquals(Optional.absent(), read(HOST_A));
    assertEquals(ImmutableSet.of(), readAll());

    insert(HOST_A_ATTRS);
    assertEquals(Optional.of(HOST_A_ATTRS), read(HOST_A));
    assertEquals(ImmutableSet.of(HOST_A_ATTRS), readAll());

    insert(HOST_B_ATTRS);
    insert(HOST_B_ATTRS);  // Double insert should be allowed.
    assertEquals(Optional.of(HOST_B_ATTRS), read(HOST_B));
    assertEquals(ImmutableSet.of(HOST_A_ATTRS, HOST_B_ATTRS), readAll());

    HostAttributes updatedA = HostAttributes.build(
        HOST_A_ATTRS.newBuilder().setAttributes(ImmutableSet.of(ATTR1, ATTR3)));
    insert(updatedA);
    assertEquals(Optional.of(updatedA), read(HOST_A));
    assertEquals(ImmutableSet.of(updatedA, HOST_B_ATTRS), readAll());

    HostAttributes updatedMode = HostAttributes.build(updatedA.newBuilder().setMode(DRAINED));
    insert(updatedMode);
    assertEquals(Optional.of(updatedMode), read(HOST_A));
    assertEquals(ImmutableSet.of(updatedMode, HOST_B_ATTRS), readAll());

    truncate();
    assertEquals(Optional.absent(), read(HOST_A));
    assertEquals(ImmutableSet.of(), readAll());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyAttributeValues() {
    HostAttributes attributes = HostAttributes.build(HOST_A_ATTRS.newBuilder()
        .setAttributes(ImmutableSet.of(new Attribute("attr1", ImmutableSet.of()))));
    insert(attributes);
  }

  @Test
  public void testNoAttributes() {
    HostAttributes attributes = HostAttributes.build(
        HOST_A_ATTRS.newBuilder().setAttributes(ImmutableSet.of()));
    insert(attributes);
    assertEquals(Optional.of(attributes), read(HOST_A));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoMode() {
    HostAttributes noMode = HOST_A_ATTRS.newBuilder();
    noMode.unsetMode();

    insert(HostAttributes.build(noMode));
  }

  @Test
  public void testSaveAttributesEmpty() {
    HostAttributes attributes = HOST_A_ATTRS.newBuilder();
    attributes.unsetAttributes();

    insert(HostAttributes.build(attributes));
    assertEquals(Optional.of(HostAttributes.build(attributes)), read(HOST_A));
  }

  @Test
  public void testSlaveIdChanges() {
    insert(HOST_A_ATTRS);
    HostAttributes updated = HostAttributes.build(HOST_A_ATTRS.newBuilder().setSlaveId(SLAVE_B));
    insert(updated);
    assertEquals(Optional.of(updated), read(HOST_A));
  }

  @Test
  public void testUpdateAttributesWithRelations() {
    // Test for regression of AURORA-1379, where host attribute mutation performed a delete,
    // violating foreign key constraints.
    insert(HOST_A_ATTRS);

    ScheduledTask builder = TaskTestUtil.makeTask("a", JobKeys.from("role", "env", "job"))
        .newBuilder();
    builder.getAssignedTask()
        .setSlaveHost(HOST_A_ATTRS.getHost())
        .setSlaveId(HOST_A_ATTRS.getSlaveId());
    final ScheduledTask taskA = ScheduledTask.build(builder);

    storage.write((NoResult.Quiet)
        storeProvider -> storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(taskA)));

    HostAttributes attributeBuilder = HOST_A_ATTRS.newBuilder().setMode(DRAINED);
    attributeBuilder.addToAttributes(new Attribute("newAttr", ImmutableSet.of("a", "b")));
    HostAttributes hostAUpdated = HostAttributes.build(attributeBuilder);
    insert(hostAUpdated);
    assertEquals(Optional.of(hostAUpdated), read(HOST_A));
  }

  private void insert(HostAttributes attributes) {
    storage.write(
        storeProvider -> storeProvider.getAttributeStore().saveHostAttributes(attributes));
  }

  private Optional<HostAttributes> read(String host) {
    return storage.read(storeProvider -> storeProvider.getAttributeStore().getHostAttributes(host));
  }

  private Set<HostAttributes> readAll() {
    return storage.read(storeProvider -> storeProvider.getAttributeStore().getHostAttributes());
  }

  private void truncate() {
    storage.write(
        (NoResult.Quiet) storeProvider -> storeProvider.getAttributeStore().deleteHostAttributes());
  }
}
