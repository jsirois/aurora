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
package org.apache.aurora.scheduler.http;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.thrift.ThriftFields;
import org.apache.aurora.thrift.ThriftStruct;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class UtilTest extends EasyMockTest {
  @Test
  public void testPrettyPrintString() {
    ThriftFields stringField = createMock(ThriftFields.class);
    ThriftStruct<ThriftFields> struct = createMock(new Clazz<ThriftStruct<ThriftFields>>() { });
    expect(struct.getFields()).andReturn(ImmutableSet.of(stringField));
    expect(struct.isSet(stringField)).andReturn(true);
    expect(struct.getFieldValue(stringField)).andReturn("fred");
    expect(stringField.getFieldName()).andReturn("bob");
    control.replay();

    String actual = Util.prettyPrint(struct);
    assertEquals("bob: \"fred\"", actual);
  }

  @Test
  public void testPrettyPrintUnsetString() {
    ThriftFields stringField = createMock(ThriftFields.class);
    ThriftStruct<ThriftFields> struct = createMock(new Clazz<ThriftStruct<ThriftFields>>() { });
    expect(struct.getFields()).andReturn(ImmutableSet.of(stringField));
    expect(struct.isSet(stringField)).andReturn(false);
    expect(stringField.getFieldName()).andReturn("bob");
    control.replay();

    String actual = Util.prettyPrint(struct);
    assertEquals("bob: not set", actual);
  }

  @Test
  public void testPrettyPrintOther() {
    ThriftFields intField = createMock(ThriftFields.class);
    ThriftStruct<ThriftFields> struct = createMock(new Clazz<ThriftStruct<ThriftFields>>() { });
    expect(struct.getFields()).andReturn(ImmutableSet.of(intField));
    expect(struct.isSet(intField)).andReturn(true);
    expect(struct.getFieldValue(intField)).andReturn(42);
    expect(intField.getFieldName()).andReturn("bob");
    control.replay();

    String actual = Util.prettyPrint(struct);
    assertEquals("bob: 42", actual);
  }

  @Test
  public void testPrettyPrintList() {
    ThriftFields listField = createMock(ThriftFields.class);
    ThriftStruct<ThriftFields> struct = createMock(new Clazz<ThriftStruct<ThriftFields>>() { });
    expect(struct.getFields()).andReturn(ImmutableSet.of(listField));
    expect(struct.isSet(listField)).andReturn(true);
    expect(struct.getFieldValue(listField)).andReturn(ImmutableList.of(1, 2, 3));
    expect(listField.getFieldName()).andReturn("bob");
    control.replay();

    String expected =
        "bob: [\n"
        + "  Item[0] = 1,\n"
        + "  Item[1] = 2,\n"
        + "  Item[2] = 3\n"
        + "]";
    String actual = Util.prettyPrint(struct);
    assertEquals(expected, actual);
  }

  @Test
  public void testPrettyPrintSet() {
    ThriftFields setField = createMock(ThriftFields.class);
    ThriftStruct<ThriftFields> struct = createMock(new Clazz<ThriftStruct<ThriftFields>>() { });
    expect(struct.getFields()).andReturn(ImmutableSet.of(setField));
    expect(struct.isSet(setField)).andReturn(true);
    expect(struct.getFieldValue(setField)).andReturn(ImmutableSet.of(1, 2, 3));
    expect(setField.getFieldName()).andReturn("bob");
    control.replay();

    String expected =
        "bob: {\n"
        + "  Item = 1,\n"
        + "  Item = 2,\n"
        + "  Item = 3\n"
        + "}";
    String actual = Util.prettyPrint(struct);
    assertEquals(expected, actual);
  }

  @Test
  public void testPrettyPrintMap() {
    ThriftFields mapField = createMock(ThriftFields.class);
    ThriftStruct<ThriftFields> struct = createMock(new Clazz<ThriftStruct<ThriftFields>>() { });
    expect(struct.getFields()).andReturn(ImmutableSet.of(mapField));
    expect(struct.isSet(mapField)).andReturn(true);
    expect(struct.getFieldValue(mapField)).andReturn(ImmutableMap.of("a", 1, "b", 2, "c", 3));
    expect(mapField.getFieldName()).andReturn("bob");
    control.replay();

    String expected =
        "bob: {\n"
        + "  \"a\" = 1,\n"
        + "  \"b\" = 2,\n"
        + "  \"c\" = 3\n"
        + "}";
    String actual = Util.prettyPrint(struct);
    assertEquals(expected, actual);
  }

  @Test
  public void testPrettyPrintStruct() {
    ThriftFields intField = createMock(ThriftFields.class);
    ThriftFields listField = createMock(ThriftFields.class);
    ThriftStruct<ThriftFields> innerStruct =
        createMock(new Clazz<ThriftStruct<ThriftFields>>() { });
    expect(innerStruct.getFields())
        .andReturn(ImmutableSet.of(intField, listField));
    expect(innerStruct.isSet(intField)).andReturn(true);
    expect(innerStruct.isSet(listField)).andReturn(true);
    expect(innerStruct.getFieldValue(intField)).andReturn(42);
    expect(innerStruct.getFieldValue(listField)).andReturn(ImmutableList.of());
    expect(intField.getFieldName()).andReturn("f_int");
    expect(listField.getFieldName()).andReturn("f_list");

    ThriftFields structField = createMock(ThriftFields.class);
    ThriftFields stringField = createMock(ThriftFields.class);
    ThriftFields doubleField = createMock(ThriftFields.class);
    ThriftStruct<ThriftFields> outerStruct =
        createMock(new Clazz<ThriftStruct<ThriftFields>>() { });
    expect(outerStruct.getFields())
        .andReturn(ImmutableSet.of(structField, stringField, doubleField));
    expect(outerStruct.isSet(structField)).andReturn(true);
    expect(outerStruct.isSet(stringField)).andReturn(false);
    expect(outerStruct.isSet(doubleField)).andReturn(true);
    expect(outerStruct.getFieldValue(structField)).andReturn(innerStruct);
    expect(outerStruct.getFieldValue(doubleField)).andReturn(42.0);
    expect(structField.getFieldName()).andReturn("bob");
    expect(stringField.getFieldName()).andReturn("fred");
    expect(doubleField.getFieldName()).andReturn("jake");

    control.replay();

    String expected =
        "bob: \n"
        + "  f_int: 42\n"
        + "  f_list: []\n"
        + "fred: not set\n"
        + "jake: 42.0";
    String actual = Util.prettyPrint(outerStruct);
    assertEquals(expected, actual);
  }
}
