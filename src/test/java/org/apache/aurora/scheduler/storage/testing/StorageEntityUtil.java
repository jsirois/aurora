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
package org.apache.aurora.scheduler.storage.testing;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Defaults;
import com.google.common.collect.ImmutableSet;
import com.google.gson.internal.Primitives;

import org.apache.aurora.thrift.ThriftFields;
import org.apache.aurora.thrift.ThriftStruct;
import org.apache.aurora.thrift.ThriftUnion;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Utility for validating objects used in storage testing.
 */
public final class StorageEntityUtil {

  private StorageEntityUtil() {
    // Utility class.
  }

  private static void assertFullyPopulated(
      String name,
      Object object,
      Set<ThriftFields> ignoredFields) {

    if (object instanceof Collection) {
      Object[] values = ((Collection<?>) object).toArray();
      assertFalse("Collection is empty: " + name, values.length == 0);
      for (int i = 0; i < values.length; i++) {
        assertFullyPopulated(name + "[" + i + "]", values[i], ignoredFields);
      }
    } else if (object instanceof Map) {
      Map<?, ?> map = (Map<?, ?>) object;
      assertFalse("Map is empty: " + name, map.isEmpty());
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        assertFullyPopulated(name + " key", entry.getKey(), ignoredFields);
        assertFullyPopulated(name + "[" + entry.getKey() + "]", entry.getValue(), ignoredFields);
      }
    } else if (object instanceof ThriftUnion) {
      @SuppressWarnings("unchecked") // Trivially safe under erasure.
      ThriftUnion<ThriftFields> union = (ThriftUnion<ThriftFields>) object;
      assertFullyPopulated(
          name + "." + union.getSetField().getFieldName(),
          union.getFieldValue(),
          ignoredFields);
    } else if (object instanceof ThriftStruct) {
      @SuppressWarnings("unchecked") // Trivially safe under erasure.
      ThriftStruct<ThriftFields> struct = (ThriftStruct<ThriftFields>) object;
      for (ThriftFields field : struct.getFields()) {
        if (!ignoredFields.contains(field)) {
          String fullName = name + "." + field.getFieldName();
          boolean mustBeSet = !ignoredFields.contains(field);
          boolean isSet = struct.isSet(field);
          if (mustBeSet) {
            assertTrue(fullName + " is not set", isSet);
          }
          if (isSet) {
            Object fieldValue = struct.getFieldValue(field);
            if (Primitives.isWrapperType(field.getFieldType())) {
              if (mustBeSet) {
                assertNotEquals(
                    "Primitive value must not be default: " + fullName,
                    Defaults.defaultValue(Primitives.unwrap(fieldValue.getClass())),
                    fieldValue);
              }
            } else {
              assertFullyPopulated(fullName, fieldValue, ignoredFields);
            }
          }
        }
      }
    }
  }

  /**
   * Ensures that an object tree is fully-populated.  This is useful when testing store
   * implementations to validate that all fields are mapped during a round-trip into and out of
   * a store implementation.
   *
   * @param object Object to ensure is fully populated.
   * @param <T> Object type.
   * @return The original {@code object}.
   */
  public static <T extends ThriftStruct<?>> T assertFullyPopulated(
      T object,
      ThriftFields... ignoredFields) {

    assertFullyPopulated(
        object.getClass().getSimpleName(),
        object,
        ImmutableSet.copyOf(ignoredFields));
    return object;
  }

  /**
   * Convenience method to get a field by name from a class, to pass as an ignored field to
   * {@link #assertFullyPopulated(String, Object, Set)}.
   *
   * @param clazz Class to get a field from.
   * @param field Field name.
   * @return Field with the given {@code name}.
   */
  public static Field getField(Class<?> clazz, String field) {
    for (Field f : clazz.getDeclaredFields()) {
      if (f.getName().equals(field)) {
        return f;
      }
    }
    throw new IllegalArgumentException("Field not found: " + field);
  }
}
