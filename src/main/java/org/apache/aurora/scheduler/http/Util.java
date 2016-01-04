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

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import org.apache.aurora.thrift.ThriftFields;
import org.apache.aurora.thrift.ThriftStruct;

/**
 * Utility functions for thrift.
 *
 * @author William Farner
 */
final class Util {
  /**
   * Pretty-prints a thrift object contents.
   *
   * @param t The thrift object to print.
   * @return The pretty-printed version of the thrift object.
   */
  static <T extends ThriftFields> String prettyPrint(ThriftStruct<T> t) {
    return t == null ? "null" : printStruct(t, 0);
  }

  /**
   * Prints an object contained in a thrift message.
   *
   * @param o The object to print.
   * @param depth The print nesting level.
   * @return The pretty-printed version of the thrift field.
   */
  private static String printValue(Object o, int depth) {
    if (o == null) {
      return "null";
    } else if (ThriftStruct.class.isAssignableFrom(o.getClass())) {
      // Since we know ThriftStruct seals in it own ThriftFields subtype, this is always safe.
      @SuppressWarnings("unchecked")
      ThriftStruct<ThriftFields> t = (ThriftStruct<ThriftFields>) o;

      return "\n" + printStruct(t, depth + 1);
    } else if (Map.class.isAssignableFrom(o.getClass())) {
      return printMap((Map) o, depth + 1);
    } else if (List.class.isAssignableFrom(o.getClass())) {
      return printList((List) o, depth + 1);
    } else if (Set.class.isAssignableFrom(o.getClass())) {
      return printSet((Set) o, depth + 1);
    } else if (String.class == o.getClass()) {
      return '"' + o.toString() + '"';
    } else {
      return o.toString();
    }
  }

  /**
   * Prints a ThriftStruct.
   *
   * @param struct The object to print.
   * @param depth The print nesting level.
   * @return The pretty-printed version of the TBase.
   */
  private static <T extends ThriftFields> String printStruct(
      ThriftStruct<T> struct,
      int depth) {

    List<String> fields = Lists.newArrayList();
    for (T field : struct.getFields()) {
      boolean fieldSet = struct.isSet(field);
      String strValue;
      if (fieldSet) {
        Object value = struct.getFieldValue(field);
        strValue = printValue(value, depth);
      } else {
        strValue = "not set";
      }
      fields.add(tabs(depth) + field.getFieldName() + ": " + strValue);
    }

    return Joiner.on("\n").join(fields);
  }

  /**
   * Prints a map in a style that is consistent with TBase pretty printing.
   *
   * @param map The map to print
   * @param depth The print nesting level.
   * @return The pretty-printed version of the map.
   */
  private static String printMap(Map<?, ?> map, int depth) {
    List<String> entries = Lists.newArrayList();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      entries.add(tabs(depth) + printValue(entry.getKey(), depth)
          + " = " + printValue(entry.getValue(), depth));
    }

    return entries.isEmpty() ? "{}"
        : String.format("{%n%s%n%s}", Joiner.on(",\n").join(entries), tabs(depth - 1));
  }

  /**
   * Prints a list in a style that is consistent with TBase pretty printing.
   *
   * @param list The list to print
   * @param depth The print nesting level.
   * @return The pretty-printed version of the list
   */
  private static String printList(List<?> list, int depth) {
    List<String> entries = Lists.newArrayList();
    for (int i = 0; i < list.size(); i++) {
      entries.add(
          String.format("%sItem[%d] = %s", tabs(depth), i, printValue(list.get(i), depth)));
    }

    return entries.isEmpty() ? "[]"
        : String.format("[%n%s%n%s]", Joiner.on(",\n").join(entries), tabs(depth - 1));
  }
  /**
   * Prints a set in a style that is consistent with TBase pretty printing.
   *
   * @param set The set to print
   * @param depth The print nesting level.
   * @return The pretty-printed version of the set
   */
  private static String printSet(Set<?> set, int depth) {
    List<String> entries = Lists.newArrayList();
    for (Object item : set) {
      entries.add(
          String.format("%sItem = %s", tabs(depth), printValue(item, depth)));
    }

    return entries.isEmpty() ? "{}"
        : String.format("{%n%s%n%s}", Joiner.on(",\n").join(entries), tabs(depth - 1));
  }

  private static String tabs(int n) {
    return Strings.repeat("  ", n);
  }

  private Util() {
    // Utility class.
  }
}
