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
package org.apache.aurora.thrift;

import com.google.common.collect.ImmutableSet;

/**
 * An immutable thrift struct or union.
 *
 * @param <F> The type of the thrift fields contained by this thrift entity.
 */
public interface ThriftEntity<F extends ThriftFields> {

  /**
   * Returns the thrift fields contained by the given thrift entity.
   *
   * @param type The type of the thrift entity to return fields for.
   * @param <F> The type of the thrift fields contained by the thrift entity of type {@code E}.
   * @param <E> The type of the thrift entity to return fields for.
   * @return The set of fields the given thrift entity contains.
   */
  static <F extends ThriftFields, E extends ThriftEntity<F>> ImmutableSet<F> fields(Class<E> type) {
    try {
      // We know (and control) that all generated entities have a static method of this signature.
      @SuppressWarnings("unchecked")
      ImmutableSet<F> fields = (ImmutableSet<F>) type.getMethod("fields").invoke(null);
      return fields;
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Checks if a given {@code field} has been set on this thrift entity.
   *
   * @param field The field to check.
   * @return {@code true} if the given field has been set on this thrift entity.
   */
  boolean isSet(F field);

  /**
   * Returns the value of the given {@code field} if it has been set.
   *
   * @param field The field whose value to retrieve.
   * @return The field's set value.
   * @throws IllegalStateException if the given field is not set.
   */
  Object getFieldValue(F field) throws IllegalArgumentException;

  /**
   * Returns this thrift entity's fields.
   *
   * @return The set of fields this thrift entity contains.
   */
  ImmutableSet<F> getFields();
}
