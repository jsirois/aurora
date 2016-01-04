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

/**
 * An immutable thrift union.
 *
 * @param <F> The type of the thrift fields contained by this union.
 */
public interface ThriftUnion<F extends ThriftFields> extends ThriftEntity<F> {

  /**
   * Creates a new union instance for a given union field value.
   *
   * @param type The type of the thrift union to create.
   * @param field The field of the union member to instantiate.
   * @param value The union member to instantiate.
   * @param <F> The type of the thrift fields contained by the union {@code U}.
   * @param <U> The type of the thrift union to be created.
   * @return A new union instance.
   */
  static <F extends ThriftFields, U extends ThriftUnion<F>> U create(
      Class<U> type,
      F field,
      Object value) {

    try {
      return type.getConstructor(field.getFieldClass()).newInstance(value);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Returns the union field this union instance has set.
   *
   * @return The set union field.
   */
  F getSetField();

  /**
   * Returns the value of this union's set field.
   *
   * @return The set union value.
   */
  Object getFieldValue();
}
