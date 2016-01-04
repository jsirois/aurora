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
 * An immutable thrift struct.
 *
 * @param <T> The type of the thrift fields contained by this struct.
 */
public interface ThriftStruct<T extends ThriftFields> extends ThriftEntity<T> {

  /**
   * Creates a builder for a thrift struct of type {@code S}.
   *
   * @param type The type of thrift struct to create a builder for.
   * @param <F> The type of the thrift fields contained by the struct {@code S}.
   * @param <S> The type of the thrift struct to create a builder for.
   * @return A new builder that can build structs of type {@code S}.
   */
  static <F extends ThriftFields, S extends ThriftStruct<F>> Builder<F, S> builder(
      Class<S> type) {

    try {
      // We know (and control) that all generated structs have a static method of this signature.
      @SuppressWarnings("unchecked")
      Builder<F, S> builder = (Builder<F, S>) type.getMethod("builder").invoke(null);
      return builder;
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * A builder that can be used to generate immutable thrift structs of type {@code S}.
   *
   * @param <F> The type of the thrift fields contained by the struct {@code S}.
   * @param <S> The type of the thrift struct that can be built with this builder.
   */
  interface Builder<F extends ThriftFields, S extends ThriftStruct<F>> {

    /**
     * Sets a field to be populated in the thrift struct when it is eventually
     * {@link #build() built}.
     * <p>
     * A given field can be set multiple times in between {@link #build()} calls, in which case the
     * most recent setting for the field will be used in the built thrift struct.
     *
     * @param field The field to populate.
     * @param value The field's value.
     * @return This builder for chaining.
     */
    Builder<F, S> set(F field, Object value);

    /**
     * Builds a thrift struct with its fields populated as specified by prior {@link #set} calls.
     * <p>
     * Note that {@code build} can be called multiple times, with or without additional {@link #set}
     * calls having been made in-between, and each {@code build} call will produce an independent
     * immutable thrift struct with the most recent builder values for each {@link #set} field.
     *
     * @return A built thrift struct.
     * @throws IllegalStateException If any required fields were not set.
     */
    S build() throws IllegalStateException;
  }
}
