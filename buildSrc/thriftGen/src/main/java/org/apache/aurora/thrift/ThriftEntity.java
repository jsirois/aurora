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

import java.lang.reflect.Type;

import com.google.common.collect.ImmutableSet;

import org.apache.thrift.TFieldIdEnum;

public interface ThriftEntity<T extends ThriftEntity.ThriftFields> {
  static <F extends ThriftFields, S extends ThriftEntity<F>> ImmutableSet<F> fields(Class<S> type) {
    try {
      @SuppressWarnings("unchecked")
      ImmutableSet<F> fields = (ImmutableSet<F>) type.getMethod("fields").invoke(null);
      return fields;
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  boolean isSet(T field);

  Object getFieldValue(T field);

  ImmutableSet<T> getFields();

  interface ThriftFields extends TFieldIdEnum {
    Type getFieldType();

    Class getFieldClass();

    abstract class NoFields implements ThriftFields {
      private NoFields() {
        // NoFields can never be extended so no fields can
        // ever be added.
      }
    }
  }

  interface ThriftStruct<T extends ThriftFields> extends ThriftEntity<T> {
    static <F extends ThriftFields, S extends ThriftStruct<F>> Builder<F, S> builder(
        Class<S> type) {

      try {
        @SuppressWarnings("unchecked")
        Builder<F, S> builder = (Builder<F, S>) type.getMethod("builder").invoke(null);
        return builder;
      } catch (ReflectiveOperationException e) {
        throw new IllegalStateException(e);
      }
    }

    interface Builder<F extends ThriftFields, S extends ThriftStruct<F>> {
      Builder<F, S> set(F field, Object value);

      S build();
    }
  }

  interface ThriftUnion<T extends ThriftFields> extends ThriftEntity<T> {
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

    T getSetField();

    Object getFieldValue();
  }
}
