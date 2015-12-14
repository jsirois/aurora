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

import java.lang.reflect.Type;

import com.google.common.base.Optional;

import org.apache.aurora.scheduler.http.api.security.FieldGetter.AbstractFieldGetter;
import org.apache.aurora.thrift.ThriftEntity;
import org.apache.aurora.thrift.ThriftEntity.ThriftFields;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Retrieves an optional struct-type field from a struct.
 */
class ThriftFieldGetter<
    T extends ThriftEntity<F>,
    F extends ThriftFields,
    V extends ThriftEntity<?>>
    extends AbstractFieldGetter<T, V> {

  private final F fieldId;

  ThriftFieldGetter(Class<T> structClass, F fieldId, Type valueType) {
    super(structClass);

    checkArgument(
        valueType.equals(fieldId.getFieldType()),
        "Value class "
            + valueType.getTypeName()
            + " does not match field metadata for "
            + fieldId
            + " (expected " + fieldId.getFieldType().getTypeName()
            + ").");

    this.fieldId = fieldId;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Optional<V> apply(T input) {
    if (input.isSet(fieldId)) {
      return Optional.of((V) input.getFieldValue(fieldId));
    } else {
      return Optional.absent();
    }
  }
}
