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

import org.apache.thrift.TFieldIdEnum;

/**
 * Metadata for the fields in a thrift struct or union type.
 */
public interface ThriftFields extends TFieldIdEnum {

  /**
   * The type of the field; will carry generic type information if the field is parametrized.
   *
   * @return The field type.
   */
  Type getFieldType();

  /**
   * The (raw) class of the field.
   *
   * @return The field class.
   */
  Class<?> getFieldClass();

  /**
   * A thrift fields type for structs or unions with no fields.
   */
  abstract class NoFields implements ThriftFields {
    private NoFields() {
      // NoFields can never be extended so no fields can ever be added.
    }
  }
}
