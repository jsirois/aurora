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

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import org.immutables.value.Value;

/**
 * Encodes a set of thrift type annotations.
 *
 * Although not widely known, the thrift grammar supports type annotations on thrift types.
 * See the grammar for more details (search for TypeAnnotation):
 *   https://git-wip-us.apache.org/repos/asf?p=thrift.git;a=blob;f=compiler/cpp/src/thrifty.yy
 */
@Value.Immutable
@Retention(RetentionPolicy.RUNTIME)
public @interface ThriftAnnotations {

  /**
   * The annotation name-value pairs.
   *
   * @return A list of one or more annotations name-value pairs.
   */
  ThriftAnnotation[] value();
}
