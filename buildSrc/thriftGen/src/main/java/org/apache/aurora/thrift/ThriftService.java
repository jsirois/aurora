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

import java.lang.reflect.Method;

import com.google.common.collect.ImmutableMap;

/**
 * A thrift service endpoint that can provide metadata for its methods.
 */
public interface ThriftService extends AutoCloseable {

  /**
   * Return a mapping of unique method names to method objects that can be used to reflectively
   * invoke the corresponding method.
   *
   * @param serviceType The service type whose methods to return.
   * @return The method metadata for this service.
   */
  static ImmutableMap<String, Method> getThriftMethods(Class<? extends ThriftService> serviceType) {
    try {
      // We know (and control) that all generated services have a static method of this signature.
      @SuppressWarnings("unchecked")
      ImmutableMap<String, Method> methods =
          (ImmutableMap<String, Method>) serviceType.getMethod("getThriftMethods").invoke(null);
      return methods;
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }
}
