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

import org.apache.shiro.authz.UnauthenticatedException;

/**
 * A special error type designed to tunnel through the normal thrift exception handling layers.
 *
 * This permits catching and re-throwing (see: {@link #authenticationChallenge()}) a special
 * authentication challenge initiating exception to the servlet filter layer which then communicates
 * this at the HTTP protocol layer.
 */
public class UnauthenticatedError extends Error {
  UnauthenticatedError() {
    super();
  }

  /**
   * Throws a special exception type that will be handled in higher servlet filter layers to trigger
   * an authentication challenge response at the HTTP protocol layer.
   *
   * @return A runtime exception to throw to satisfy the compiler.
   */
  public RuntimeException authenticationChallenge() {
    throw new UnauthenticatedException();
  }
}
