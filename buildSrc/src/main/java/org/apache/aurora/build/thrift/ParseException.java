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
package org.apache.aurora.build.thrift;

/**
 * Indicates an unexpected semantic parsing error.
 *
 * If thrown, the thrift IDL was itself was valid, but it expressed relationships not supported
 * by the thrift spec.
 */
class ParseException extends RuntimeException {
  ParseException(String message) {
    super(message);
  }
}
