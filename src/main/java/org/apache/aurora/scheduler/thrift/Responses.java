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
package org.apache.aurora.scheduler.thrift;

import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.ResponseCode;
import org.apache.aurora.gen.ResponseDetail;
import org.apache.aurora.gen.Result;

import static org.apache.aurora.gen.ResponseCode.ERROR;
import static org.apache.aurora.gen.ResponseCode.INVALID_REQUEST;
import static org.apache.aurora.gen.ResponseCode.OK;

/**
 * Utility class for constructing responses to API calls.
 */
public final class Responses {

  private Responses() {
    // Utility class.
  }

  /**
   * Creates an {@link ResponseCode#ERROR} response that has a single associated error message.
   *
   * @param message The error message.
   * @return A response with an {@code ERROR} code set containing the message indicated.
   */
  public static Response error(String message) {
    return create(ERROR, message);
  }

  /**
   * Creates an {@link ResponseCode#ERROR} response that has a single associated error message
   * formed from the given error's message.
   *
   * @param error The error that occurred.
   * @return A response with an {@code ERROR} code set containing the message indicated.
   */
  public static Response error(Throwable error) {
    return error(error.getMessage());
  }

  /**
   * Creates a response with the given error code that has a single associated error message formed
   * from the given error's message.
   *
   * @param code The response error code.
   * @param error The error that occurred.
   * @return A response with an {@code ERROR} code set containing the message indicated.
   */
  public static Response error(ResponseCode code, Throwable error) {
    return create(code, error.getMessage());
  }

  private static Response withDetail(Response.Builder builder, String message) {
    return builder.setDetails(ResponseDetail.create(message)).build();
  }

  /**
   * Creates a response with the given code an a single associated detail message.
   *
   * @param code The response code.
   * @param message The detail message.
   * @return A response with the given code set containing the detail message indicated.
   */
  public static Response create(ResponseCode code, String message) {
    return withDetail(responseBuilder(code), message);
  }

  private static Response.Builder responseBuilder(ResponseCode code) {
    return Response.builder().setResponseCode(code);
  }

  private static Response.Builder okBuilder() {
    return responseBuilder(OK);
  }

  /**
   * Creates an {@link ResponseCode#OK} response that has no result entity or associated detail
   * messages.
   *
   * @return A response with an {@code OK} code set.
   */
  public static Response ok()  {
    return okBuilder().build();
  }

  static Response ok(Result result) {
    return okBuilder().setResult(result).build();
  }

  static Response ok(String message) {
    return withDetail(okBuilder(), message);
  }

  /**
   * Creates an {@link ResponseCode#INVALID_REQUEST} response that has a single associated error
   * message.
   *
   * @param message TThe error message.
   * @return A response with an {@code INVALID_REQUEST} code set containing the message indicated.
   */
  public static Response invalidRequest(String message) {
    return create(INVALID_REQUEST, message);
  }
}
