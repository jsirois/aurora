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
   * Adds a human-friendly message to a response, usually to indicate a problem or deprecation
   * encountered while handling the request.
   *
   * @param message Message to include in the response.
   * @return {@code response} with {@code message} included.
   */
  public static Response message(String message) {
    return Response.builder().setDetails(ResponseDetail.create(message)).build();
  }

  /**
   * Identical to {@link #message(String)} that also applies a response code.
   *
   * @param code Response code to include.
   * @param message Message to include in the response.
   * @return {@code response} with {@code message} included.
   * @see {@link #message(String)}
   */
  public static Response message(ResponseCode code, String message) {
    return Response.builder()
        .setResponseCode(code)
        .setDetails(ResponseDetail.create(message))
        .build();
  }

  /**
   * Identical to {@link #message(String)} that also applies a response code and
   * extracts a message from the provided {@link Throwable}.
   *
   * @param code Response code to include.
   * @param throwable {@link Throwable} to extract message from.
   * @return {@link #message(String)}
   */
  public static Response error(ResponseCode code, Throwable throwable) {
    return Response.builder()
        .setResponseCode(code)
        .setDetails(ResponseDetail.create(throwable.getMessage()))
        .build();
  }

  /**
   * Creates an ERROR response that has a single associated error message.
   *
   * @param message The error message.
   * @return A response with an ERROR code set containing the message indicated.
   */
  public static Response error(String message) {
    return message(ERROR, message);
  }

  /**
   * Creates an OK response that has no result entity.
   *
   * @return Ok response with an empty result.
   */
  public static Response ok()  {
    return Response.builder().setResponseCode(OK).build();
  }

  static Response invalidRequest(String message) {
    return message(INVALID_REQUEST, message);
  }

  static Response ok(Result result) {
    return Response.builder().setResponseCode(OK).setResult(result).build();
  }
}
