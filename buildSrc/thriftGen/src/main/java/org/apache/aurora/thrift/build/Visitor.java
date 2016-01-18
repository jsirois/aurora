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
package org.apache.aurora.thrift.build;

import java.io.IOException;

import com.facebook.swift.parser.visitor.Visitable;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.squareup.javapoet.ClassName;

/**
 * A visitor of thrift definitions - typically a code generator.
 * <p>
 * Visitors can either act on each definition as it is parsed by handling
 * {@link #visit(Visitable)} or else act upon the full set of definitions when the parse is
 * {@link #finish(ImmutableMap) finished}.  If the visitor chooses to do the latter, it is
 * responsible for collecting definitions for {@link #finish(ImmutableMap)} to act upon by
 * implementing definition collection in {@link #visit(Visitable)}.
 *
 * @param <T> The type of thrift definition this visitor can handle.
 */
interface Visitor<T extends Visitable> {

  /**
   * Equivalent to calling {@link #failing(Optional)} with {@code Optional.absent()}.
   *
   * @return A visitor that always fails.
   */
  static Visitor<?> failing() {
    return failing(Optional.absent());
  }

  /**
   * Equivalent to calling {@link #failing(Optional)} with {@code Optional.of(reason)}.
   *
   * @return A visitor that always fails with the given {@code reason}.
   */
  static Visitor<?> failing(String reason) {
    return failing(Optional.of(reason));
  }

  /**
   * Creates a visitor that will always fail by throwing {@link UnsupportedFeatureException}.
   * If a {@code reason} is given, the exception message will be augmented with it.
   *
   * @return A visitor that always fails.
   */
  static Visitor<?> failing(Optional<String> reason) {
    return new Visitor<Visitable>() {
      @Override
      public void visit(Visitable visitable) throws IOException {
        String msg = String.format("Unsupported thrift IDL type: %s", visitable);
        if (reason.isPresent()) {
          msg = String.format("%s%n%s", msg, reason.get());
        }
        throw new UnsupportedFeatureException(msg);
      }
    };
  }

  /**
   * Visits a definition, performing some work - typically code generation.
   *
   * @param visitable The parsed definition to visit.
   * @throws IOException If there is a problem emitting code for or otherwise processing
   *                     {@code visitable}.
   */
  default void visit(T visitable) throws IOException {
    // noop
  }

  /**
   * Finalizes processing at the end of a file parse.
   *
   * @param structRenderers A mapping of types to renderers of those types' literal values.
   * @throws IOException If there is a problem emitting code for or otherwise processing
   *                     {@code visitable}.
   */
  default void finish(ImmutableMap<ClassName, AbstractStructRenderer> structRenderers)
      throws IOException {
    // noop
  }
}
