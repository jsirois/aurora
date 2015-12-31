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
package org.apache.aurora.build;

import java.io.IOException;

import com.facebook.swift.parser.visitor.Visitable;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

interface Visitor<T extends Visitable> {
  static Visitor<?> failing() {
    return failing(Optional.absent());
  }

  static Visitor<?> failing(String reason) {
    return failing(Optional.of(reason));
  }

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

  default void visit(T visitable) throws IOException {
    // noop
  }

  default void finish(ImmutableMap<String, AbstractStructRenderer> structRenderers)
      throws IOException {
    // noop
  }
}
