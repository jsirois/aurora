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

import com.google.common.base.Optional;

final class FieldGetters {
  private FieldGetters() {
    // Utility class.
  }

  public static <P, C, G>
      FieldGetter<P, G> compose(final FieldGetter<P, C> parent, final FieldGetter<C, G> child) {

    return new FieldGetter.AbstractFieldGetter<P, G>(parent.getStructClass()) {
      @Override public Optional<G> apply(P input) {
        Optional<C> parentValue = parent.apply(input);
        if (parentValue.isPresent()) {
          return child.apply(parentValue.get());
        } else {
          return Optional.absent();
        }
      }
    };
  }
}
