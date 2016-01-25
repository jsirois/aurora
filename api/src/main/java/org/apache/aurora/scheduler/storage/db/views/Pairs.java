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
package org.apache.aurora.scheduler.storage.db.views;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;

import static java.util.Objects.requireNonNull;

/**
 * Utility class for translating collections of {@link Pair} to and from maps.
 */
public final class Pairs {

  public static class Pair<A, B> {
    private final A first;
    private final B second;

    public Pair(@Nullable A first,@Nullable B second) {
      this.first = requireNonNull(first);
      this.second = requireNonNull(second);
    }

    @Nullable
    public A getFirst() {
      return first;
    }

    @Nullable
    public B getSecond() {
      return second;
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) { return true; }
      if (!(o instanceof Pair)) { return false; }

      Pair<?, ?> that = (Pair<?, ?>) o;
      return Objects.equals(first, that.first) &&
          Objects.equals(second, that.second);
    }

    @Override
    public String toString() {
      return String.format("(%s, %s)", getFirst(), getSecond());
    }

    @Override
    public int hashCode() {
      return Objects.hash(first, second);
    }
  }

  private Pairs() {
    // Utility class.
  }

  public static <K, V> Map<K, V> toMap(Iterable<Pair<K, V>> pairs) {
    ImmutableMap.Builder<K, V> map = ImmutableMap.builder();
    for (Pair<K, V> pair : pairs) {
      map.put(pair.getFirst(), pair.getSecond());
    }
    return map.build();
  }

  public static <K, V> List<Pairs.Pair<K, V>> fromMap(Map<K, V> map) {
    return FluentIterable.from(map.entrySet())
        .transform(e -> new Pairs.Pair<>(e.getKey(), e.getValue()))
        .toList();
  }
}
