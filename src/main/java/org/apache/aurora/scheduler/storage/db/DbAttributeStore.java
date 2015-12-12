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
package org.apache.aurora.scheduler.storage.db;

import java.util.Objects;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

import org.apache.aurora.GuavaUtils;
import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.peer.MutableHostAttributes;
import org.apache.aurora.scheduler.storage.AttributeStore;

import static com.google.common.base.Preconditions.checkArgument;

import static org.apache.aurora.common.base.MorePreconditions.checkNotBlank;
import static org.apache.aurora.common.inject.TimedInterceptor.Timed;

/**
 * Attribute store backed by a relational database.
 */
class DbAttributeStore implements AttributeStore.Mutable {

  private final AttributeMapper mapper;

  @Inject
  DbAttributeStore(AttributeMapper mapper) {
    this.mapper = Objects.requireNonNull(mapper);
  }

  @Override
  public void deleteHostAttributes() {
    mapper.truncate();
  }

  @Timed("attribute_store_save")
  @Override
  public boolean saveHostAttributes(HostAttributes hostAttributes) {
    checkNotBlank(hostAttributes.getHost());
    checkArgument(hostAttributes.isSetMode());

    if (Iterables.any(hostAttributes.getAttributes(), EMPTY_VALUES)) {
      throw new IllegalArgumentException(
          "Host attributes contains empty values: " + hostAttributes);
    }

    Optional<HostAttributes> existing = getHostAttributes(hostAttributes.getHost());
    if (existing.equals(Optional.of(hostAttributes))) {
      return false;
    } else if (existing.isPresent()) {
      mapper.updateHostModeAndSlaveId(
          hostAttributes.getHost(),
          hostAttributes.getMode(),
          hostAttributes.getSlaveId());
    } else {
      mapper.insert(hostAttributes);
    }

    mapper.deleteAttributeValues(hostAttributes.getHost());
    if (!hostAttributes.getAttributes().isEmpty()) {
      mapper.insertAttributeValues(hostAttributes);
    }

    return true;
  }

  private static final Predicate<Attribute> EMPTY_VALUES = new Predicate<Attribute>() {
    @Override
    public boolean apply(Attribute attribute) {
      return attribute.getValues().isEmpty();
    }
  };

  @Timed("attribute_store_fetch_one")
  @Override
  public Optional<HostAttributes> getHostAttributes(String host) {
    return Optional.fromNullable(mapper.select(host)).transform(MutableHostAttributes::toThrift);
  }

  @Timed("attribute_store_fetch_all")
  @Override
  public Set<HostAttributes> getHostAttributes() {
    return mapper.selectAll().stream()
        .map(MutableHostAttributes::toThrift)
        .collect(GuavaUtils.toImmutableSet());
  }
}
