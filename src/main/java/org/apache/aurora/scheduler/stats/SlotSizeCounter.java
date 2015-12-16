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
package org.apache.aurora.scheduler.stats;

import java.util.Map;
import java.util.Objects;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;

import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.scheduler.ResourceAggregates;

import static java.util.Objects.requireNonNull;

/**
 * A stat computer that aggregates the number of 'slots' available at different pre-determined
 * slot sizes, broken down by dedicated and non-dedicated hosts.
 */
class SlotSizeCounter implements Runnable {
  private static final Map<String, ResourceAggregate> SLOT_SIZES = ImmutableMap.of(
      "small", ResourceAggregates.SMALL,
      "medium", ResourceAggregates.MEDIUM,
      "large", ResourceAggregates.LARGE,
      "xlarge", ResourceAggregates.XLARGE);

  // Ensures all counters are always initialized regardless of the Resource availability.
  private static final Iterable<String> SLOT_GROUPS = ImmutableList.of(
      getPrefix(false, false),
      getPrefix(false, true),
      getPrefix(true, false),
      getPrefix(true, true)
  );

  private final Map<String, ResourceAggregate> slotSizes;
  private final MachineResourceProvider machineResourceProvider;
  private final CachedCounters cachedCounters;

  @VisibleForTesting
  SlotSizeCounter(
      final Map<String, ResourceAggregate> slotSizes,
      MachineResourceProvider machineResourceProvider,
      CachedCounters cachedCounters) {

    this.slotSizes = requireNonNull(slotSizes);
    this.machineResourceProvider = requireNonNull(machineResourceProvider);
    this.cachedCounters = requireNonNull(cachedCounters);
  }

  static class MachineResource {
    private final ResourceAggregate size;
    private final boolean dedicated;
    private final boolean revocable;

    MachineResource(ResourceAggregate size, boolean dedicated, boolean revocable) {
      this.size = requireNonNull(size);
      this.dedicated = dedicated;
      this.revocable = revocable;
    }

    public ResourceAggregate getSize() {
      return size;
    }

    public boolean isDedicated() {
      return dedicated;
    }

    public boolean isRevocable() {
      return revocable;
    }

    @Override
    public int hashCode() {
      return Objects.hash(size, dedicated, revocable);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof MachineResource)) {
        return false;
      }

      MachineResource other = (MachineResource) obj;
      return Objects.equals(size, other.size)
          && Objects.equals(dedicated, other.dedicated)
          && Objects.equals(revocable, other.revocable);
    }
  }

  interface MachineResourceProvider {
    Iterable<MachineResource> get();
  }

  @Inject
  SlotSizeCounter(MachineResourceProvider machineResourceProvider, CachedCounters cachedCounters) {
    this(SLOT_SIZES, machineResourceProvider, cachedCounters);
  }

  private static String getPrefix(boolean dedicated, boolean revocable) {
    String dedicatedSuffix = dedicated ? "dedicated_" : "";
    String revocableSuffix = revocable ? "revocable_" : "";
    return "empty_slots_" + dedicatedSuffix + revocableSuffix;
  }

  @VisibleForTesting
  static String getStatName(String slotName, boolean dedicated, boolean revocable) {
    return getPrefix(dedicated, revocable) + slotName;
  }

  private int countSlots(Iterable<ResourceAggregate> slots, final ResourceAggregate slotSize) {
    Function<ResourceAggregate, Integer> counter = new Function<ResourceAggregate, Integer>() {
      @Override
      public Integer apply(ResourceAggregate machineSlack) {
        return ResourceAggregates.divide(machineSlack, slotSize);
      }
    };

    int sum = 0;
    for (int slotCount : FluentIterable.from(slots).transform(counter)) {
      sum += slotCount;
    }
    return sum;
  }

  private void updateStats(
      String name,
      Iterable<MachineResource> slots,
      ResourceAggregate slotSize) {

    ImmutableMultimap.Builder<String, ResourceAggregate> builder = ImmutableMultimap.builder();
    for (MachineResource slot : slots) {
      builder.put(getStatName(name, slot.isDedicated(), slot.isRevocable()), slot.getSize());
    }

    ImmutableMultimap<String, ResourceAggregate> sizes = builder.build();

    for (String slotGroup : SLOT_GROUPS) {
      String statName = slotGroup + name;
      cachedCounters.get(statName).set(countSlots(sizes.get(statName), slotSize));
    }
  }

  @Override
  public void run() {
    Iterable<MachineResource> slots = machineResourceProvider.get();
    slotSizes.entrySet().stream().forEach(e -> updateStats(e.getKey(), slots, e.getValue()));
  }
}
