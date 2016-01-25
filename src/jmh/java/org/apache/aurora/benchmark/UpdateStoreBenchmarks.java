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
package org.apache.aurora.benchmark;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.db.DbUtil;
import org.apache.thrift.TException;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

public class UpdateStoreBenchmarks {

  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
  @Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
  @Fork(1)
  @State(Scope.Thread)
  public static class JobDetailsBenchmark {
    private static final String USER = "user";
    private Storage storage;
    private Set<JobUpdateKey> keys;

    @Param({"1000", "5000", "10000"})
    private int instances;

    @Setup(Level.Trial)
    public void setUp() {
      storage = DbUtil.createStorage();
    }

    @Setup(Level.Iteration)
    public void setUpIteration() {
      storage.write((NoResult.Quiet) storeProvider -> {
        JobUpdateStore.Mutable updateStore = storeProvider.getJobUpdateStore();
        Set<JobUpdateDetails> updates =
            new JobUpdates.Builder().setNumInstanceEvents(instances).build(1);

        ImmutableSet.Builder<JobUpdateKey> keyBuilder = ImmutableSet.builder();
        for (JobUpdateDetails details : updates) {
          JobUpdateKey key = details.getUpdate().getSummary().getKey();
          keyBuilder.add(key);
          String lockToken = UUID.randomUUID().toString();
          storeProvider.getLockStore().saveLock(
              Lock.create(LockKey.job(key.getJob()), lockToken, USER, 0L));

          updateStore.saveJobUpdate(details.getUpdate(), Optional.of(lockToken));

          for (JobUpdateEvent updateEvent : details.getUpdateEvents()) {
            updateStore.saveJobUpdateEvent(key, updateEvent);
          }

          for (JobInstanceUpdateEvent instanceEvent : details.getInstanceEvents()) {
            updateStore.saveJobInstanceUpdateEvent(key, instanceEvent);
          }
        }
        keys = keyBuilder.build();
      });
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
      storage.write((NoResult.Quiet) storeProvider -> {
        storeProvider.getJobUpdateStore().deleteAllUpdatesAndEvents();
        storeProvider.getLockStore().deleteLocks();
      });
    }

    @Benchmark
    public JobUpdateDetails run() throws TException {
      return storage.read(store -> store.getJobUpdateStore().fetchJobUpdateDetails(
          Iterables.getOnlyElement(keys)).get());
    }
  }
}
