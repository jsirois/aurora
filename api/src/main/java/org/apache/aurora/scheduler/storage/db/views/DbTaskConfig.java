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
import java.util.Set;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.MesosContainer;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.peer.MutableConstraint;
import org.apache.aurora.gen.peer.MutableExecutorConfig;
import org.apache.aurora.gen.peer.MutableIdentity;
import org.apache.aurora.gen.peer.MutableJobKey;
import org.apache.aurora.gen.peer.MutableMetadata;

public final class DbTaskConfig {
  private long rowId;
  private MutableJobKey job;
  private MutableIdentity owner;
  private String environment;
  private String jobName;
  private boolean isService;
  private double numCpus;
  private long ramMb;
  private long diskMb;
  private int priority;
  private int maxTaskFailures;
  private boolean production;
  private Set<MutableConstraint> constraints;
  private Set<String> requestedPorts;
  private List<Pairs.Pair<String, String>> taskLinks;
  private String contactEmail;
  private MutableExecutorConfig executorConfig;
  private Set<MutableMetadata> metadata;
  private DbContainer container;
  private String tier;

  private DbTaskConfig() {
  }

  public long getRowId() {
    return rowId;
  }

  public TaskConfig toThrift() {
    return TaskConfig.builder()
        .setJob(job.toThrift())
        .setOwner(owner.toThrift())
        .setEnvironment(environment)
        .setJobName(jobName)
        .setIsService(isService)
        .setNumCpus(numCpus)
        .setRamMb(ramMb)
        .setDiskMb(diskMb)
        .setPriority(priority)
        .setMaxTaskFailures(maxTaskFailures)
        .setProduction(production)
        .setTier(tier)
        .setConstraints(FluentIterable.from(constraints)
            .transform(MutableConstraint::toThrift)
            .toSet())
        .setRequestedPorts(ImmutableSet.copyOf(requestedPorts))
        .setTaskLinks(Pairs.toMap(taskLinks))
        .setContactEmail(contactEmail)
        .setExecutorConfig(executorConfig.toThrift())
        .setMetadata(FluentIterable.from(metadata).transform(MutableMetadata::toThrift).toSet())
        // TODO(John Sirois): The default constructed below is accurately modelled as the builder
        // default as taken from the api.thrift default - consider only setting when non-null.
        .setContainer(
            container == null ? Container.mesos(MesosContainer.create()) : container.toThrift())
        .build();
  }
}
