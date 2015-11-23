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
package org.apache.aurora.scheduler.api.rest;

import com.facebook.swift.codec.ThriftCodec;
import com.facebook.swift.codec.ThriftCodecManager;
import com.facebook.swift.codec.internal.compiler.CompilerThriftCodecFactory;
import com.facebook.swift.codec.metadata.MetadataErrorException;
import com.facebook.swift.codec.metadata.MetadataErrors;
import com.facebook.swift.codec.metadata.MetadataWarningException;
import com.facebook.swift.codec.metadata.ThriftCatalog;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.rest.gen.APIVersion;
import org.apache.aurora.rest.gen.AcquireLockResult;
import org.apache.aurora.rest.gen.AddInstancesConfig;
import org.apache.aurora.rest.gen.AssignedTask;
import org.apache.aurora.rest.gen.Attribute;
import org.apache.aurora.rest.gen.ConfigGroup;
import org.apache.aurora.rest.gen.ConfigRewrite;
import org.apache.aurora.rest.gen.ConfigSummary;
import org.apache.aurora.rest.gen.ConfigSummaryResult;
import org.apache.aurora.rest.gen.Constraint;
import org.apache.aurora.rest.gen.Container;
import org.apache.aurora.rest.gen.CronCollisionPolicy;
import org.apache.aurora.rest.gen.DockerContainer;
import org.apache.aurora.rest.gen.DockerParameter;
import org.apache.aurora.rest.gen.DrainHostsResult;
import org.apache.aurora.rest.gen.EndMaintenanceResult;
import org.apache.aurora.rest.gen.ExecutorConfig;
import org.apache.aurora.rest.gen.GetJobUpdateDetailsResult;
import org.apache.aurora.rest.gen.GetJobUpdateDiffResult;
import org.apache.aurora.rest.gen.GetJobUpdateSummariesResult;
import org.apache.aurora.rest.gen.GetJobsResult;
import org.apache.aurora.rest.gen.GetLocksResult;
import org.apache.aurora.rest.gen.GetPendingReasonResult;
import org.apache.aurora.rest.gen.GetQuotaResult;
import org.apache.aurora.rest.gen.HostAttributes;
import org.apache.aurora.rest.gen.HostStatus;
import org.apache.aurora.rest.gen.Hosts;
import org.apache.aurora.rest.gen.Identity;
import org.apache.aurora.rest.gen.InstanceConfigRewrite;
import org.apache.aurora.rest.gen.InstanceKey;
import org.apache.aurora.rest.gen.InstanceTaskConfig;
import org.apache.aurora.rest.gen.JobConfigRewrite;
import org.apache.aurora.rest.gen.JobConfiguration;
import org.apache.aurora.rest.gen.JobInstanceUpdateEvent;
import org.apache.aurora.rest.gen.JobKey;
import org.apache.aurora.rest.gen.JobStats;
import org.apache.aurora.rest.gen.JobSummary;
import org.apache.aurora.rest.gen.JobSummaryResult;
import org.apache.aurora.rest.gen.JobUpdate;
import org.apache.aurora.rest.gen.JobUpdateAction;
import org.apache.aurora.rest.gen.JobUpdateDetails;
import org.apache.aurora.rest.gen.JobUpdateEvent;
import org.apache.aurora.rest.gen.JobUpdateInstructions;
import org.apache.aurora.rest.gen.JobUpdateKey;
import org.apache.aurora.rest.gen.JobUpdatePulseStatus;
import org.apache.aurora.rest.gen.JobUpdateQuery;
import org.apache.aurora.rest.gen.JobUpdateRequest;
import org.apache.aurora.rest.gen.JobUpdateSettings;
import org.apache.aurora.rest.gen.JobUpdateState;
import org.apache.aurora.rest.gen.JobUpdateStatus;
import org.apache.aurora.rest.gen.JobUpdateSummary;
import org.apache.aurora.rest.gen.LimitConstraint;
import org.apache.aurora.rest.gen.ListBackupsResult;
import org.apache.aurora.rest.gen.Lock;
import org.apache.aurora.rest.gen.LockKey;
import org.apache.aurora.rest.gen.LockValidation;
import org.apache.aurora.rest.gen.MaintenanceMode;
import org.apache.aurora.rest.gen.MaintenanceStatusResult;
import org.apache.aurora.rest.gen.MesosContainer;
import org.apache.aurora.rest.gen.Metadata;
import org.apache.aurora.rest.gen.Mode;
import org.apache.aurora.rest.gen.Package;
import org.apache.aurora.rest.gen.PendingReason;
import org.apache.aurora.rest.gen.PopulateJobResult;
import org.apache.aurora.rest.gen.PulseJobUpdateResult;
import org.apache.aurora.rest.gen.QueryRecoveryResult;
import org.apache.aurora.rest.gen.Range;
import org.apache.aurora.rest.gen.ResourceAggregate;
import org.apache.aurora.rest.gen.Response;
import org.apache.aurora.rest.gen.ResponseCode;
import org.apache.aurora.rest.gen.ResponseDetail;
import org.apache.aurora.rest.gen.Result;
import org.apache.aurora.rest.gen.RewriteConfigsRequest;
import org.apache.aurora.rest.gen.RoleSummary;
import org.apache.aurora.rest.gen.RoleSummaryResult;
import org.apache.aurora.rest.gen.ScheduleStatus;
import org.apache.aurora.rest.gen.ScheduleStatusResult;
import org.apache.aurora.rest.gen.ScheduledTask;
import org.apache.aurora.rest.gen.ServerInfo;
import org.apache.aurora.rest.gen.SessionKey;
import org.apache.aurora.rest.gen.StartJobUpdateResult;
import org.apache.aurora.rest.gen.StartMaintenanceResult;
import org.apache.aurora.rest.gen.TaskConfig;
import org.apache.aurora.rest.gen.TaskConstraint;
import org.apache.aurora.rest.gen.TaskEvent;
import org.apache.aurora.rest.gen.TaskQuery;
import org.apache.aurora.rest.gen.ValueConstraint;
import org.apache.aurora.rest.gen.Volume;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RestGenTest {
  private ThriftCodecManager createManager() {
    CompilerThriftCodecFactory codecFactory = new CompilerThriftCodecFactory(/* debug */ false);
    MetadataErrors.Monitor errorMonitor = new MetadataErrors.Monitor() {
      @Override public void onError(MetadataErrorException errorMessage) {
        System.out.printf(">>> error: %s%n", errorMessage);
      }
      @Override public void onWarning(MetadataWarningException warningMessage) {
        System.out.printf(">>> warning: %s%n", warningMessage);
      }
    };
    ThriftCatalog catalog = new ThriftCatalog(errorMonitor);
    ImmutableSet<ThriftCodec<?>> knownCodecs = ImmutableSet.of();
    return new ThriftCodecManager(codecFactory, catalog, knownCodecs);
  }

  private void timeCodecCreation(Class<?> ...thriftTypes) {
    ThriftCodecManager codecManager = createManager();
    long totalElapsed = 0L;
    for (Class<?> thriftType : thriftTypes) {
      long startNs = System.nanoTime();
      codecManager.getCodec(thriftType);
      long elapsed = System.nanoTime() - startNs;
      totalElapsed += elapsed;
      System.out.printf("Codec creation for %s took %dns%n", thriftType.getSimpleName(), elapsed);
    }
    System.out.printf("Total elapsed for %d items took %fs%n", thriftTypes.length,
        totalElapsed / 1_000_000_000.0);
  }

  @Test
  public void testTimings() {
    timeCodecCreation(
        APIVersion.class,
        AcquireLockResult.class,
        AddInstancesConfig.class,
        AssignedTask.class,
        Attribute.class,
        ConfigGroup.class,
        ConfigRewrite.class,
        ConfigSummary.class,
        ConfigSummaryResult.class,
        Constraint.class,
        Container.class,
        CronCollisionPolicy.class,
        DockerContainer.class,
        DockerParameter.class,
        DrainHostsResult.class,
        EndMaintenanceResult.class,
        ExecutorConfig.class,
        GetJobUpdateDetailsResult.class,
        GetJobUpdateDiffResult.class,
        GetJobUpdateSummariesResult.class,
        GetJobsResult.class,
        GetLocksResult.class,
        GetPendingReasonResult.class,
        GetQuotaResult.class,
        HostAttributes.class,
        HostStatus.class,
        Hosts.class,
        Identity.class,
        InstanceConfigRewrite.class,
        InstanceKey.class,
        InstanceTaskConfig.class,
        JobConfigRewrite.class,
        JobConfiguration.class,
        JobInstanceUpdateEvent.class,
        JobKey.class,
        JobStats.class,
        JobSummary.class,
        JobSummaryResult.class,
        JobUpdate.class,
        JobUpdateAction.class,
        JobUpdateDetails.class,
        JobUpdateEvent.class,
        JobUpdateInstructions.class,
        JobUpdateKey.class,
        JobUpdatePulseStatus.class,
        JobUpdateQuery.class,
        JobUpdateRequest.class,
        JobUpdateSettings.class,
        JobUpdateState.class,
        JobUpdateStatus.class,
        JobUpdateSummary.class,
        LimitConstraint.class,
        ListBackupsResult.class,
        Lock.class,
        LockKey.class,
        LockValidation.class,
        MaintenanceMode.class,
        MaintenanceStatusResult.class,
        MesosContainer.class,
        Metadata.class,
        Mode.class,
        Package.class,
        PendingReason.class,
        PopulateJobResult.class,
        PulseJobUpdateResult.class,
        QueryRecoveryResult.class,
        Range.class,
        ResourceAggregate.class,
        Response.class,
        ResponseCode.class,
        ResponseDetail.class,
        Result.class,
        RewriteConfigsRequest.class,
        RoleSummary.class,
        RoleSummaryResult.class,
        ScheduleStatus.class,
        ScheduleStatusResult.class,
        ScheduledTask.class,
        ServerInfo.class,
        SessionKey.class,
        StartJobUpdateResult.class,
        StartMaintenanceResult.class,
        TaskConfig.class,
        TaskConstraint.class,
        TaskEvent.class,
        TaskQuery.class,
        ValueConstraint.class,
        Volume.class);
  }

  @Test
  public void testStruct() throws Exception {
    Metadata metadata = Metadata.builder().key("bob").value("42").build();

    ThriftCodecManager codecManager = createManager();
    ThriftCodec<Metadata> metadataCodec = codecManager.getCodec(Metadata.class);
    TMemoryBuffer buffer = new TMemoryBuffer(1024);
    metadataCodec.write(metadata, new TBinaryProtocol(buffer));

    org.apache.aurora.gen.Metadata traditionalMetadata = new org.apache.aurora.gen.Metadata();
    traditionalMetadata.read(new TBinaryProtocol(buffer));

    assertEquals(metadata.key(), traditionalMetadata.getKey());
    assertEquals(metadata.value(), traditionalMetadata.getValue());
  }

  @Test
  public void testUnion() throws Exception {
    JobKey jobKey = JobKey.builder().environment("dev").role("aurora").name("cleaner").build();
    LockKey lockKey = new LockKey(jobKey);

    ThriftCodecManager codecManager = createManager();
    ThriftCodec<LockKey> metadataCodec = codecManager.getCodec(LockKey.class);
    TMemoryBuffer buffer = new TMemoryBuffer(1024);
    metadataCodec.write(lockKey, new TBinaryProtocol(buffer));

    org.apache.aurora.gen.LockKey traditionalLockKey = new org.apache.aurora.gen.LockKey();
    traditionalLockKey.read(new TBinaryProtocol(buffer));

    assertEquals(lockKey.isSetJob(), traditionalLockKey.isSetJob());
    assertEquals(lockKey.getSetId(), traditionalLockKey.getSetField().getThriftFieldId());

    org.apache.aurora.gen.JobKey traditionalJob = traditionalLockKey.getJob();
    assertEquals(jobKey.environment(), traditionalJob.getEnvironment());
    assertEquals(jobKey.role(), traditionalJob.getRole());
    assertEquals(jobKey.name(), traditionalJob.getName());
  }
}
