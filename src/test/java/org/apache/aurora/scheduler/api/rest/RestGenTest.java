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

import java.nio.ByteBuffer;
import java.util.LinkedList;

import com.facebook.nifty.client.FramedClientConnector;
import com.facebook.swift.codec.ThriftCodec;
import com.facebook.swift.codec.ThriftCodecManager;
import com.facebook.swift.codec.internal.compiler.CompilerThriftCodecFactory;
import com.facebook.swift.codec.metadata.MetadataErrorException;
import com.facebook.swift.codec.metadata.MetadataErrors;
import com.facebook.swift.codec.metadata.MetadataWarningException;
import com.facebook.swift.codec.metadata.ThriftCatalog;
import com.facebook.swift.service.ThriftClientManager;
import com.facebook.swift.service.ThriftEventHandler;
import com.facebook.swift.service.ThriftServer;
import com.facebook.swift.service.ThriftServiceProcessor;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.APIVersion;
import org.apache.aurora.gen.AcquireLockResult;
import org.apache.aurora.gen.AddInstancesConfig;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.ConfigGroup;
import org.apache.aurora.gen.ConfigRewrite;
import org.apache.aurora.gen.ConfigSummary;
import org.apache.aurora.gen.ConfigSummaryResult;
import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.CronCollisionPolicy;
import org.apache.aurora.gen.DockerContainer;
import org.apache.aurora.gen.DockerParameter;
import org.apache.aurora.gen.DrainHostsResult;
import org.apache.aurora.gen.EndMaintenanceResult;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.GetJobUpdateDetailsResult;
import org.apache.aurora.gen.GetJobUpdateDiffResult;
import org.apache.aurora.gen.GetJobUpdateSummariesResult;
import org.apache.aurora.gen.GetJobsResult;
import org.apache.aurora.gen.GetLocksResult;
import org.apache.aurora.gen.GetPendingReasonResult;
import org.apache.aurora.gen.GetQuotaResult;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.HostStatus;
import org.apache.aurora.gen.Hosts;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.InstanceConfigRewrite;
import org.apache.aurora.gen.InstanceKey;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobConfigRewrite;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobStats;
import org.apache.aurora.gen.JobSummary;
import org.apache.aurora.gen.JobSummaryResult;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateAction;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdatePulseStatus;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.JobUpdateRequest;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateState;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.LimitConstraint;
import org.apache.aurora.gen.ListBackupsResult;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.gen.LockValidation;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.MaintenanceStatusResult;
import org.apache.aurora.gen.MesosContainer;
import org.apache.aurora.gen.Metadata;
import org.apache.aurora.gen.Mode;
import org.apache.aurora.gen.Package;
import org.apache.aurora.gen.PendingReason;
import org.apache.aurora.gen.PopulateJobResult;
import org.apache.aurora.gen.PulseJobUpdateResult;
import org.apache.aurora.gen.QueryRecoveryResult;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.ReadOnlyScheduler;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.ResponseCode;
import org.apache.aurora.gen.ResponseDetail;
import org.apache.aurora.gen.Result;
import org.apache.aurora.gen.RewriteConfigsRequest;
import org.apache.aurora.gen.RoleSummary;
import org.apache.aurora.gen.RoleSummaryResult;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduleStatusResult;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.ServerInfo;
import org.apache.aurora.gen.StartJobUpdateResult;
import org.apache.aurora.gen.StartMaintenanceResult;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.gen.ValueConstraint;
import org.apache.aurora.gen.Volume;
import org.apache.aurora.gen.storage.DeduplicatedScheduledTask;
import org.apache.aurora.gen.storage.DeduplicatedSnapshot;
import org.apache.aurora.gen.storage.Frame;
import org.apache.aurora.gen.storage.FrameChunk;
import org.apache.aurora.gen.storage.FrameHeader;
import org.apache.aurora.gen.storage.LogEntry;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.PruneJobUpdateHistory;
import org.apache.aurora.gen.storage.QuotaConfiguration;
import org.apache.aurora.gen.storage.RemoveJob;
import org.apache.aurora.gen.storage.RemoveLock;
import org.apache.aurora.gen.storage.RemoveQuota;
import org.apache.aurora.gen.storage.RemoveTasks;
import org.apache.aurora.gen.storage.RewriteTask;
import org.apache.aurora.gen.storage.SaveCronJob;
import org.apache.aurora.gen.storage.SaveFrameworkId;
import org.apache.aurora.gen.storage.SaveHostAttributes;
import org.apache.aurora.gen.storage.SaveJobInstanceUpdateEvent;
import org.apache.aurora.gen.storage.SaveJobUpdate;
import org.apache.aurora.gen.storage.SaveJobUpdateEvent;
import org.apache.aurora.gen.storage.SaveLock;
import org.apache.aurora.gen.storage.SaveQuota;
import org.apache.aurora.gen.storage.SaveTasks;
import org.apache.aurora.gen.storage.SchedulerMetadata;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.gen.storage.StoredCronJob;
import org.apache.aurora.gen.storage.StoredJobUpdateDetails;
import org.apache.aurora.gen.storage.Transaction;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class RestGenTest extends EasyMockTest {
  private ThriftCodecManager createManager() {
    CompilerThriftCodecFactory codecFactory = new CompilerThriftCodecFactory(/* debug */ false);
    MetadataErrors.Monitor errorMonitor = new MetadataErrors.Monitor() {
      @Override public void onError(MetadataErrorException errorMessage) {
        System.err.printf("error: %s%n", errorMessage);
      }
      @Override public void onWarning(MetadataWarningException warningMessage) {
        System.err.printf("warning: %s%n", warningMessage);
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
    control.replay();

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
        DeduplicatedScheduledTask.class,
        DeduplicatedSnapshot.class,
        DockerContainer.class,
        DockerParameter.class,
        DrainHostsResult.class,
        EndMaintenanceResult.class,
        ExecutorConfig.class,
        Frame.class,
        FrameChunk.class,
        FrameHeader.class,
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
        LogEntry.class,
        MaintenanceMode.class,
        MaintenanceStatusResult.class,
        MesosContainer.class,
        Metadata.class,
        Mode.class,
        Op.class,
        Package.class,
        PendingReason.class,
        PopulateJobResult.class,
        PruneJobUpdateHistory.class,
        PulseJobUpdateResult.class,
        QueryRecoveryResult.class,
        QuotaConfiguration.class,
        Range.class,
        RemoveJob.class,
        RemoveLock.class,
        RemoveQuota.class,
        RemoveTasks.class,
        ResourceAggregate.class,
        Response.class,
        ResponseCode.class,
        ResponseDetail.class,
        Result.class,
        RewriteConfigsRequest.class,
        RewriteTask.class,
        RoleSummary.class,
        RoleSummaryResult.class,
        SaveCronJob.class,
        SaveFrameworkId.class,
        SaveHostAttributes.class,
        SaveJobInstanceUpdateEvent.class,
        SaveJobUpdate.class,
        SaveJobUpdateEvent.class,
        SaveLock.class,
        SaveQuota.class,
        SaveTasks.class,
        ScheduleStatus.class,
        ScheduleStatusResult.class,
        ScheduledTask.class,
        SchedulerMetadata.class,
        ServerInfo.class,
        Snapshot.class,
        StartJobUpdateResult.class,
        StartMaintenanceResult.class,
        StoredCronJob.class,
        StoredJobUpdateDetails.class,
        TaskConfig.class,
        TaskConstraint.class,
        TaskEvent.class,
        TaskQuery.class,
        Transaction.class,
        ValueConstraint.class,
        Volume.class);
  }

  @Test
  public void testStruct() throws Exception {
    control.replay();

    Metadata metadata = Metadata.builder().setKey("bob").setValue("42").build();

    ThriftCodecManager codecManager = createManager();
    ThriftCodec<Metadata> metadataCodec = codecManager.getCodec(Metadata.class);
    TMemoryBuffer buffer = new TMemoryBuffer(1024);
    TBinaryProtocol protocol = new TBinaryProtocol(buffer);
    metadataCodec.write(metadata, protocol);

    org.apache.aurora.gen.Metadata traditionalMetadata = metadataCodec.read(protocol);

    assertEquals(metadata.getKey(), traditionalMetadata.getKey());
    assertEquals(metadata.getValue(), traditionalMetadata.getValue());
  }

  @Test
  public void testUnion() throws Exception {
    control.replay();

    JobKey jobKey =
        JobKey.builder()
            .setEnvironment("dev")
            .setRole("aurora")
            .setName("cleaner")
            .build();
    LockKey lockKey = new LockKey(jobKey);

    ThriftCodecManager codecManager = createManager();
    ThriftCodec<LockKey> metadataCodec = codecManager.getCodec(LockKey.class);
    TMemoryBuffer buffer = new TMemoryBuffer(1024);
    TBinaryProtocol protocol = new TBinaryProtocol(buffer);
    metadataCodec.write(lockKey, protocol);

    org.apache.aurora.gen.LockKey traditionalLockKey = metadataCodec.read(protocol);

    assertEquals(lockKey.isSetJob(), traditionalLockKey.isSetJob());
    assertEquals(lockKey.getSetId(), traditionalLockKey.getSetField().getThriftFieldId());

    org.apache.aurora.gen.JobKey traditionalJob = traditionalLockKey.getJob();
    assertEquals(jobKey.getEnvironment(), traditionalJob.getEnvironment());
    assertEquals(jobKey.getRole(), traditionalJob.getRole());
    assertEquals(jobKey.getName(), traditionalJob.getName());
  }

  @Test(expected = NullPointerException.class)
  public void testUnionNonNull() {
    control.replay();

    LockKey.job(null);
  }

  @Test
  public void testByteBuffer() throws Exception {
    control.replay();

    byte[] deflatedEntry = {0xC, 0xA, 0xF, 0xE, 0xB, 0xA, 0xB, 0xE};
    LogEntry logEntry = new LogEntry(ByteBuffer.wrap(deflatedEntry));

    ThriftCodecManager codecManager = createManager();
    ThriftCodec<LogEntry> metadataCodec = codecManager.getCodec(LogEntry.class);
    TMemoryBuffer buffer = new TMemoryBuffer(1024);
    TBinaryProtocol protocol = new TBinaryProtocol(buffer);
    metadataCodec.write(logEntry, protocol);

    org.apache.aurora.gen.storage.LogEntry traditionalLogEntry = metadataCodec.read(protocol);

    assertEquals(logEntry.isSetDeflatedEntry(), traditionalLogEntry.isSetDeflatedEntry());
    assertEquals(ByteBuffer.wrap(deflatedEntry), traditionalLogEntry.getDeflatedEntry());
  }

  @Test
  public void testAsyncService() throws Exception {
    ReadOnlyScheduler.Async readOnlyScheduler = createMock(ReadOnlyScheduler.Async.class);
    Response getLocksResponse =
        Response.builder()
            .setResponseCode(ResponseCode.OK)
            .setDetails(ResponseDetail.builder().setMessage("A-OK").build())
            .build();
    expect(readOnlyScheduler.getLocks()).andReturn(Futures.immediateFuture(getLocksResponse));
    control.replay();

    ThriftCodecManager codecManager = createManager();
    LinkedList<ThriftEventHandler> listeners = new LinkedList<>();
    ThriftServiceProcessor processor =
        new ThriftServiceProcessor(codecManager, listeners, readOnlyScheduler);
    try (ThriftServer server = new ThriftServer(processor).start();
        ThriftClientManager clientManager = new ThriftClientManager(codecManager)) {

      FramedClientConnector connector =
          new FramedClientConnector(HostAndPort.fromParts("localhost", server.getPort()));

      Response response =
          Futures.transform(
              clientManager.createClient(connector, ReadOnlyScheduler.Async.class),
              ReadOnlyScheduler.Async::getLocks).get();

      assertEquals(getLocksResponse, response);
    }
  }

  @Test
  public void testSyncService() throws Exception {
    ReadOnlyScheduler.Sync readOnlyScheduler = createMock(ReadOnlyScheduler.Sync.class);
    Response getLocksResponse =
        Response.builder()
            .setResponseCode(ResponseCode.OK)
            .setDetails(ResponseDetail.builder().setMessage("A-OK").build())
            .build();
    expect(readOnlyScheduler.getLocks()).andReturn(getLocksResponse);
    control.replay();

    ThriftCodecManager codecManager = createManager();
    LinkedList<ThriftEventHandler> listeners = new LinkedList<>();
    ThriftServiceProcessor processor =
        new ThriftServiceProcessor(codecManager, listeners, readOnlyScheduler);
    try (ThriftServer server = new ThriftServer(processor).start();
        ThriftClientManager clientManager = new ThriftClientManager(codecManager)) {

      FramedClientConnector connector =
          new FramedClientConnector(HostAndPort.fromParts("localhost", server.getPort()));

      try (ReadOnlyScheduler.Sync syncClient =
              clientManager.createClient(connector, ReadOnlyScheduler.Sync.class).get()) {

        Response response = syncClient.getLocks();
        assertEquals(getLocksResponse, response);
      }
    }
  }
}
