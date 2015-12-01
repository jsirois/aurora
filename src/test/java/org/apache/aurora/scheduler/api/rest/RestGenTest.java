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
import org.apache.aurora.gen.rest.APIVersion;
import org.apache.aurora.gen.rest.AcquireLockResult;
import org.apache.aurora.gen.rest.AddInstancesConfig;
import org.apache.aurora.gen.rest.AssignedTask;
import org.apache.aurora.gen.rest.Attribute;
import org.apache.aurora.gen.rest.ConfigGroup;
import org.apache.aurora.gen.rest.ConfigRewrite;
import org.apache.aurora.gen.rest.ConfigSummary;
import org.apache.aurora.gen.rest.ConfigSummaryResult;
import org.apache.aurora.gen.rest.Constraint;
import org.apache.aurora.gen.rest.Container;
import org.apache.aurora.gen.rest.CronCollisionPolicy;
import org.apache.aurora.gen.rest.DockerContainer;
import org.apache.aurora.gen.rest.DockerParameter;
import org.apache.aurora.gen.rest.DrainHostsResult;
import org.apache.aurora.gen.rest.EndMaintenanceResult;
import org.apache.aurora.gen.rest.ExecutorConfig;
import org.apache.aurora.gen.rest.GetJobUpdateDetailsResult;
import org.apache.aurora.gen.rest.GetJobUpdateDiffResult;
import org.apache.aurora.gen.rest.GetJobUpdateSummariesResult;
import org.apache.aurora.gen.rest.GetJobsResult;
import org.apache.aurora.gen.rest.GetLocksResult;
import org.apache.aurora.gen.rest.GetPendingReasonResult;
import org.apache.aurora.gen.rest.GetQuotaResult;
import org.apache.aurora.gen.rest.HostAttributes;
import org.apache.aurora.gen.rest.HostStatus;
import org.apache.aurora.gen.rest.Hosts;
import org.apache.aurora.gen.rest.Identity;
import org.apache.aurora.gen.rest.InstanceConfigRewrite;
import org.apache.aurora.gen.rest.InstanceKey;
import org.apache.aurora.gen.rest.InstanceTaskConfig;
import org.apache.aurora.gen.rest.JobConfigRewrite;
import org.apache.aurora.gen.rest.JobConfiguration;
import org.apache.aurora.gen.rest.JobInstanceUpdateEvent;
import org.apache.aurora.gen.rest.JobKey;
import org.apache.aurora.gen.rest.JobStats;
import org.apache.aurora.gen.rest.JobSummary;
import org.apache.aurora.gen.rest.JobSummaryResult;
import org.apache.aurora.gen.rest.JobUpdate;
import org.apache.aurora.gen.rest.JobUpdateAction;
import org.apache.aurora.gen.rest.JobUpdateDetails;
import org.apache.aurora.gen.rest.JobUpdateEvent;
import org.apache.aurora.gen.rest.JobUpdateInstructions;
import org.apache.aurora.gen.rest.JobUpdateKey;
import org.apache.aurora.gen.rest.JobUpdatePulseStatus;
import org.apache.aurora.gen.rest.JobUpdateQuery;
import org.apache.aurora.gen.rest.JobUpdateRequest;
import org.apache.aurora.gen.rest.JobUpdateSettings;
import org.apache.aurora.gen.rest.JobUpdateState;
import org.apache.aurora.gen.rest.JobUpdateStatus;
import org.apache.aurora.gen.rest.JobUpdateSummary;
import org.apache.aurora.gen.rest.LimitConstraint;
import org.apache.aurora.gen.rest.ListBackupsResult;
import org.apache.aurora.gen.rest.Lock;
import org.apache.aurora.gen.rest.LockKey;
import org.apache.aurora.gen.rest.LockValidation;
import org.apache.aurora.gen.rest.MaintenanceMode;
import org.apache.aurora.gen.rest.MaintenanceStatusResult;
import org.apache.aurora.gen.rest.MesosContainer;
import org.apache.aurora.gen.rest.Metadata;
import org.apache.aurora.gen.rest.Mode;
import org.apache.aurora.gen.rest.Package;
import org.apache.aurora.gen.rest.PendingReason;
import org.apache.aurora.gen.rest.PopulateJobResult;
import org.apache.aurora.gen.rest.PulseJobUpdateResult;
import org.apache.aurora.gen.rest.QueryRecoveryResult;
import org.apache.aurora.gen.rest.Range;
import org.apache.aurora.gen.rest.ReadOnlyScheduler;
import org.apache.aurora.gen.rest.ResourceAggregate;
import org.apache.aurora.gen.rest.Response;
import org.apache.aurora.gen.rest.ResponseCode;
import org.apache.aurora.gen.rest.ResponseDetail;
import org.apache.aurora.gen.rest.Result;
import org.apache.aurora.gen.rest.RewriteConfigsRequest;
import org.apache.aurora.gen.rest.RoleSummary;
import org.apache.aurora.gen.rest.RoleSummaryResult;
import org.apache.aurora.gen.rest.ScheduleStatus;
import org.apache.aurora.gen.rest.ScheduleStatusResult;
import org.apache.aurora.gen.rest.ScheduledTask;
import org.apache.aurora.gen.rest.ServerInfo;
import org.apache.aurora.gen.rest.SessionKey;
import org.apache.aurora.gen.rest.StartJobUpdateResult;
import org.apache.aurora.gen.rest.StartMaintenanceResult;
import org.apache.aurora.gen.rest.TaskConfig;
import org.apache.aurora.gen.rest.TaskConstraint;
import org.apache.aurora.gen.rest.TaskEvent;
import org.apache.aurora.gen.rest.TaskQuery;
import org.apache.aurora.gen.rest.ValueConstraint;
import org.apache.aurora.gen.rest.Volume;
import org.apache.aurora.gen.storage.rest.DeduplicatedScheduledTask;
import org.apache.aurora.gen.storage.rest.DeduplicatedSnapshot;
import org.apache.aurora.gen.storage.rest.Frame;
import org.apache.aurora.gen.storage.rest.FrameChunk;
import org.apache.aurora.gen.storage.rest.FrameHeader;
import org.apache.aurora.gen.storage.rest.LogEntry;
import org.apache.aurora.gen.storage.rest.Op;
import org.apache.aurora.gen.storage.rest.PruneJobUpdateHistory;
import org.apache.aurora.gen.storage.rest.QuotaConfiguration;
import org.apache.aurora.gen.storage.rest.RemoveJob;
import org.apache.aurora.gen.storage.rest.RemoveLock;
import org.apache.aurora.gen.storage.rest.RemoveQuota;
import org.apache.aurora.gen.storage.rest.RemoveTasks;
import org.apache.aurora.gen.storage.rest.RewriteTask;
import org.apache.aurora.gen.storage.rest.SaveCronJob;
import org.apache.aurora.gen.storage.rest.SaveFrameworkId;
import org.apache.aurora.gen.storage.rest.SaveHostAttributes;
import org.apache.aurora.gen.storage.rest.SaveJobInstanceUpdateEvent;
import org.apache.aurora.gen.storage.rest.SaveJobUpdate;
import org.apache.aurora.gen.storage.rest.SaveJobUpdateEvent;
import org.apache.aurora.gen.storage.rest.SaveLock;
import org.apache.aurora.gen.storage.rest.SaveQuota;
import org.apache.aurora.gen.storage.rest.SaveTasks;
import org.apache.aurora.gen.storage.rest.SchedulerMetadata;
import org.apache.aurora.gen.storage.rest.Snapshot;
import org.apache.aurora.gen.storage.rest.StoredCronJob;
import org.apache.aurora.gen.storage.rest.StoredJobUpdateDetails;
import org.apache.aurora.gen.storage.rest.Transaction;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertArrayEquals;
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
        SessionKey.class,
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
    metadataCodec.write(metadata, new TBinaryProtocol(buffer));

    org.apache.aurora.gen.Metadata traditionalMetadata = new org.apache.aurora.gen.Metadata();
    traditionalMetadata.read(new TBinaryProtocol(buffer));

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
    metadataCodec.write(lockKey, new TBinaryProtocol(buffer));

    org.apache.aurora.gen.LockKey traditionalLockKey = new org.apache.aurora.gen.LockKey();
    traditionalLockKey.read(new TBinaryProtocol(buffer));

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

    byte[] bytes = {0xC, 0xA, 0xF, 0xE, 0xB, 0xA, 0xB, 0xE};
    ByteBuffer deflatedEntry = ByteBuffer.wrap(bytes);
    LogEntry logEntry = new LogEntry(deflatedEntry);

    ThriftCodecManager codecManager = createManager();
    ThriftCodec<LogEntry> metadataCodec = codecManager.getCodec(LogEntry.class);
    TMemoryBuffer buffer = new TMemoryBuffer(1024);
    metadataCodec.write(logEntry, new TBinaryProtocol(buffer));

    org.apache.aurora.gen.storage.LogEntry traditionalLogEntry =
        new org.apache.aurora.gen.storage.LogEntry();
    traditionalLogEntry.read(new TBinaryProtocol(buffer));

    assertEquals(logEntry.isSetDeflatedEntry(), traditionalLogEntry.isSetDeflatedEntry());
    assertArrayEquals(bytes, traditionalLogEntry.getDeflatedEntry());
    assertEquals(logEntry.getDeflatedEntry(), traditionalLogEntry.bufferForDeflatedEntry());
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
