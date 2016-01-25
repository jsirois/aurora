#!/usr/bin/env bash

THRIFT_ENTITIES=(
  IAcquireLockResult
  IAddInstancesConfig
  IAssignedTask
  IAttribute
  IConfigGroup
  IConfigRewrite
  IConfigSummary
  IConfigSummaryResult
  IConstraint
  IContainer
  IDockerContainer
  IDockerParameter
  IDrainHostsResult
  IEndMaintenanceResult
  IExecutorConfig
  IGetJobUpdateDetailsResult
  IGetJobUpdateDiffResult
  IGetJobUpdateSummariesResult
  IGetJobsResult
  IGetLocksResult
  IGetPendingReasonResult
  IGetQuotaResult
  IHostAttributes
  IHostStatus
  IHosts
  IIdentity
  IInstanceConfigRewrite
  IInstanceKey
  IInstanceTaskConfig
  IJobConfigRewrite
  IJobConfiguration
  IJobInstanceUpdateEvent
  IJobKey
  IJobStats
  IJobSummary
  IJobSummaryResult
  IJobUpdate
  IJobUpdateDetails
  IJobUpdateEvent
  IJobUpdateInstructions
  IJobUpdateKey
  IJobUpdateQuery
  IJobUpdateRequest
  IJobUpdateSettings
  IJobUpdateState
  IJobUpdateSummary
  ILimitConstraint
  IListBackupsResult
  ILock
  ILockKey
  IMaintenanceStatusResult
  IMesosContainer
  IMetadata
  IPackage
  IPendingReason
  IPopulateJobResult
  IPulseJobUpdateResult
  IQueryRecoveryResult
  IRange
  IResourceAggregate
  IResponse
  IResponseDetail
  IResult
  IRewriteConfigsRequest
  IRoleSummary
  IRoleSummaryResult
  IScheduleStatusResult
  IScheduledTask
  IServerInfo
  IStartJobUpdateResult
  IStartMaintenanceResult
  ITaskConfig
  ITaskConstraint
  ITaskEvent
  ITaskQuery
  IValueConstraint
  IVolume
)

find src -name "*.java" | {
  while read file
  do
    sed -i -r \
      -e "s/(api|test|storage)Constants/Constants/g" \
      -e "s|import org.apache.aurora.scheduler.storage.entities.I|import org.apache.aurora.gen.|" \
      -e "s|AuroraAdminMetadata|AuroraAdminMetadata|g" \
      -e "s|IAcquireLockResult|AcquireLockResult|g" \
      -e "s|IAddInstancesConfig|AddInstancesConfig|g" \
      -e "s|IAssignedTask|AssignedTask|g" \
      -e "s|IAttribute|Attribute|g" \
      -e "s|IConfigGroup|ConfigGroup|g" \
      -e "s|IConfigRewrite|ConfigRewrite|g" \
      -e "s|IConfigSummary|ConfigSummary|g" \
      -e "s|IConfigSummaryResult|ConfigSummaryResult|g" \
      -e "s|IConstraint|Constraint|g" \
      -e "s|IContainer|Container|g" \
      -e "s|IDockerContainer|DockerContainer|g" \
      -e "s|IDockerParameter|DockerParameter|g" \
      -e "s|IDrainHostsResult|DrainHostsResult|g" \
      -e "s|IEndMaintenanceResult|EndMaintenanceResult|g" \
      -e "s|IExecutorConfig|ExecutorConfig|g" \
      -e "s|IGetJobUpdateDetailsResult|GetJobUpdateDetailsResult|g" \
      -e "s|IGetJobUpdateDiffResult|GetJobUpdateDiffResult|g" \
      -e "s|IGetJobUpdateSummariesResult|GetJobUpdateSummariesResult|g" \
      -e "s|IGetJobsResult|GetJobsResult|g" \
      -e "s|IGetLocksResult|GetLocksResult|g" \
      -e "s|IGetPendingReasonResult|GetPendingReasonResult|g" \
      -e "s|IGetQuotaResult|GetQuotaResult|g" \
      -e "s|IHostAttributes|HostAttributes|g" \
      -e "s|IHostStatus|HostStatus|g" \
      -e "s|IHosts|Hosts|g" \
      -e "s|IIdentity|Identity|g" \
      -e "s|IInstanceConfigRewrite|InstanceConfigRewrite|g" \
      -e "s|IInstanceKey|InstanceKey|g" \
      -e "s|IInstanceTaskConfig|InstanceTaskConfig|g" \
      -e "s|IJobConfigRewrite|JobConfigRewrite|g" \
      -e "s|IJobConfiguration|JobConfiguration|g" \
      -e "s|IJobInstanceUpdateEvent|JobInstanceUpdateEvent|g" \
      -e "s|IJobKey|JobKey|g" \
      -e "s|IJobStats|JobStats|g" \
      -e "s|IJobSummary|JobSummary|g" \
      -e "s|IJobSummaryResult|JobSummaryResult|g" \
      -e "s|IJobUpdate|JobUpdate|g" \
      -e "s|IJobUpdateDetails|JobUpdateDetails|g" \
      -e "s|IJobUpdateEvent|JobUpdateEvent|g" \
      -e "s|IJobUpdateInstructions|JobUpdateInstructions|g" \
      -e "s|IJobUpdateKey|JobUpdateKey|g" \
      -e "s|IJobUpdateQuery|JobUpdateQuery|g" \
      -e "s|IJobUpdateRequest|JobUpdateRequest|g" \
      -e "s|IJobUpdateSettings|JobUpdateSettings|g" \
      -e "s|IJobUpdateState|JobUpdateState|g" \
      -e "s|IJobUpdateSummary|JobUpdateSummary|g" \
      -e "s|ILimitConstraint|LimitConstraint|g" \
      -e "s|IListBackupsResult|ListBackupsResult|g" \
      -e "s|ILock|Lock|g" \
      -e "s|ILockKey|LockKey|g" \
      -e "s|IMaintenanceStatusResult|MaintenanceStatusResult|g" \
      -e "s|IMesosContainer|MesosContainer|g" \
      -e "s|IMetadata|Metadata|g" \
      -e "s|IPackage|Package|g" \
      -e "s|IPendingReason|PendingReason|g" \
      -e "s|IPopulateJobResult|PopulateJobResult|g" \
      -e "s|IPulseJobUpdateResult|PulseJobUpdateResult|g" \
      -e "s|IQueryRecoveryResult|QueryRecoveryResult|g" \
      -e "s|IRange|Range|g" \
      -e "s|IResourceAggregate|ResourceAggregate|g" \
      -e "s|IResponse|Response|g" \
      -e "s|IResponseDetail|ResponseDetail|g" \
      -e "s|IResult|Result|g" \
      -e "s|IRewriteConfigsRequest|RewriteConfigsRequest|g" \
      -e "s|IRoleSummary|RoleSummary|g" \
      -e "s|IRoleSummaryResult|RoleSummaryResult|g" \
      -e "s|IScheduleStatusResult|ScheduleStatusResult|g" \
      -e "s|IScheduledTask|ScheduledTask|g" \
      -e "s|IServerInfo|ServerInfo|g" \
      -e "s|IStartJobUpdateResult|StartJobUpdateResult|g" \
      -e "s|IStartMaintenanceResult|StartMaintenanceResult|g" \
      -e "s|ITaskConfig|TaskConfig|g" \
      -e "s|ITaskConstraint|TaskConstraint|g" \
      -e "s|ITaskEvent|TaskEvent|g" \
      -e "s|ITaskQuery|TaskQuery|g" \
      -e "s|IValueConstraint|ValueConstraint|g" \
      -e "s|IVolume|Volume|g" \
      ${file}
  done
}
