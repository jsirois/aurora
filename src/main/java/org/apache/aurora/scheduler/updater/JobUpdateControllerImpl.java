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
package org.apache.aurora.scheduler.updater;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import org.apache.aurora.common.collections.Pair;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.gen.InstanceKey;
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateAction;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdatePulseStatus;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.scheduler.base.InstanceKeys;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.state.LockManager;
import org.apache.aurora.scheduler.state.LockManager.LockException;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.updater.StateEvaluator.Failure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkState;

import static org.apache.aurora.gen.JobUpdateStatus.ABORTED;
import static org.apache.aurora.gen.JobUpdateStatus.ERROR;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLED_BACK;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLED_FORWARD;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_BACK;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_FORWARD;
import static org.apache.aurora.gen.JobUpdateStatus.ROLL_FORWARD_AWAITING_PULSE;
import static org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.ACTIVE_QUERY;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.AUTO_RESUME_STATES;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.GET_ACTIVE_RESUME_STATE;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.GET_BLOCKED_RESUME_STATE;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.GET_PAUSE_STATE;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.GET_UNBLOCKED_STATE;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.MonitorAction;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.MonitorAction.ROLL_BACK;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.MonitorAction.ROLL_FORWARD;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.MonitorAction.STOP_WATCHING;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.assertTransitionAllowed;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.getBlockedState;
import static org.apache.aurora.scheduler.updater.OneWayJobUpdater.EvaluationResult;
import static org.apache.aurora.scheduler.updater.OneWayJobUpdater.OneWayStatus;
import static org.apache.aurora.scheduler.updater.OneWayJobUpdater.OneWayStatus.SUCCEEDED;
import static org.apache.aurora.scheduler.updater.SideEffect.InstanceUpdateStatus;

/**
 * Implementation of an updater that orchestrates the process of gradually updating the
 * configuration of tasks in a job.
 * <p>
 * TODO(wfarner): Consider using AbstractIdleService here.
 */
class JobUpdateControllerImpl implements JobUpdateController {
  private static final Logger LOG = LoggerFactory.getLogger(JobUpdateControllerImpl.class);

  private final UpdateFactory updateFactory;
  private final LockManager lockManager;
  private final Storage storage;
  private final ScheduledExecutorService executor;
  private final StateManager stateManager;
  private final Clock clock;
  private final PulseHandler pulseHandler;

  // Currently-active updaters. An active updater is one that is rolling forward or back. Paused
  // and completed updates are represented only in storage, not here.
  private final Map<JobKey, UpdateFactory.Update> updates =
      Collections.synchronizedMap(Maps.newHashMap());

  @Inject
  JobUpdateControllerImpl(
      UpdateFactory updateFactory,
      LockManager lockManager,
      Storage storage,
      ScheduledExecutorService executor,
      StateManager stateManager,
      Clock clock) {

    this.updateFactory = requireNonNull(updateFactory);
    this.lockManager = requireNonNull(lockManager);
    this.storage = requireNonNull(storage);
    this.executor = requireNonNull(executor);
    this.stateManager = requireNonNull(stateManager);
    this.clock = requireNonNull(clock);
    this.pulseHandler = new PulseHandler(clock);
  }

  @Override
  public void start(final JobUpdate update, final AuditData auditData)
      throws UpdateStateException {

    requireNonNull(update);
    requireNonNull(auditData);

    storage.write((NoResult<UpdateStateException>) storeProvider -> {
      JobUpdateSummary summary = update.getSummary();
      JobUpdateInstructions instructions = update.getInstructions();
      JobKey job = summary.getKey().getJob();

      // Validate the update configuration by making sure we can create an updater for it.
      updateFactory.newUpdate(update.getInstructions(), true);

      if (instructions.getInitialState().isEmpty() && !instructions.isSetDesiredState()) {
        throw new IllegalArgumentException("Update instruction is a no-op.");
      }

      List<JobUpdateSummary> activeJobUpdates =
          storeProvider.getJobUpdateStore().fetchJobUpdateSummaries(queryActiveByJob(job));
      if (!activeJobUpdates.isEmpty()) {
        throw new UpdateStateException("An active update already exists for this job, "
            + "please terminate it before starting another. "
            + "Active updates are those in states " + Updates.ACTIVE_JOB_UPDATE_STATES);
      }

      LOG.info("Starting update for job " + job);
      Lock lock;
      try {
        lock = lockManager.acquireLock(LockKey.job(job), auditData.getUser());
      } catch (LockException e) {
        throw new UpdateStateException(e.getMessage(), e);
      }

      storeProvider.getJobUpdateStore().saveJobUpdate(
          update,
          Optional.of(requireNonNull(lock.getToken())));

      JobUpdateStatus status = ROLLING_FORWARD;
      if (isCoordinatedUpdate(instructions)) {
        status = ROLL_FORWARD_AWAITING_PULSE;
        pulseHandler.initializePulseState(update, status);
      }

      recordAndChangeJobUpdateStatus(
          storeProvider,
          summary.getKey(),
          addAuditData(eventBuilder(status), auditData));
    });
  }

  @Override
  public void pause(final JobUpdateKey key, AuditData auditData) throws UpdateStateException {
    requireNonNull(key);
    LOG.info("Attempting to pause update " + key);
    unscopedChangeUpdateStatus(
        key,
        Functions.compose(createAuditedEvent(auditData), GET_PAUSE_STATE));
  }

  @Override
  public void resume(final JobUpdateKey key, final AuditData auditData)
      throws UpdateStateException {

    requireNonNull(key);
    requireNonNull(auditData);
    LOG.info("Attempting to resume update " + key);
    storage.write((NoResult<UpdateStateException>) storeProvider -> {
      JobUpdateDetails details = Iterables.getOnlyElement(
          storeProvider.getJobUpdateStore().fetchJobUpdateDetails(queryByUpdate(key)), null);

      if (details == null) {
        throw new UpdateStateException("Update does not exist: " + key);
      }

      JobUpdate update = details.getUpdate();
      JobUpdateKey key1 = update.getSummary().getKey();
      Function<JobUpdateStatus, JobUpdateStatus> stateChange =
          isCoordinatedAndPulseExpired(key1, update.getInstructions())
              ? GET_BLOCKED_RESUME_STATE
              : GET_ACTIVE_RESUME_STATE;

      JobUpdateStatus newStatus = stateChange.apply(update.getSummary().getState().getStatus());
      changeUpdateStatus(
          storeProvider,
          update.getSummary(),
          addAuditData(eventBuilder(newStatus), auditData));
    });
  }

  @Override
  public void abort(JobUpdateKey key, AuditData auditData) throws UpdateStateException {
    unscopedChangeUpdateStatus(
        key,
        Functions.compose(createAuditedEvent(auditData), Functions.constant(ABORTED)));
  }

  private static Function<JobUpdateStatus, JobUpdateEvent> createAuditedEvent(
      final AuditData auditData) {

    return status -> addAuditData(eventBuilder(status), auditData);
  }

  @Override
  public void systemResume() {
    storage.write((NoResult.Quiet) storeProvider -> {
      for (JobUpdateDetails details
          : storeProvider.getJobUpdateStore().fetchJobUpdateDetails(ACTIVE_QUERY)) {

        JobUpdateSummary summary = details.getUpdate().getSummary();
        JobUpdateInstructions instructions = details.getUpdate().getInstructions();
        JobUpdateKey key = summary.getKey();
        JobUpdateStatus status = summary.getState().getStatus();

        if (isCoordinatedUpdate(instructions)) {
          LOG.info("Automatically restoring pulse state for " + key);
          pulseHandler.initializePulseState(details.getUpdate(), status);
        }

        if (AUTO_RESUME_STATES.contains(status)) {
          LOG.info("Automatically resuming update " + key);

          try {
            changeJobUpdateStatus(storeProvider, key, newEvent(status), false);
          } catch (UpdateStateException e) {
            throw Throwables.propagate(e);
          }
        }
      }
    });
  }

  @Override
  public JobUpdatePulseStatus pulse(final JobUpdateKey key) throws UpdateStateException {
    final PulseState state = pulseHandler.pulseAndGet(key);
    if (state == null) {
      LOG.info("Not pulsing inactive job update: " + key);
      return JobUpdatePulseStatus.FINISHED;
    }

    LOG.debug(
        "Job update {} has been pulsed. Timeout of {} msec is reset.",
        key,
        state.getPulseTimeoutMs());

    if (JobUpdateStateMachine.isAwaitingPulse(state.getStatus())) {
      // Attempt to unblock a job update previously blocked on expired pulse.
      executor.execute(() -> {
        try {
          unscopedChangeUpdateStatus(
              key,
              status ->
                  JobUpdateEvent.builder().setStatus(GET_UNBLOCKED_STATE.apply(status)).build());
        } catch (UpdateStateException e) {
          LOG.error("Error while processing job update pulse: " + e);
        }
      });
    }

    return JobUpdatePulseStatus.OK;
  }

  @Override
  public void instanceChangedState(final ScheduledTask updatedTask) {
    instanceChanged(
        InstanceKeys.from(
            updatedTask.getAssignedTask().getTask().getJob(),
            updatedTask.getAssignedTask().getInstanceId()),
        Optional.of(updatedTask));
  }

  @Override
  public void instanceDeleted(InstanceKey instance) {
    // This is primarily used to detect when an instance was stuck in PENDING and killed, which
    // results in deletion.
    instanceChanged(instance, Optional.absent());
  }

  private void instanceChanged(final InstanceKey instance, final Optional<ScheduledTask> state) {
    storage.write((NoResult.Quiet) storeProvider -> {
      JobKey job = instance.getJobKey();
      UpdateFactory.Update update = updates.get(job);
      if (update != null) {
        if (update.getUpdater().containsInstance(instance.getInstanceId())) {
          LOG.info("Forwarding task change for " + InstanceKeys.toString(instance));
          try {
            evaluateUpdater(
                storeProvider,
                update,
                getOnlyMatch(storeProvider.getJobUpdateStore(), queryActiveByJob(job)),
                ImmutableMap.of(instance.getInstanceId(), state));
          } catch (UpdateStateException e) {
            throw Throwables.propagate(e);
          }
        } else {
          LOG.info("Instance " + instance + " is not part of active update for "
              + JobKeys.canonicalString(job));
        }
      }
    });
  }

  private JobUpdateSummary getOnlyMatch(JobUpdateStore store, JobUpdateQuery query) {
    return Iterables.getOnlyElement(store.fetchJobUpdateSummaries(query));
  }

  @VisibleForTesting
  static JobUpdateQuery queryActiveByJob(JobKey job) {
    return JobUpdateQuery.builder()
        .setJobKey(job)
        .setUpdateStatuses(Updates.ACTIVE_JOB_UPDATE_STATES)
        .build();
  }

  /**
   * Changes the state of an update, without the 'scope' of an update ID.  This should only be used
   * when responding to outside inputs that are inherently un-scoped, such as a user action or task
   * state change.
   *
   * @param key Update identifier.
   * @param stateChange State change computation, based on the current state of the update.
   * @throws UpdateStateException If no active update exists for the provided {@code job}, or
   *                              if the proposed state transition is not allowed.
   */
  private void unscopedChangeUpdateStatus(
      final JobUpdateKey key,
      final Function<? super JobUpdateStatus, JobUpdateEvent> stateChange)
      throws UpdateStateException {

    storage.write((NoResult<UpdateStateException>) storeProvider -> {

      JobUpdateSummary update = Iterables.getOnlyElement(
          storeProvider.getJobUpdateStore().fetchJobUpdateSummaries(queryByUpdate(key)), null);
      if (update == null) {
        throw new UpdateStateException("Update does not exist " + key);
      }

      changeUpdateStatus(storeProvider, update, stateChange.apply(update.getState().getStatus()));
    });
  }

  private void changeUpdateStatus(
      MutableStoreProvider storeProvider,
      JobUpdateSummary updateSummary,
      JobUpdateEvent event) throws UpdateStateException {

    if (updateSummary.getState().getStatus() == event.getStatus()) {
      return;
    }

    assertTransitionAllowed(updateSummary.getState().getStatus(), event.getStatus());
    recordAndChangeJobUpdateStatus(storeProvider, updateSummary.getKey(), event);
  }

  private void recordAndChangeJobUpdateStatus(
      MutableStoreProvider storeProvider,
      JobUpdateKey key,
      JobUpdateEvent event) throws UpdateStateException {

    changeJobUpdateStatus(storeProvider, key, event, true);
  }

  private static final Set<JobUpdateStatus> TERMINAL_STATES = ImmutableSet.of(
      ROLLED_FORWARD,
      ROLLED_BACK,
      ABORTED,
      JobUpdateStatus.FAILED,
      ERROR
  );

  private void changeJobUpdateStatus(
      MutableStoreProvider storeProvider,
      JobUpdateKey key,
      JobUpdateEvent proposedEvent,
      boolean recordChange) throws UpdateStateException {

    JobUpdateStatus status;
    boolean record;

    JobUpdateStore.Mutable updateStore = storeProvider.getJobUpdateStore();
    Optional<String> updateLock = updateStore.getLockToken(key);
    if (updateLock.isPresent()) {
      status = proposedEvent.getStatus();
      record = recordChange;
    } else {
      LOG.error("Update " + key + " does not have a lock");
      status = ERROR;
      record = true;
    }

    LOG.info("Update {} is now in state {}", key, status);
    if (record) {
      updateStore.saveJobUpdateEvent(
          key,
          proposedEvent.toBuilder().setTimestampMs(clock.nowMillis()).setStatus(status).build());
    }

    if (TERMINAL_STATES.contains(status)) {
      if (updateLock.isPresent()) {
        lockManager.releaseLock(Lock.builder()
            .setKey(LockKey.job(key.getJob()))
            .setToken(updateLock.get())
            .build());
      }

      pulseHandler.remove(key);
    } else {
      pulseHandler.updatePulseStatus(key, status);
    }

    MonitorAction action = JobUpdateStateMachine.getActionForStatus(status);
    JobKey job = key.getJob();
    if (action == STOP_WATCHING) {
      updates.remove(job);
    } else if (action == ROLL_FORWARD || action == ROLL_BACK) {
      if (action == ROLL_BACK) {
        updates.remove(job);
      } else {
        checkState(!updates.containsKey(job), "Updater already exists for " + job);
      }

      JobUpdate jobUpdate = updateStore.fetchJobUpdate(key).get();
      UpdateFactory.Update update;
      try {
        update = updateFactory.newUpdate(jobUpdate.getInstructions(), action == ROLL_FORWARD);
      } catch (RuntimeException e) {
        LOG.warn("Uncaught exception: " + e, e);
        changeJobUpdateStatus(
            storeProvider,
            key,
            eventBuilder(ERROR).setMessage("Internal scheduler error: " + e.getMessage()).build(),
            true);
        return;
      }
      updates.put(job, update);
      evaluateUpdater(
          storeProvider,
          update,
          jobUpdate.getSummary(),
          ImmutableMap.of());
    }
  }

  private static Optional<ScheduledTask> getActiveInstance(
      TaskStore taskStore,
      JobKey job,
      int instanceId) {

    return Optional.fromNullable(Iterables.getOnlyElement(
        taskStore.fetchTasks(Query.instanceScoped(job, instanceId).active()), null));
  }

  private static final Set<InstanceUpdateStatus> NOOP_INSTANCE_UPDATE =
      ImmutableSet.of(InstanceUpdateStatus.WORKING, InstanceUpdateStatus.SUCCEEDED);

  private static boolean isCoordinatedUpdate(JobUpdateInstructions instructions) {
    return instructions.getSettings().getBlockIfNoPulsesAfterMs() > 0;
  }

  private boolean isCoordinatedAndPulseExpired(
      JobUpdateKey key,
      JobUpdateInstructions instructions) {

    if (isCoordinatedUpdate(instructions)) {
      PulseState pulseState = pulseHandler.get(key);
      boolean result = pulseState == null || pulseState.isBlocked(clock);
      LOG.info("Coordinated update {} pulse expired: {}", key, result);
      return result;
    } else {
      return false;
    }
  }

  @VisibleForTesting
  static final String LOST_LOCK_MESSAGE = "Updater has lost its exclusive lock, unable to proceed.";

  @VisibleForTesting
  static final String PULSE_TIMEOUT_MESSAGE = "Pulses from external service have timed out.";

  private void evaluateUpdater(
      final MutableStoreProvider storeProvider,
      final UpdateFactory.Update update,
      JobUpdateSummary summary,
      Map<Integer, Optional<ScheduledTask>> changedInstance) throws UpdateStateException {

    JobUpdateStatus updaterStatus = summary.getState().getStatus();
    final JobUpdateKey key = summary.getKey();

    JobUpdateStore.Mutable updateStore = storeProvider.getJobUpdateStore();
    if (!updateStore.getLockToken(key).isPresent()) {
      recordAndChangeJobUpdateStatus(
          storeProvider,
          key,
          eventBuilder(ERROR).setMessage(LOST_LOCK_MESSAGE).build());
      return;
    }

    JobUpdateInstructions instructions = updateStore.fetchJobUpdateInstructions(key).get();
    if (isCoordinatedAndPulseExpired(key, instructions)) {
      // Move coordinated update into awaiting pulse state.
      JobUpdateStatus blockedStatus = getBlockedState(summary.getState().getStatus());
      changeUpdateStatus(
          storeProvider,
          summary,
          eventBuilder(blockedStatus).setMessage(PULSE_TIMEOUT_MESSAGE).build());
      return;
    }

    InstanceStateProvider<Integer, Optional<ScheduledTask>> stateProvider =
        instanceId -> getActiveInstance(storeProvider.getTaskStore(), key.getJob(), instanceId);

    EvaluationResult<Integer> result = update.getUpdater().evaluate(changedInstance, stateProvider);

    LOG.info(key + " evaluation result: " + result);

    for (Map.Entry<Integer, SideEffect> entry : result.getSideEffects().entrySet()) {
      Iterable<InstanceUpdateStatus> statusChanges;

      int instanceId = entry.getKey();
      List<JobInstanceUpdateEvent> savedEvents = updateStore.fetchInstanceEvents(key, instanceId);

      Set<JobUpdateAction> savedActions =
          FluentIterable.from(savedEvents).transform(EVENT_TO_ACTION).toSet();

      // Don't bother persisting a sequence of status changes that represents an instance that
      // was immediately recognized as being healthy and in the desired state.
      if (entry.getValue().getStatusChanges().equals(NOOP_INSTANCE_UPDATE)
          && savedEvents.isEmpty()) {

        LOG.info("Suppressing no-op update for instance " + instanceId);
        statusChanges = ImmutableSet.of();
      } else {
        statusChanges = entry.getValue().getStatusChanges();
      }

      for (InstanceUpdateStatus statusChange : statusChanges) {
        JobUpdateAction action = STATE_MAP.get(Pair.of(statusChange, updaterStatus));
        requireNonNull(action);

        // A given instance update action may only be issued once during the update lifecycle.
        // Suppress duplicate events due to pause/resume operations.
        if (savedActions.contains(action)) {
          LOG.info("Suppressing duplicate update {} for instance {}.", action, instanceId);
        } else {
          JobInstanceUpdateEvent event = JobInstanceUpdateEvent.builder()
              .setInstanceId(instanceId)
              .setTimestampMs(clock.nowMillis())
              .setAction(action)
              .build();
          updateStore.saveJobInstanceUpdateEvent(summary.getKey(), event);
        }
      }
    }

    OneWayStatus status = result.getStatus();
    if (status == SUCCEEDED || status == OneWayStatus.FAILED) {
      if (SideEffect.hasActions(result.getSideEffects().values())) {
        throw new IllegalArgumentException(
            "A terminal state should not specify actions: " + result);
      }

      JobUpdateEvent.Builder event = JobUpdateEvent.builder();
      if (status == SUCCEEDED) {
        event.setStatus(update.getSuccessStatus());
      } else {
        event.setStatus(update.getFailureStatus());
        // Generate a transition message based on one (arbitrary) instance in the group that pushed
        // the update over the failure threshold (in all likelihood this group is of size 1).
        // This is done as a rough cut to aid in diagnosing a failed update, as generating a
        // complete summary would likely be of dubious value.
        for (Map.Entry<Integer, SideEffect> entry : result.getSideEffects().entrySet()) {
          Optional<Failure> failure = entry.getValue().getFailure();
          if (failure.isPresent()) {
            event.setMessage(failureMessage(entry.getKey(), failure.get()));
            break;
          }
        }
      }
      changeUpdateStatus(storeProvider, summary, event.build());
    } else {
      LOG.info("Executing side-effects for update of " + key + ": " + result.getSideEffects());
      for (Map.Entry<Integer, SideEffect> entry : result.getSideEffects().entrySet()) {
        InstanceKey instance = InstanceKeys.from(key.getJob(), entry.getKey());

        Optional<InstanceAction> action = entry.getValue().getAction();
        if (action.isPresent()) {
          Optional<InstanceActionHandler> handler = action.get().getHandler();
          if (handler.isPresent()) {
            Optional<Amount<Long, Time>> reevaluateDelay = handler.get().getReevaluationDelay(
                instance,
                instructions,
                storeProvider,
                stateManager,
                updaterStatus);
            if (reevaluateDelay.isPresent()) {
              executor.schedule(
                  getDeferredEvaluator(instance, key),
                  reevaluateDelay.get().getValue(),
                  reevaluateDelay.get().getUnit().getTimeUnit());
            }
          }
        }
      }
    }
  }

  @VisibleForTesting
  static final Function<JobInstanceUpdateEvent, JobUpdateAction> EVENT_TO_ACTION =
      JobInstanceUpdateEvent::getAction;

  @VisibleForTesting
  static String failureMessage(int instanceId, Failure failure) {
    return String.format("Latest failure: instance %d %s", instanceId, failure.getReason());
  }

  /**
   * Associates an instance updater state change and the job's update status to an action.
   */
  private static final Map<Pair<InstanceUpdateStatus, JobUpdateStatus>, JobUpdateAction> STATE_MAP =
      ImmutableMap.<Pair<InstanceUpdateStatus, JobUpdateStatus>, JobUpdateAction>builder()
          .put(
              Pair.of(InstanceUpdateStatus.WORKING, ROLLING_FORWARD),
              JobUpdateAction.INSTANCE_UPDATING)
          .put(
              Pair.of(InstanceUpdateStatus.SUCCEEDED, ROLLING_FORWARD),
              JobUpdateAction.INSTANCE_UPDATED)
          .put(
              Pair.of(InstanceUpdateStatus.FAILED, ROLLING_FORWARD),
              JobUpdateAction.INSTANCE_UPDATE_FAILED)
          .put(
              Pair.of(InstanceUpdateStatus.WORKING, ROLLING_BACK),
              JobUpdateAction.INSTANCE_ROLLING_BACK)
          .put(
              Pair.of(InstanceUpdateStatus.SUCCEEDED, ROLLING_BACK),
              JobUpdateAction.INSTANCE_ROLLED_BACK)
          .put(
              Pair.of(InstanceUpdateStatus.FAILED, ROLLING_BACK),
              JobUpdateAction.INSTANCE_ROLLBACK_FAILED)
          .build();

  @VisibleForTesting
  static JobUpdateQuery queryByUpdate(JobUpdateKey key) {
    return JobUpdateQuery.builder().setKey(key).build();
  }

  private static JobUpdateEvent newEvent(JobUpdateStatus status) {
    return eventBuilder(status).build();
  }

  private static JobUpdateEvent.Builder eventBuilder(JobUpdateStatus status) {
    return JobUpdateEvent.builder().setStatus(status);
  }

  private static JobUpdateEvent addAuditData(JobUpdateEvent.Builder event, AuditData auditData) {
    return event.setMessage(auditData.getMessage().orNull())
        .setUser(auditData.getUser())
        .build();
  }

  private Runnable getDeferredEvaluator(final InstanceKey instance, final JobUpdateKey key) {
    return () -> storage.write((NoResult.Quiet) storeProvider -> {
      JobUpdateSummary summary =
          getOnlyMatch(storeProvider.getJobUpdateStore(), queryByUpdate(key));
      JobUpdateStatus status = summary.getState().getStatus();
      // Suppress this evaluation if the updater is not currently active.
      if (JobUpdateStateMachine.isActive(status)) {
        UpdateFactory.Update update = updates.get(instance.getJobKey());
        try {
          evaluateUpdater(
              storeProvider,
              update,
              summary,
              ImmutableMap.of(
                  instance.getInstanceId(),
                  getActiveInstance(
                      storeProvider.getTaskStore(),
                      instance.getJobKey(),
                      instance.getInstanceId())));
        } catch (UpdateStateException e) {
          throw Throwables.propagate(e);
        }
      }
    });
  }

  private static class PulseHandler {
    private final Clock clock;

    // TODO(maxim): expose this data via a debug endpoint AURORA-1103.
    // Currently active coordinated update pulse states. A pulse state is added when a coordinated
    // update is created and removed only when an update reaches terminal state. A PAUSED update
    // pulse state is still retained in the map and accepts pulses.
    private final Map<JobUpdateKey, PulseState> pulseStates = Maps.newHashMap();

    PulseHandler(Clock clock) {
      this.clock = requireNonNull(clock);
    }

    synchronized void initializePulseState(JobUpdate update, JobUpdateStatus status) {
      pulseStates.put(update.getSummary().getKey(), new PulseState(
          status,
          update.getInstructions().getSettings().getBlockIfNoPulsesAfterMs(),
          0L));
    }

    synchronized PulseState pulseAndGet(JobUpdateKey key) {
      PulseState state = pulseStates.get(key);
      if (state != null) {
        state = pulseStates.put(key, new PulseState(
            state.getStatus(),
            state.getPulseTimeoutMs(),
            clock.nowMillis()));
      }
      return state;
    }

    synchronized void updatePulseStatus(JobUpdateKey key, JobUpdateStatus status) {
      PulseState state = pulseStates.get(key);
      if (state != null) {
        pulseStates.put(key, new PulseState(
            status,
            state.getPulseTimeoutMs(),
            state.getLastPulseMs()));
      }
    }

    synchronized void remove(JobUpdateKey key) {
      pulseStates.remove(key);
    }

    synchronized PulseState get(JobUpdateKey key) {
      return pulseStates.get(key);
    }
  }

  private static class PulseState {
    private final JobUpdateStatus status;
    private final long pulseTimeoutMs;
    private final long lastPulseMs;

    PulseState(JobUpdateStatus status, long pulseTimeoutMs, long lastPulseMs) {
      this.status = requireNonNull(status);
      this.pulseTimeoutMs = pulseTimeoutMs;
      this.lastPulseMs = lastPulseMs;
    }

    JobUpdateStatus getStatus() {
      return status;
    }

    long getPulseTimeoutMs() {
      return pulseTimeoutMs;
    }

    long getLastPulseMs() {
      return lastPulseMs;
    }

    boolean isBlocked(Clock clock) {
      return clock.nowMillis() - lastPulseMs >= pulseTimeoutMs;
    }
  }
}
