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
package org.apache.aurora.scheduler.configuration;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.CmdLine;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConfig._Fields;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.gen.ValueConstraint;

import static org.apache.aurora.gen.Constants.GOOD_IDENTIFIER_PATTERN_JVM;

/**
 * Manages translation from a string-mapped configuration to a concrete configuration type, and
 * defaults for optional values.
 *
 * TODO(William Farner): Add input validation to all fields (strings not empty, positive ints, etc).
 */
public final class ConfigurationManager {

  @CmdLine(name = "allowed_container_types",
      help = "Container types that are allowed to be used by jobs.")
  private static final Arg<List<Container._Fields>> ALLOWED_CONTAINER_TYPES =
      Arg.create(ImmutableList.of(Container._Fields.MESOS));

  @CmdLine(name = "allow_docker_parameters",
      help = "Allow to pass docker container parameters in the job.")
  private static final Arg<Boolean> ENABLE_DOCKER_PARAMETERS = Arg.create(false);

  public static final String DEDICATED_ATTRIBUTE = "dedicated";

  private static final Pattern GOOD_IDENTIFIER = Pattern.compile(GOOD_IDENTIFIER_PATTERN_JVM);

  private static final int MAX_IDENTIFIER_LENGTH = 255;

  private interface Validator<T> {
    void validate(T value) throws TaskDescriptionException;
  }

  private static class GreaterThan implements Validator<Number> {
    private final double min;
    private final String label;

    GreaterThan(double min, String label) {
      this.min = min;
      this.label = label;
    }

    @Override
    public void validate(Number value) throws TaskDescriptionException {
      if (this.min >= value.doubleValue()) {
        throw new TaskDescriptionException(label + " must be greater than " + this.min);
      }
    }
  }

  private static class RequiredFieldValidator<T> implements Validator<TaskConfig> {
    private final _Fields field;
    private final Validator<T> validator;

    RequiredFieldValidator(_Fields field, Validator<T> validator) {
      this.field = field;
      this.validator = validator;
    }

    public void validate(TaskConfig task) throws TaskDescriptionException {
      if (!task.isSet(field)) {
        throw new TaskDescriptionException("Field " + field.getFieldName() + " is required.");
      }
      @SuppressWarnings("unchecked")
      T value = (T) task.getFieldValue(field);
      validator.validate(value);
    }
  }

  private static final Iterable<RequiredFieldValidator<?>> REQUIRED_FIELDS_VALIDATORS =
      ImmutableList.of(
          new RequiredFieldValidator<>(_Fields.NUM_CPUS, new GreaterThan(0.0, "num_cpus")),
          new RequiredFieldValidator<>(_Fields.RAM_MB, new GreaterThan(0.0, "ram_mb")),
          new RequiredFieldValidator<>(_Fields.DISK_MB, new GreaterThan(0.0, "disk_mb")));

  private ConfigurationManager() {
    // Utility class.
  }

  /**
   * Verifies that an identifier is an acceptable name component.
   *
   * @param identifier Identifier to check.
   * @return false if the identifier is null or invalid.
   */
  public static boolean isGoodIdentifier(@Nullable String identifier) {
    return identifier != null
        && GOOD_IDENTIFIER.matcher(identifier).matches()
        && identifier.length() <= MAX_IDENTIFIER_LENGTH;
  }

  private static void requireNonNull(Object value, String error) throws TaskDescriptionException {
    if (value == null) {
      throw new TaskDescriptionException(error);
    }
  }

  private static void assertOwnerValidity(Identity jobOwner) throws TaskDescriptionException {
    requireNonNull(jobOwner, "No job owner specified!");
    requireNonNull(jobOwner.getRole(), "No job role specified!");
    requireNonNull(jobOwner.getUser(), "No job user specified!");

    if (!isGoodIdentifier(jobOwner.getRole())) {
      throw new TaskDescriptionException(
          "Job role contains illegal characters: " + jobOwner.getRole());
    }

    if (!isGoodIdentifier(jobOwner.getUser())) {
      throw new TaskDescriptionException(
          "Job user contains illegal characters: " + jobOwner.getUser());
    }
  }

  private static String getRole(ValueConstraint constraint) {
    return Iterables.getOnlyElement(constraint.getValues()).split("/")[0];
  }

  private static boolean isValueConstraint(TaskConstraint taskConstraint) {
    return taskConstraint.getSetField() == TaskConstraint._Fields.VALUE;
  }

  public static boolean isDedicated(Iterable<Constraint> taskConstraints) {
    return Iterables.any(taskConstraints, getConstraintByName(DEDICATED_ATTRIBUTE));
  }

  @Nullable
  private static Constraint getDedicatedConstraint(TaskConfig task) {
    return Iterables.find(task.getConstraints(), getConstraintByName(DEDICATED_ATTRIBUTE), null);
  }

  /**
   * Check validity of and populates defaults in a job configuration.  This will return a deep copy
   * of the provided job configuration with default configuration values applied, and configuration
   * map values sanitized and applied to their respective struct fields.
   *
   * @param rawJob Job to validate and populate.
   * @return A deep copy of {@code job} that has been populated.
   * @throws TaskDescriptionException If the job configuration is invalid.
   */
  public static JobConfiguration validateAndPopulate(JobConfiguration rawJob)
      throws TaskDescriptionException {

    Objects.requireNonNull(rawJob);

    if (!rawJob.isSetTaskConfig()) {
      throw new TaskDescriptionException("Job configuration must have taskConfig set.");
    }

    if (rawJob.getInstanceCount() <= 0) {
      throw new TaskDescriptionException("Instance count must be positive.");
    }

    JobConfiguration.Builder builder = rawJob.toBuilder();

    if (!JobKeys.isValid(rawJob.getKey())) {
      throw new TaskDescriptionException("Job key " + rawJob.getKey() + " is invalid.");
    }

    if (rawJob.isSetOwner()) {
      assertOwnerValidity(rawJob.getOwner());

      if (!rawJob.getKey().getRole().equals(rawJob.getOwner().getRole())) {
        throw new TaskDescriptionException("Role in job key must match job owner.");
      }
    }

    builder.setTaskConfig(
        validateAndPopulate(rawJob.getTaskConfig()));

    JobConfiguration job = builder.build();
    // Only one of [service=true, cron_schedule] may be set.
    if (!Strings.isNullOrEmpty(job.getCronSchedule()) && job.getTaskConfig().isIsService()) {
      throw new TaskDescriptionException(
          "A service task may not be run on a cron schedule: " + builder);
    }
    return job;
  }

  /**
   * Check validity of and populates defaults in a task configuration.  This will return a deep copy
   * of the provided task configuration with default configuration values applied, and configuration
   * map values sanitized and applied to their respective struct fields.
   *
   *
   * @param rawConfig Task config to validate and populate.
   * @return A reference to the modified {@code config} (for chaining).
   * @throws TaskDescriptionException If the task is invalid.
   */
  public static TaskConfig validateAndPopulate(TaskConfig rawConfig)
      throws TaskDescriptionException {

    TaskConfig.Builder builder = rawConfig.toBuilder();

    maybeFillLinks(builder, rawConfig);

    if (!isGoodIdentifier(rawConfig.getJobName())) {
      throw new TaskDescriptionException(
          "Job name contains illegal characters: " + rawConfig.getJobName());
    }

    if (!isGoodIdentifier(rawConfig.getEnvironment())) {
      throw new TaskDescriptionException(
          "Environment contains illegal characters: " + rawConfig.getEnvironment());
    }

    if (rawConfig.isSetTier() && !isGoodIdentifier(rawConfig.getTier())) {
      throw new TaskDescriptionException("Tier contains illegal characters: " + rawConfig.getTier());
    }

    if (rawConfig.isSetJob()) {
      if (!JobKeys.isValid(rawConfig.getJob())) {
        // Job key is set but invalid
        throw new TaskDescriptionException("Job key " + rawConfig.getJob() + " is invalid.");
      }

      if (!rawConfig.getJob().getRole().equals(rawConfig.getOwner().getRole())) {
        // Both owner and job key are set but don't match
        throw new TaskDescriptionException("Role must match job owner.");
      }
    } else {
      // TODO(maxim): Make sure both key and owner are populated to support older clients.
      // Remove in 0.7.0. (AURORA-749).
      // Job key is not set -> populate from owner, environment and name
      assertOwnerValidity(rawConfig.getOwner());
      builder.setJob(JobKeys.from(
          rawConfig.getOwner().getRole(),
          rawConfig.getEnvironment(),
          rawConfig.getJobName()));
    }

    if (!rawConfig.isSetExecutorConfig()) {
      throw new TaskDescriptionException("Configuration may not be null");
    }

    // Maximize the usefulness of any thrown error message by checking required fields first.
    TaskConfig validatable = builder.build();
    for (RequiredFieldValidator<?> validator : REQUIRED_FIELDS_VALIDATORS) {
      validator.validate(validatable);
    }

    Constraint constraint = getDedicatedConstraint(rawConfig);
    if (constraint != null) {
      if (!isValueConstraint(constraint.getConstraint())) {
        throw new TaskDescriptionException("A dedicated constraint must be of value type.");
      }

      ValueConstraint valueConstraint = constraint.getConstraint().getValue();

      if (valueConstraint.getValues().size() != 1) {
        throw new TaskDescriptionException("A dedicated constraint must have exactly one value");
      }

      String dedicatedRole = getRole(valueConstraint);
      if (!rawConfig.getOwner().getRole().equals(dedicatedRole)) {
        throw new TaskDescriptionException(
            "Only " + dedicatedRole + " may use hosts dedicated for that role.");
      }
    }

    Container containerConfig = rawConfig.getContainer();
    Optional<Container._Fields> containerType = Optional.of(containerConfig.getSetField());
    if (containerConfig.isSetDocker()) {
      if (!containerConfig.getDocker().isSetImage()) {
        throw new TaskDescriptionException("A container must specify an image");
      }
      if (!containerConfig.getDocker().getParameters().isEmpty()
          && !ENABLE_DOCKER_PARAMETERS.get()) {
        throw new TaskDescriptionException("Docker parameters not allowed.");
      }
    }
    if (!ALLOWED_CONTAINER_TYPES.get().contains(containerType.get())) {
      throw new TaskDescriptionException(
          "The container type " + containerType.get().toString() + " is not allowed");
    }

    return builder.build();
  }

  /**
   * Provides a filter for the given constraint name.
   *
   * @param name The name of the constraint.
   * @return A filter that matches the constraint.
   */
  public static Predicate<Constraint> getConstraintByName(final String name) {
    return constraint -> constraint.getName().equals(name);
  }

  private static void maybeFillLinks(TaskConfig.Builder builder, TaskConfig task) {
    if (task.getTaskLinks().isEmpty()) {
      ImmutableMap.Builder<String, String> links = ImmutableMap.builder();
      if (task.getRequestedPorts().contains("health")) {
        links.put("health", "http://%host%:%port:health%");
      }
      if (task.getRequestedPorts().contains("http")) {
        links.put("http", "http://%host%:%port:http%");
      }
      builder.setTaskLinks(links.build());
    }
  }

  /**
   * Thrown when an invalid task or job configuration is encountered.
   */
  public static class TaskDescriptionException extends Exception {
    public TaskDescriptionException(String msg) {
      super(msg);
    }
  }
}
