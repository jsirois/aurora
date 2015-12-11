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

import java.util.Map;
import java.util.Optional;

import javax.inject.Inject;

import com.google.common.collect.Maps;

import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.DockerContainer;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.ValueConstraint;
import org.apache.aurora.scheduler.storage.db.views.DbTaskConfig;
import org.apache.aurora.scheduler.storage.db.views.Pairs;

import static java.util.Objects.requireNonNull;

class TaskConfigManager {
  private final TaskConfigMapper configMapper;
  private final JobKeyMapper jobKeyMapper;

  @Inject
  TaskConfigManager(TaskConfigMapper configMapper, JobKeyMapper jobKeyMapper) {
    this.configMapper = requireNonNull(configMapper);
    this.jobKeyMapper = requireNonNull(jobKeyMapper);
  }

  private Optional<Long> getConfigRow(TaskConfig config) {
    // We could optimize this slightly by first comparing the un-hydrated row and breaking early.

    Map<TaskConfig, DbTaskConfig> rowsByConfig =
        Maps.uniqueIndex(
            configMapper.selectConfigsByJob(config.getJob()),
            DbTaskConfig::toThrift);

    return Optional.ofNullable(rowsByConfig.get(config)).map(DbTaskConfig::getRowId);
  }

  long insert(TaskConfig config) {
    InsertResult configInsert = new InsertResult();

    // Determine whether this config is already stored.
    Optional<Long> existingRow = getConfigRow(config);
    if (existingRow.isPresent()) {
      return existingRow.get();
    }

    jobKeyMapper.merge(config.getJob());
    configMapper.insert(config, configInsert);
    for (Constraint constraint : config.getConstraints()) {
      InsertResult constraintResult = new InsertResult();
      configMapper.insertConstraint(configInsert.getId(), constraint, constraintResult);
      switch (constraint.getConstraint().getSetField()) {
        case VALUE:
          ValueConstraint valueConstraint = constraint.getConstraint().getValue();
          InsertResult valueResult = new InsertResult();
          configMapper.insertValueConstraint(
              constraintResult.getId(),
              valueConstraint,
              valueResult);
          configMapper.insertValueConstraintValues(
              valueResult.getId(),
              valueConstraint.getValues());
          break;

        case LIMIT:
          configMapper.insertLimitConstraint(
              constraintResult.getId(),
              constraint.getConstraint().getLimit());
          break;

        default:
          throw new IllegalStateException(
              "Unhandled constraint type " + constraint.getConstraint().getSetField());
      }
    }

    if (!config.getRequestedPorts().isEmpty()) {
      configMapper.insertRequestedPorts(configInsert.getId(), config.getRequestedPorts());
    }

    if (!config.getTaskLinks().isEmpty()) {
      configMapper.insertTaskLinks(
          configInsert.getId(),
          Pairs.fromMap(config.getTaskLinks()));
    }

    if (!config.getMetadata().isEmpty()) {
      configMapper.insertMetadata(configInsert.getId(), config.getMetadata());
    }

    // TODO(wfarner): It would be nice if this generalized to different Container types.
    if (config.getContainer().isSetDocker()) {
      DockerContainer container = config.getContainer().getDocker();
      InsertResult containerInsert = new InsertResult();
      configMapper.insertContainer(configInsert.getId(), container, containerInsert);
      if (!container.getParameters().isEmpty()) {
        configMapper.insertDockerParameters(containerInsert.getId(), container.getParameters());
      }
    }

    return configInsert.getId();
  }
}
