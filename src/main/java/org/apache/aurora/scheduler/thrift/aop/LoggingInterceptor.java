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
package org.apache.aurora.scheduler.thrift.aop;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.ResponseCode;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.thrift.Responses;
import org.apache.shiro.ShiroException;

/**
 * A method interceptor that logs all invocations as well as any unchecked exceptions thrown from
 * the underlying call.
 */
class LoggingInterceptor implements MethodInterceptor {
  private static final Logger LOG = Logger.getLogger(LoggingInterceptor.class.getName());

  private static String renderJobConfiguration(JobConfiguration configuration) {
    if (configuration.isSetTaskConfig()) {
      TaskConfig taskConfig = configuration.getTaskConfig();
      return configuration.toBuilder()
          .setTaskConfig(
              taskConfig.toBuilder()
                  .setExecutorConfig(ExecutorConfig.create("BLANKED", "BLANKED"))
                  .build())
          .build()
          .toString();
    } else {
      return configuration.toString();
    }
  }

  @Override
  public Object invoke(MethodInvocation invocation) throws Throwable {
    List<String> argStrings = Lists.newArrayList();
    for (Object arg : invocation.getArguments()) {
      if (arg == null) {
        argStrings.add("null");
      } else {
        if (JobConfiguration.class.isInstance(arg)) {
          argStrings.add(renderJobConfiguration((JobConfiguration) arg));
        } else {
          argStrings.add(arg.toString());
        }
      }
    }
    String methodName = invocation.getMethod().getName();
    String message = String.format("%s(%s)", methodName, String.join(", ", argStrings));
    LOG.info(message);
    try {
      return invocation.proceed();
    } catch (Storage.TransientStorageException e) {
      LOG.log(Level.WARNING, "Uncaught transient exception while handling " + message, e);
      return Responses.error(ResponseCode.ERROR_TRANSIENT, e);
    } catch (RuntimeException e) {
      // We need shiro's exceptions to bubble up to the Shiro servlet filter so we intentionally
      // do not swallow them here.
      Throwables.propagateIfInstanceOf(e, ShiroException.class);
      LOG.log(Level.WARNING, "Uncaught exception while handling " + message, e);
      return Responses.error(ResponseCode.ERROR, e);
    }
  }
}
