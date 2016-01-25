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
package org.apache.aurora.scheduler.http.api.security;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;
import javax.inject.Provider;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.AbstractSequentialIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.gen.AddInstancesConfig;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateRequest;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.ResponseCode;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.http.api.security.FieldGetter.AbstractFieldGetter;
import org.apache.aurora.scheduler.http.api.security.FieldGetter.IdentityFieldGetter;
import org.apache.aurora.scheduler.spi.Permissions;
import org.apache.aurora.scheduler.spi.Permissions.Domain;
import org.apache.aurora.scheduler.thrift.Responses;
import org.apache.aurora.thrift.ImmutableThriftAnnotation;
import org.apache.aurora.thrift.ThriftAnnotation;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.subject.Subject;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkState;

/**
 * Interceptor that extracts and validates job keys from parameters annotated with
 * {@literal @Annotation(@Parameter(name = "authorizing", value = "authorizing"))} and performs
 * permission checks scoped to it.
 *
 * <p>
 * For example, if intercepting a class that implements {@code A}:
 *
 * <pre>
 * public interface A {
 *   Response setInstances(
 *       @Annotation(@Parameter(name = "authorizing", value = "true")) JobKey jobKey,
 *       int instances);
 * }
 * </pre>
 *
 * This interceptor will check that the current {@link org.apache.shiro.subject.Subject} has the
 * permission (prefix + ":setInstances:role:env:name").
 *
 * <p>
 * It is important that this interceptor only be applied to methods returning
 * {@link org.apache.aurora.gen.Response} and that authentication is called before this interceptor
 * is invoked, otherwise this interceptor will not allow the invocation to proceed.
 */
class ShiroAuthorizingParamInterceptor implements MethodInterceptor {
  @VisibleForTesting
  static final FieldGetter<TaskQuery, JobKey> QUERY_TO_JOB_KEY =
      new AbstractFieldGetter<TaskQuery, JobKey>(TaskQuery.class) {
        @Override
        public Optional<JobKey> apply(TaskQuery input) {
          Optional<Set<JobKey>> targetJobs = JobKeys.from(Query.arbitrary(input));
          if (targetJobs.isPresent() && targetJobs.get().size() == 1) {
            return Optional.of(Iterables.getOnlyElement(targetJobs.get()));
          } else {
            return Optional.absent();
          }
        }
      };

  private static class JobKeyGetter {
    private final int index;
    private final Function<Object, Optional<JobKey>> func;

    JobKeyGetter(int index, Function<Object, Optional<JobKey>> func) {
      this.index = index;
      this.func = func;
    }
  }

  private static final FieldGetter<JobUpdateRequest, TaskConfig> UPDATE_REQUEST_GETTER =
      new ThriftFieldGetter<>(
          JobUpdateRequest.class,
          JobUpdateRequest.Fields.TASK_CONFIG,
          TaskConfig.class);

  private static final FieldGetter<TaskConfig, JobKey> TASK_CONFIG_GETTER =
      new ThriftFieldGetter<>(TaskConfig.class, TaskConfig.Fields.JOB, JobKey.class);

  private static final FieldGetter<JobConfiguration, JobKey> JOB_CONFIGURATION_GETTER =
      new ThriftFieldGetter<>(JobConfiguration.class, JobConfiguration.Fields.KEY, JobKey.class);

  private static final FieldGetter<Lock, LockKey> LOCK_GETTER =
      new ThriftFieldGetter<>(Lock.class, Lock.Fields.KEY, LockKey.class);

  private static final FieldGetter<LockKey, JobKey> LOCK_KEY_GETTER =
      new ThriftFieldGetter<>(LockKey.class, LockKey.Fields.JOB, JobKey.class);

  private static final FieldGetter<JobUpdateKey, JobKey> JOB_UPDATE_KEY_GETTER =
      new ThriftFieldGetter<>(JobUpdateKey.class, JobUpdateKey.Fields.JOB, JobKey.class);

  private static final FieldGetter<AddInstancesConfig, JobKey> ADD_INSTANCES_CONFIG_GETTER =
      new ThriftFieldGetter<>(
          AddInstancesConfig.class,
          AddInstancesConfig.Fields.KEY,
          JobKey.class);

  @SuppressWarnings("unchecked")
  private static final Set<FieldGetter<?, JobKey>> FIELD_GETTERS =
      ImmutableSet.of(
          FieldGetters.compose(UPDATE_REQUEST_GETTER, TASK_CONFIG_GETTER),
          TASK_CONFIG_GETTER,
          JOB_CONFIGURATION_GETTER,
          FieldGetters.compose(LOCK_GETTER, LOCK_KEY_GETTER),
          LOCK_KEY_GETTER,
          JOB_UPDATE_KEY_GETTER,
          ADD_INSTANCES_CONFIG_GETTER,
          QUERY_TO_JOB_KEY,
          new IdentityFieldGetter<>(JobKey.class));

  private static final Map<Class<?>, Function<?, Optional<JobKey>>> FIELD_GETTERS_BY_TYPE =
      ImmutableMap.<Class<?>, Function<?, Optional<JobKey>>>builder()
          .putAll(Maps.uniqueIndex(FIELD_GETTERS, FieldGetter::getStructClass))
          .build();

  private static final ThriftAnnotation AUTHORIZING_PARAM =
      ImmutableThriftAnnotation.of("authorizing", "true");

  @VisibleForTesting
  static final String SHIRO_AUTHORIZATION_FAILURES = "shiro_authorization_failures";

  @VisibleForTesting
  static final String SHIRO_BAD_REQUESTS = "shiro_bad_requests";

  /**
   * Return each method in the inheritance hierarchy of the given method in breadth-first order,
   * superclass before interfaces.
   */
  private static Iterable<Method> getCandidateMethods(final Method method) {
    return () -> new AbstractSequentialIterator<Method>(method) {
      @Override
      protected Method computeNext(Method previous) {
        String name = previous.getName();
        Class<?>[] parameterTypes = previous.getParameterTypes();
        Class<?> declaringClass = previous.getDeclaringClass();

        if (declaringClass.isInterface()) {
          return null;
        }

        Iterable<Class<?>> searchOrder = ImmutableList.<Class<?>>builder()
            .addAll(Optional.fromNullable(declaringClass.getSuperclass()).asSet())
            .addAll(ImmutableList.copyOf(declaringClass.getInterfaces()))
            .build();

        for (Class<?> klazz : searchOrder) {
          try {
            return klazz.getMethod(name, parameterTypes);
          } catch (NoSuchMethodException ignored) {
            // Expected.
          }
        }

        return null;
      }
    };
  }

  private static Iterable<JobKeyGetter> annotatedParameterGetters(Method method) {
    for (Method candidateMethod : getCandidateMethods(method)) {
      Parameter[] parameters = candidateMethod.getParameters();
      ImmutableList.Builder<JobKeyGetter> jobKeyGetters = ImmutableList.builder();
      for (int i = 0; i < parameters.length; i++) {
        Parameter param = parameters[i];
        if (AUTHORIZING_PARAM.equals(param.getAnnotation(ThriftAnnotation.class))) {
          Class<?> parameterType = param.getType();
          @SuppressWarnings("unchecked")
          Optional<Function<Object, Optional<JobKey>>> jobKeyGetter =
              Optional.fromNullable(
                  (Function<Object, Optional<JobKey>>) FIELD_GETTERS_BY_TYPE.get(parameterType));
          if (!jobKeyGetter.isPresent()) {
            throw new UnsupportedOperationException(
                "No "
                    + JobKey.class.getName()
                    + " field getter was supplied for "
                    + parameterType.getName());
          }

          jobKeyGetters.add(new JobKeyGetter(i, jobKeyGetter.get()));
        }
      }

      ImmutableList<JobKeyGetter> getters = jobKeyGetters.build();
      if (!Iterables.isEmpty(getters)) {
        return getters;
      }
    }

    throw new UnsupportedOperationException(
        "No parameter annotated with "
            + AUTHORIZING_PARAM
            + " found on method "
            + method.getName()
            + " of "
            + method.getDeclaringClass().getName()
            + " or any of its superclasses.");
  }

  private static final CacheLoader<Method, Function<Object[], Optional<JobKey>>> LOADER =
      new CacheLoader<Method, Function<Object[], Optional<JobKey>>>() {
        @Override
        public Function<Object[], Optional<JobKey>> load(Method method) {
          if (!Response.class.isAssignableFrom(method.getReturnType())) {
            throw new UnsupportedOperationException(
                "Method "
                    + method.getName()
                    + " of class "
                    + method.getDeclaringClass().getName()
                    + " does not return "
                    + Response.class.getName());
          }

          Iterable<JobKeyGetter> getters = annotatedParameterGetters(method);
          return arguments -> {
            Iterable<JobKeyGetter> nonNullArgGetters =
                Iterables.filter(getters, getter -> arguments[getter.index] != null);
            if (Iterables.isEmpty(nonNullArgGetters)) {
              return Optional.absent();
            } else {
              if (Iterables.size(nonNullArgGetters) > 1) {
                throw new IllegalStateException(
                    "Too many non-null arguments annotated with "
                        + AUTHORIZING_PARAM
                        + " passed to "
                        + method.getName()
                        + " of "
                        + method.getDeclaringClass().getName());
              }

              JobKeyGetter getter = Iterables.getOnlyElement(nonNullArgGetters);
              return getter.func.apply(arguments[getter.index]);
            }
          };
        }
      };

  private final LoadingCache<Method, Function<Object[], Optional<JobKey>>> authorizingParamGetters =
      CacheBuilder.newBuilder().build(LOADER);

  private final Domain domain;

  private volatile boolean initialized;

  private Provider<Subject> subjectProvider;
  private AtomicLong authorizationFailures;
  private AtomicLong badRequests;

  ShiroAuthorizingParamInterceptor(Domain domain) {
    this.domain = requireNonNull(domain);
  }

  @Inject
  void initialize(Provider<Subject> newSubjectProvider, StatsProvider statsProvider) {
    checkState(!initialized);

    this.subjectProvider = requireNonNull(newSubjectProvider);
    authorizationFailures = statsProvider.makeCounter(SHIRO_AUTHORIZATION_FAILURES);
    badRequests = statsProvider.makeCounter(SHIRO_BAD_REQUESTS);

    initialized = true;
  }

  @VisibleForTesting
  Permission makeWildcardPermission(String methodName) {
    return Permissions.createUnscopedPermission(domain, methodName);
  }

  @VisibleForTesting
  Permission makeTargetPermission(String methodName, JobKey jobKey) {
    return Permissions.createJobScopedPermission(methodName, jobKey);
  }

  @Override
  public Object invoke(MethodInvocation invocation) throws Throwable {
    checkState(initialized);

    Method method = invocation.getMethod();
    // Short-circuit request processing restrictions if the caller would be allowed to
    // operate on every possible job key. This allows a broadly-scoped TaskQuery.
    Subject subject = subjectProvider.get();
    if (subject.isPermitted(makeWildcardPermission(method.getName()))) {
      return invocation.proceed();
    }

    Optional<JobKey> jobKey = authorizingParamGetters
        .getUnchecked(invocation.getMethod())
        .apply(invocation.getArguments());
    if (jobKey.isPresent() && JobKeys.isValid(jobKey.get())) {
      Permission targetPermission = makeTargetPermission(method.getName(), jobKey.get());
      if (subject.isPermitted(targetPermission)) {
        return invocation.proceed();
      } else {
        authorizationFailures.incrementAndGet();
        return Responses.create(
            ResponseCode.AUTH_FAILED,
            "Subject " + subject + " is not permitted to " + targetPermission + ".");
      }
    } else {
      badRequests.incrementAndGet();
      return Responses.invalidRequest("Missing or invalid job key from request.");
    }
  }

  @VisibleForTesting
  LoadingCache<Method, Function<Object[], Optional<JobKey>>> getAuthorizingParamGetters() {
    return authorizingParamGetters;
  }
}
