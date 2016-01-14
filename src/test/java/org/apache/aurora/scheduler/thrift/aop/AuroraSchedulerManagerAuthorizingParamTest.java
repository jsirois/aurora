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

import java.lang.reflect.Method;
import java.util.Collection;

import com.google.common.collect.Collections2;
import com.google.common.reflect.Invokable;
import com.google.common.reflect.Parameter;

import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.AuroraSchedulerManager;
import org.apache.aurora.thrift.ImmutableParameter;
import org.apache.aurora.thrift.ImmutableThriftAnnotation;
import org.apache.aurora.thrift.ThriftAnnotation;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AuroraSchedulerManagerAuthorizingParamTest {
  // TODO(John Sirois): DRY this up with ShiroAuthorizingParamInterceptor
  private static final ThriftAnnotation AUTHORIZING_PARAM =
      ImmutableThriftAnnotation.builder()
          .value(ImmutableParameter.of("authorizing", "true"))
          .build();

  @Test
  public void testAllAuroraSchedulerManagerSyncMethodsHaveAuthorizingParam() throws Exception {
    for (Method declaredMethod : AuroraSchedulerManager.Sync.class.getDeclaredMethods()) {
      Invokable<?, ?> invokable = Invokable.from(declaredMethod);
      // We only care about the abstract interface methods an not the reflection helper methods.
      if (invokable.isAbstract()) {
        // Confirm the abstract method is in-fact a thrift method.
        Method thriftMethod = AuroraSchedulerManager.Sync.getThriftMethod(declaredMethod.getName());
        assertEquals(declaredMethod, thriftMethod);

        // Confirm the method has 1 authorizing parameter identified.
        Collection<Parameter> parameters = invokable.getParameters();
        Invokable<?, ?> annotatedInvokable = Invokable.from(
            AuroraAdmin.Sync.class.getMethod(
                invokable.getName(),
                parameters.stream()
                    .map(input -> input.getType().getRawType())
                    .toArray(Class[]::new)));

        Collection<Parameter> annotatedParameters = Collections2.filter(
            annotatedInvokable.getParameters(),
            param -> AUTHORIZING_PARAM.equals(param.getAnnotation(ThriftAnnotation.class)));

        assertEquals(
            "Method " + invokable + " should have 1 " + AUTHORIZING_PARAM
                + " annotation but " + annotatedParameters.size() + " were found.",
            1,
            annotatedParameters.size());
      }
    }
  }
}
