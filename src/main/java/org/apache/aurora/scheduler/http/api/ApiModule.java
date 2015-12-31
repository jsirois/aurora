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
package org.apache.aurora.scheduler.http.api;

import java.io.IOException;

import javax.inject.Singleton;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.HttpMethod;

import com.facebook.swift.service.ThriftEventHandler;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Provides;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

import org.apache.aurora.codec.ThriftBinaryCodec;
import org.apache.aurora.codec.ThriftServiceProcessor.ServiceDescriptor;
import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.CmdLine;
import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.scheduler.http.CorsFilter;
import org.apache.aurora.scheduler.http.JettyServerModule;
import org.apache.aurora.scheduler.http.LeaderRedirectFilter;
import org.apache.aurora.scheduler.http.api.security.UnauthenticatedError;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.server.TServlet;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlets.GzipFilter;
import org.eclipse.jetty.util.resource.Resource;

public class ApiModule extends ServletModule {
  public static final String API_PATH = "/api";

  @CmdLine(name = "enable_cors_support", help = "Enable CORS support for thrift end points.")
  private static final Arg<Boolean> ENABLE_CORS_SUPPORT = Arg.create(false);

  /**
   * Set the {@code Access-Control-Allow-Origin} header for API requests. See
   * http://www.w3.org/TR/cors/
   */
  @CmdLine(name = "enable_cors_for",
      help = "List of domains for which CORS support should be enabled.")
  private static final Arg<String> ENABLE_CORS_FOR = Arg.create("*");

  private static final String API_CLIENT_ROOT = Resource
      .newClassPathResource("org/apache/aurora/scheduler/gen/client")
      .toString();

  @Override
  protected void configureServlets() {
    if (ENABLE_CORS_SUPPORT.get()) {
      filter(API_PATH).through(new CorsFilter(ENABLE_CORS_FOR.get()));
    }
    // NOTE: GzipFilter is applied only to /api instead of globally because the
    // Jersey-managed servlets have a conflicting filter applied to them.
    filter(API_PATH)
        .through(
            new GzipFilter(),
            ImmutableMap.of("methods", Joiner.on(',').join(HttpMethod.GET, HttpMethod.POST)));
    serve(API_PATH).with(TServlet.class);

    filter(ApiBeta.PATH, ApiBeta.PATH + "/*").through(LeaderRedirectFilter.class);
    filter(ApiBeta.PATH, ApiBeta.PATH + "/*")
        .through(GuiceContainer.class, JettyServerModule.GUICE_CONTAINER_PARAMS);
    bind(ApiBeta.class);

    serve("/apiclient", "/apiclient/*")
        .with(new DefaultServlet(), ImmutableMap.<String, String>builder()
            .put("resourceBase", API_CLIENT_ROOT)
            .put("pathInfoOnly", "true")
            .put("dirAllowed", "false")
            .build());
  }

  @Provides
  @Singleton
  TServlet provideApiThriftServlet(AuroraAdmin.Sync schedulerThriftInterface) {
    ServiceDescriptor serviceDescriptor =
        ServiceDescriptor.create(schedulerThriftInterface, AuroraAdmin.Sync.class);

    ThriftEventHandler propagateUnauthenticatedError = new ThriftEventHandler() {
      @Override
      public void preWriteException(Object context, String methodName, Throwable t) {
        // We need to abort standard thrift error handling here (which marshals exceptions into the
        // response payload) and raise an Exception that can be bubbled up to the Shiro
        // authentication Servlet filter layer.  With Swift/Netty/Futures pipeline, this can only be
        // done via an Error; thus the UnauthenticatedError is re-thrown here to reach the TServlet
        // Servlet layer just a few lines below.
        if (t instanceof UnauthenticatedError) {
          throw (UnauthenticatedError) t;
        }
        super.preWriteException(context, methodName, t);
      }
    };

    TProcessor processor =
        ThriftBinaryCodec.processorFor(propagateUnauthenticatedError, serviceDescriptor);

    return new TServlet(processor, new TJSONProtocol.Factory()) {
      @Override
      protected void doPost(HttpServletRequest request, HttpServletResponse response)
          throws ServletException, IOException {

        // NB: All TServlet handling routes through doPost.
        try {
          super.doPost(request, response);
        } catch (UnauthenticatedError e) {
          // This throws a special exception type up through the Servlet filter layer that will
          // signal the Shiro BasicHttpAuthenticationFilter to send a 401 with a challenge.
          throw e.authenticationChallenge();
        }
      }
    };
  }
}
