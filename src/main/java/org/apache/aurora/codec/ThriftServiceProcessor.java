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
package org.apache.aurora.codec;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.facebook.nifty.core.RequestContext;
import com.facebook.nifty.processor.NiftyProcessor;
import com.facebook.swift.codec.ThriftCodecManager;
import com.facebook.swift.service.ContextChain;
import com.facebook.swift.service.RuntimeTException;
import com.facebook.swift.service.ThriftEventHandler;
import com.facebook.swift.service.ThriftMethodProcessor;
import com.facebook.swift.service.metadata.ThriftMethodMetadata;
import com.facebook.swift.service.metadata.ThriftServiceMetadata;
import com.google.auto.value.AutoValue;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TType;

import autovalue.shaded.com.google.common.common.collect.ImmutableList;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkArgument;

import static org.apache.thrift.TApplicationException.INTERNAL_ERROR;
import static org.apache.thrift.TApplicationException.INVALID_MESSAGE_TYPE;
import static org.apache.thrift.TApplicationException.UNKNOWN_METHOD;

public class ThriftServiceProcessor implements NiftyProcessor {
  private static final Logger LOG = Logger.getLogger(ThriftServiceProcessor.class.getName());
  private final ImmutableList<? extends ThriftEventHandler> eventHandlers;

  @AutoValue
  public abstract static class ServiceDescriptor {
    public static ServiceDescriptor create(Object service, Class<?> serviceInterface) {
      requireNonNull(service);
      requireNonNull(serviceInterface);
      checkArgument(serviceInterface.isInstance(service));

      return new AutoValue_ThriftServiceProcessor_ServiceDescriptor(service, serviceInterface);
    }

    public abstract Object getService();
    public abstract Class<?> getInterface();
  }

  private final ImmutableMap<String, ThriftMethodProcessor> methodProcessors;

  ThriftServiceProcessor(
      ThriftCodecManager codecManager,
      Iterable<? extends ThriftEventHandler> eventHandlers,
      ServiceDescriptor... services) {

    requireNonNull(codecManager);
    requireNonNull(eventHandlers);
    requireNonNull(services);

    ImmutableMap.Builder<String, ThriftMethodProcessor> methodProcessors = ImmutableMap.builder();
    for (ServiceDescriptor descriptor : services) {
      ThriftServiceMetadata serviceMetadata =
          new ThriftServiceMetadata(descriptor.getInterface(), codecManager.getCatalog());

      for (ThriftMethodMetadata thriftMethodMetadata : serviceMetadata.getMethods().values()) {
        ThriftMethodProcessor methodProcessor =
            new ThriftMethodProcessor(
                descriptor.getService(),
                serviceMetadata.getName(),
                thriftMethodMetadata,
                codecManager);
        methodProcessors.put(thriftMethodMetadata.getName(), methodProcessor);
      }
    }
    this.methodProcessors = methodProcessors.build();
    this.eventHandlers = ImmutableList.copyOf(eventHandlers);
  }

  private static final Supplier<Constructor<ContextChain>> CONTEXT_CHAIN_CONSTRUCTOR =
      Suppliers.memoize(() -> {
        try {
          // TODO(John Sirois): XXX Upstream a fix that either elevates ServiceDescriptor as 1st
          // class in com.facebook.swift.service.ThriftServiceProcessor or else, less appealing,
          // expose the ContextChain constructor or interface.
          Constructor<ContextChain> constructor =
              ContextChain.class.getDeclaredConstructor(
                  List.class,
                  String.class,
                  RequestContext.class);
          constructor.setAccessible(true);
          return constructor;
        } catch (NoSuchMethodException e) {
          throw new RuntimeTException(
              "Unexpected problem setting up service method call.",
              new TException(e));
        }
      });

  private static Optional<ContextChain> createContextChain(
      TProtocol out, int sequenceId, List<? extends ThriftEventHandler> eventHandlers,
      String qualifiedName,
      RequestContext requestContext) throws TException {

    Constructor<ContextChain> constructor = CONTEXT_CHAIN_CONSTRUCTOR.get();
    try {
      return Optional.of(constructor.newInstance(eventHandlers, qualifiedName, requestContext));
    } catch (ReflectiveOperationException e) {
      writeApplicationException(
          out,
          qualifiedName,
          sequenceId,
          INTERNAL_ERROR,
          "Problem dispatching method call to '" + qualifiedName + "'",
          e);
      return Optional.empty();
    }
  }

  @Override
  public ListenableFuture<Boolean> process(
      TProtocol in,
      TProtocol out,
      RequestContext requestContext)
      throws TException {

    try {
      final SettableFuture<Boolean> resultFuture = SettableFuture.create();
      TMessage message = in.readMessageBegin();
      String methodName = message.name;
      int sequenceId = message.seqid;

      // lookup method
      ThriftMethodProcessor method = methodProcessors.get(methodName);
      if (method == null) {
        TProtocolUtil.skip(in, TType.STRUCT);
        writeApplicationException(
            out,
            methodName,
            sequenceId,
            UNKNOWN_METHOD,
            "Invalid method name: '" + methodName + "'",
            null);
        return Futures.immediateFuture(true);
      }

      switch (message.type) {
        case TMessageType.CALL:
        case TMessageType.ONEWAY:
          // Ideally we'd check the message type here to make the presence/absence of
          // the "oneway" keyword annotating the method matches the message type.
          // Unfortunately most clients send both one-way and two-way messages as CALL
          // message type instead of using ONEWAY message type, and servers ignore the
          // difference.
          break;

        default:
          TProtocolUtil.skip(in, TType.STRUCT);
          writeApplicationException(
              out,
              methodName,
              sequenceId,
              INVALID_MESSAGE_TYPE,
              "Received invalid message type " + message.type + " from client",
              null);
          return Futures.immediateFuture(true);
      }

      // invoke method
      Optional<ContextChain> contextChain =
          createContextChain(
              out,
              sequenceId,
              eventHandlers,
              method.getQualifiedName(),
              requestContext);

      if (!contextChain.isPresent()) {
        return Futures.immediateFuture(true);
      }
      ContextChain context = contextChain.get();

      ListenableFuture<Boolean> processResult = method.process(in, out, sequenceId, context);

      Futures.addCallback(
          processResult,
          new FutureCallback<Boolean>() {
            @Override public void onSuccess(Boolean result) {
              context.done();
              resultFuture.set(result);
            }

            @Override public void onFailure(Throwable t) {
              context.done();
              resultFuture.setException(t);
            }
          });

      return resultFuture;
    }
    catch (Exception e) {
      return Futures.immediateFailedFuture(e);
    }
  }

  private static TApplicationException writeApplicationException(
      TProtocol outputProtocol,
      String methodName,
      int sequenceId,
      int errorCode,
      String errorMessage,
      Throwable cause)
      throws TException {

    // unexpected exception
    TApplicationException applicationException = new TApplicationException(errorCode, errorMessage);
    if (cause != null) {
      applicationException.initCause(cause);
    }

    LOG.log(Level.SEVERE, errorMessage, applicationException);

    // Application exceptions are sent to client, and the connection can be reused
    outputProtocol.writeMessageBegin(new TMessage(methodName, TMessageType.EXCEPTION, sequenceId));
    applicationException.write(outputProtocol);
    outputProtocol.writeMessageEnd();
    outputProtocol.getTransport().flush();

    return applicationException;
  }
}
