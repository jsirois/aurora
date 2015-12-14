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

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.logging.Logger;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import javax.annotation.Nullable;

import com.facebook.nifty.processor.NiftyProcessor;
import com.facebook.nifty.processor.NiftyProcessorAdapters;
import com.facebook.swift.codec.ThriftCodec;
import com.facebook.swift.codec.ThriftCodecManager;
import com.facebook.swift.codec.internal.compiler.CompilerThriftCodecFactory;
import com.facebook.swift.codec.metadata.MetadataErrorException;
import com.facebook.swift.codec.metadata.MetadataErrors;
import com.facebook.swift.codec.metadata.MetadataWarningException;
import com.facebook.swift.codec.metadata.ThriftCatalog;
import com.google.common.primitives.UnsignedBytes;

import org.apache.aurora.codec.ThriftServiceProcessor.ServiceDescriptor;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.thrift.ThriftEntity;
import org.apache.aurora.thrift.ThriftEntity.ThriftStruct;
import org.apache.aurora.thrift.ThriftEntity.ThriftUnion;
import org.apache.thrift.TByteArrayOutputStream;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;

import autovalue.shaded.com.google.common.common.base.Predicate;
import autovalue.shaded.com.google.common.common.base.Predicates;
import autovalue.shaded.com.google.common.common.collect.FluentIterable;
import autovalue.shaded.com.google.common.common.collect.ImmutableList;
import autovalue.shaded.com.google.common.common.collect.ImmutableSet;

import static java.util.Objects.requireNonNull;

/**
 * Codec that works for thrift objects.
 */
public final class ThriftBinaryCodec {

  // NB: As of 12/2/2015 and thrift 0.9.3, the default underlying TByteArrayOutputStream otherwise
  // used is 32 bytes (mimicking the underlying java 1.8 ByteArrayOutputStream default).  With no
  // hard snashot data to go from, 10KB seems like a size that would need no expansion for many
  // cases save very large task configs and snapshots.  For snapshots we get to 5GB in 10 doublings,
  // which seems reasonable.
  // TODO(John Sirois): Actually test this value for some set of real-world transaction logs and
  // tune in a data-driven way.
  public static final int DEFAULT_BUFFER_SIZE = 1024 * 10;

  private static TProtocol createProtocol(TTransport transport) {
    return getProtocol(transport);
  }

  private static TProtocol getProtocol(TTransport transport) {
    return new TBinaryProtocol.Factory().getProtocol(transport);
  }

  private static TByteArrayOutputStream createBuffer() {
    return new TByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
  }

  private static final Logger LOG = Logger.getLogger(ThriftBinaryCodec.class.getName());

  private static final ThriftCodecManager CODEC_MANAGER =
      new ThriftCodecManager(
          new CompilerThriftCodecFactory(/* debug */ false),
          new ThriftCatalog(new MetadataErrors.Monitor() {
            @Override public void onError(MetadataErrorException errorMessage) {
              LOG.severe(errorMessage.toString());
            }
            @Override public void onWarning(MetadataWarningException warningMessage) {
              LOG.warning(warningMessage.toString());
            }
          }),
          ImmutableSet.of()); // A priori known codecs,

  // TODO(John Sirois): XXX DOCME
  public static TProcessor processorFor(ServiceDescriptor... services) {
    NiftyProcessor processor =
        new ThriftServiceProcessor(CODEC_MANAGER, ImmutableList.of(), services);
    return NiftyProcessorAdapters.processorToTProcessor(processor);
  }

  private static final Predicate<Class<?>> UNION_OR_STRUCT =
      Predicates.in(ImmutableSet.of(ThriftStruct.class, ThriftUnion.class));

  @SuppressWarnings("unchecked")
  public static <T extends ThriftEntity<?>> ThriftCodec<T> codecForType(Class<T> clazz) {
    Class<?> thriftEntity = clazz;
    while(thriftEntity != null
        && !FluentIterable.of(thriftEntity.getInterfaces()).anyMatch(UNION_OR_STRUCT)) {
      thriftEntity = thriftEntity.getSuperclass();
    }
    if (thriftEntity == null) {
      throw new IllegalArgumentException(
          String.format("%s is not a thrift struct", clazz.getTypeName()));
    }
    return CODEC_MANAGER.getCodec((Class<T>) thriftEntity);
  }

  private static <T extends ThriftEntity<?>> ThriftCodec<T> codecForObject(T thriftEntity) {
    @SuppressWarnings("unchecked") // Trivially safe under erasure
    Class<T> aClass = (Class<T>) thriftEntity.getClass();
    return codecForType(aClass);
  }

  private ThriftBinaryCodec() {
    // Utility class.
  }

  /**
   * Identical to {@link #decodeNonNull(Class, byte[])}, but allows for a null buffer.
   *
   * @param clazz Class to instantiate and deserialize to.
   * @param buffer Buffer to decode.
   * @param <T> Target type.
   * @return A populated message, or {@code null} if the buffer was {@code null}.
   * @throws CodingException If the message could not be decoded.
   */
  @Nullable
  public static <T extends ThriftEntity<?>> T decode(Class<T> clazz, @Nullable byte[] buffer)
      throws CodingException {

    if (buffer == null) {
      return null;
    }
    return decodeNonNull(clazz, buffer);
  }

  /**
   * Decodes a binary-encoded byte array into a target type.
   *
   * @param clazz Class to instantiate and deserialize to.
   * @param buffer Buffer to decode.
   * @param <T> Target type.
   * @return A populated message.
   * @throws CodingException If the message could not be decoded.
   */
  public static <T extends ThriftEntity<?>> T decodeNonNull(Class<T> clazz, byte[] buffer)
      throws CodingException {

    requireNonNull(clazz);
    requireNonNull(buffer);

    try {
      TProtocol protocol = createProtocol(new TIOStreamTransport(new ByteArrayInputStream(buffer)));
      return codecForType(clazz).read(protocol);
    } catch (Exception e) { // Unfortunately swift ThriftCodec.read throws Exception.
      throw new CodingException("Failed to deserialize thrift object.", e);
    }
  }

  /**
   * Identical to {@link #encodeNonNull(ThriftEntity)}, but allows for a null input.
   *
   * @param tBase Object to encode.
   * @return Encoded object, or {@code null} if the argument was {@code null}.
   * @throws CodingException If the object could not be encoded.
   */
  @Nullable
  public static <T extends ThriftEntity<?>> byte[] encode(@Nullable T tBase)
      throws CodingException {

    if (tBase == null) {
      return null;
    }
    return encodeNonNull(tBase);
  }

  /**
   * Encodes a thrift object into a binary array.
   *
   * @param tBase Object to encode.
   * @return Encoded object.
   * @throws CodingException If the object could not be encoded.
   */
  public static <T extends ThriftEntity<?>> byte[] encodeNonNull(T tBase) throws CodingException {
    requireNonNull(tBase);

    try {
      ThriftCodec<T> codec = codecForObject(tBase);
      TByteArrayOutputStream buffer = createBuffer();
      codec.write(tBase, getProtocol(new TIOStreamTransport(buffer)));
      return buffer.toByteArray();
    } catch (Exception e) {  // Unfortunately swift ThriftCodec.read throws Exception.
      throw new CodingException("Failed to serialize: " + tBase, e);
    }
  }

  // See http://www.zlib.net/zlib_how.html
  // "If the memory is available, buffers sizes on the order of 128K or 256K bytes should be used."
  private static final int DEFLATER_BUFFER_SIZE = Amount.of(256, Data.KB).as(Data.BYTES);

  // Empirical from microbenchmarks (assuming 20MiB/s writes to the replicated log and a large
  // de-duplicated Snapshot from a production environment).
  // TODO(ksweeney): Consider making this configurable.
  private static final int DEFLATE_LEVEL = 3;

  /**
   * Encodes a thrift object into a DEFLATE-compressed binary array.
   *
   * @param thriftEntity Object to encode.
   * @return Deflated, encoded object.
   * @throws CodingException If the object could not be encoded.
   */
  public static <T extends ThriftEntity<?>> byte[] deflateNonNull(T thriftEntity) throws CodingException {
    requireNonNull(thriftEntity);

    ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
    try {
      // NOTE: Buffering is needed here for performance.
      // There are actually 2 buffers in play here - the BufferedOutputStream prevents thrift from
      // causing a call to deflate() on every encoded primitive. The DeflaterOutputStream buffer
      // allows the underlying Deflater to operate on a larger chunk at a time without stopping to
      // copy the intermediate compressed output to outBytes.
      // See http://bugs.java.com/bugdatabase/view_bug.do?bug_id=4986239
      TTransport transport = new TIOStreamTransport(
          new BufferedOutputStream(
              new DeflaterOutputStream(outBytes, new Deflater(DEFLATE_LEVEL), DEFLATER_BUFFER_SIZE),
              DEFLATER_BUFFER_SIZE));
      TProtocol protocol = getProtocol(transport);

      ThriftCodec<T> codec = codecForObject(thriftEntity);
      codec.write(thriftEntity, protocol);
      transport.close();
      return outBytes.toByteArray();
    } catch (Exception e) { // Unfortunately swift ThriftCodec.read throws Exception.
      throw new CodingException("Failed to serialize: " + thriftEntity, e);
    }
  }

  /**
   * @param clazz Class to instantiate and deserialize to.
   * @param buffer Compressed buffer to decode.
   * @return A populated message.
   * @throws CodingException If the message could not be decoded.
   */
  public static <T extends ThriftEntity<?>> T inflateNonNull(Class<T> clazz, byte[] buffer)
      throws CodingException {
    return inflateNonNull(clazz, ByteBuffer.wrap(buffer));
  }

  /**
   * @param clazz Class to instantiate and deserialize to.
   * @param buffer Compressed buffer to decode.
   * @return A populated message.
   * @throws CodingException If the message could not be decoded.
   */
  public static <T extends ThriftEntity<?>> T inflateNonNull(Class<T> clazz, ByteBuffer buffer)
      throws CodingException {

    requireNonNull(clazz);
    requireNonNull(buffer);
    try {
      TTransport transport = new TIOStreamTransport(
          new InflaterInputStream(new ByteBufferInputStream(buffer)));
      TProtocol protocol = getProtocol(transport);
      return codecForType(clazz).read(protocol);
    } catch (Exception e) { // Unfortunately swift ThriftCodec.read throws Exception.
      throw new CodingException("Failed to deserialize: " + e, e);
    }
  }

  static class ByteBufferInputStream extends InputStream {
    private final ByteBuffer buffer;

    ByteBufferInputStream(ByteBuffer buffer) {
      this.buffer = buffer.duplicate();
    }

    @Override
    public int read() throws IOException {
      if (!buffer.hasRemaining()) {
        return -1;
      }
      return UnsignedBytes.toInt(buffer.get());
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (!buffer.hasRemaining()) {
        return -1;
      }
      int amount = Math.min(buffer.remaining(), len);
      buffer.get(b, off, amount);
      return amount;
    }
  }

  /**
   * Thrown when serialization or deserialization failed.
   */
  public static class CodingException extends Exception {
    public CodingException(String message) {
      super(message);
    }
    public CodingException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }
}
