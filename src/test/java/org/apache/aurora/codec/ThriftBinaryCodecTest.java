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

import java.io.IOException;
import java.nio.ByteBuffer;

import com.facebook.swift.codec.ThriftCodec;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.codec.ThriftBinaryCodec.ByteBufferInputStream;
import org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.thrift.ThriftEntity;
import org.apache.aurora.thrift.ThriftFields.NoFields;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class ThriftBinaryCodecTest {

  @Test
  public void testRoundTrip() throws CodingException {
    Identity original = Identity.create("mesos", "jack");
    assertEquals(original,
        ThriftBinaryCodec.decode(Identity.class, ThriftBinaryCodec.encode(original)));
  }

  @Test
  public void testRoundTripNull() throws CodingException {
    assertNull(ThriftBinaryCodec.decode(Identity.class, ThriftBinaryCodec.encode(null)));
  }

  @Test
  public void testRoundTripNonNull() throws CodingException {
    Identity original = Identity.create("mesos", "jill");
    assertEquals(original,
        ThriftBinaryCodec.decodeNonNull(Identity.class, ThriftBinaryCodec.encodeNonNull(original)));
  }

  @Test(expected = NullPointerException.class)
  public void testEncodeNonNull() throws CodingException {
    ThriftBinaryCodec.encodeNonNull(null);
  }

  @Test(expected = NullPointerException.class)
  public void testDecodeNonNull() throws CodingException {
    ThriftBinaryCodec.decodeNonNull(Identity.class, null);
  }

  @Test
  public void testCodecForThrift() {
    ThriftCodec<Identity> identityCodec = ThriftBinaryCodec.codecForType(Identity.class);
    assertNotNull(identityCodec);

    assertSame(identityCodec, ThriftBinaryCodec.codecForType(Identity.class));

    ThriftCodec<LockKey> lockKeyCodec = ThriftBinaryCodec.codecForType(LockKey.class);
    assertNotNull(identityCodec);
    assertNotSame(identityCodec, lockKeyCodec);
  }

  static class MyThriftEntity implements ThriftEntity<NoFields> {
    @Override public boolean isSet(NoFields field) {
      throw new IllegalStateException("Not implemented");
    }

    @Override public Object getFieldValue(NoFields field) {
      throw new IllegalStateException("Not implemented");
    }

    @Override public ImmutableSet<NoFields> getFields() {
      throw new IllegalStateException("Not implemented");
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCodecForUnregisteredThrift() {
    ThriftBinaryCodec.codecForType(MyThriftEntity.class);
  }

  @Test(expected = CodingException.class)
  public void testDecodeUnregisteredThrift() throws CodingException {
    ThriftBinaryCodec.decodeNonNull(MyThriftEntity.class, new byte[0]);
  }

  @Test(expected = CodingException.class)
  public void testEncodeUnregisteredThrift() throws CodingException {
    ThriftBinaryCodec.encodeNonNull(new MyThriftEntity());
  }

  @Test(expected = CodingException.class)
  public void testDeflateUnregisteredThrift() throws CodingException {
    ThriftBinaryCodec.deflateNonNull(new MyThriftEntity());
  }

  @Test(expected = CodingException.class)
  public void testInflateUnregisteredThrift() throws CodingException {
    ThriftBinaryCodec.inflateNonNull(MyThriftEntity.class, new byte[0]);
  }

  @Test
  public void testInflateDeflateRoundTrip() throws CodingException {
    Identity original = Identity.create("aurora", "jsmith");

    byte[] deflated = ThriftBinaryCodec.deflateNonNull(original);

    Identity inflated = ThriftBinaryCodec.inflateNonNull(Identity.class, deflated);

    assertEquals(original, inflated);
  }

  @Test
  public void testByteBufferInputStreamRead() throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {0x0, 0x1});
    try (ByteBufferInputStream stream = new ByteBufferInputStream(buffer)) {
      assertEquals(0x0, stream.read());
      assertEquals(0x1, stream.read());
      assertEquals(-1, stream.read());
      assertEquals(-1, stream.read());
    }
  }

  @Test
  public void testByteBufferInputStreamReadArray() throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {0xC, 0xA, 0xF, 0xE, 0xB, 0xA, 0xB, 0xE, 0x9});
    try (ByteBufferInputStream stream = new ByteBufferInputStream(buffer)) {
      byte[] copyBuf = new byte[4];
      assertEquals(2, stream.read(copyBuf, 0, 2));
      assertArrayEquals(new byte[] {0xC, 0xA, 0x0, 0x0}, copyBuf);

      assertEquals(2, stream.read(copyBuf, 2, 2));
      assertArrayEquals(new byte[] {0xC, 0xA, 0xF, 0xE}, copyBuf);

      assertEquals(4, stream.read(copyBuf, 0, 4));
      assertArrayEquals(new byte[] {0xB, 0xA, 0xB, 0xE}, copyBuf);

      assertEquals(1, stream.read(copyBuf, 0, 4));
      assertArrayEquals(new byte[] {0x9, 0xA, 0xB, 0xE}, copyBuf);

      assertEquals(-1, stream.read(copyBuf, 0, 4));
      assertArrayEquals(new byte[] {0x9, 0xA, 0xB, 0xE}, copyBuf);
    }
  }
}
