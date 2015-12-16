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

import java.nio.ByteBuffer;

import org.apache.aurora.codec.ThriftBinaryCodec.ByteBufferInputStream;
import org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import org.apache.aurora.gen.Identity;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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
  public void testInflateDeflateRoundTrip() throws CodingException {
    Identity original = Identity.create("aurora", "jsmith");

    byte[] deflated = ThriftBinaryCodec.deflateNonNull(original);

    Identity inflated = ThriftBinaryCodec.inflateNonNull(Identity.class, deflated);

    assertEquals(original, inflated);
  }

  @Test
  public void testByteBufferInputStreamRead() {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {0x0, 0x1});
    ByteBufferInputStream stream = new ByteBufferInputStream(buffer);
    assertEquals(0x0, stream.read());
    assertEquals(0x1, stream.read());
    assertEquals(-1, stream.read());
    assertEquals(-1, stream.read());
  }

  @Test
  public void testByteBufferInputStreamReadArray() {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8});
    ByteBufferInputStream stream = new ByteBufferInputStream(buffer);

    byte[] copyBuf = new byte[4];
    assertEquals(2, stream.read(copyBuf, 0, 2));
    assertArrayEquals(new byte[] {0x0, 0x1, 0x0, 0x0}, copyBuf);

    assertEquals(2, stream.read(copyBuf, 2, 2));
    assertArrayEquals(new byte[] {0x0, 0x1, 0x2, 0x3}, copyBuf);

    assertEquals(4, stream.read(copyBuf, 0, 4));
    assertArrayEquals(new byte[] {0x4, 0x5, 0x6, 0x7}, copyBuf);

    assertEquals(1, stream.read(copyBuf, 0, 4));
    assertArrayEquals(new byte[] {0x8, 0x5, 0x6, 0x7}, copyBuf);

    assertEquals(-1, stream.read(copyBuf, 0, 4));
    assertArrayEquals(new byte[] {0x8, 0x5, 0x6, 0x7}, copyBuf);
  }
}
