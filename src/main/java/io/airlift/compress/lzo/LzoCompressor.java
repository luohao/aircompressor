/*
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
package io.airlift.compress.lzo;

import io.airlift.compress.Compressor;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.nio.ByteBuffer;

import static io.airlift.compress.lzo.LzoRawCompressor.MAX_TABLE_SIZE;

/**
 * This class is not thread-safe
 * FIXME: for now, this class is a wrapper around the HadoopLzoCompressor and use native lzo under the hood.
 */
public class LzoCompressor
        implements Compressor
{
    private final int[] table = new int[MAX_TABLE_SIZE];
    private final com.hadoop.compression.lzo.LzoCodec codec;
    private static final Configuration HADOOP_CONF = new Configuration();
    private final org.apache.hadoop.io.compress.Compressor compressor;

    public LzoCompressor()
    {
        codec = new com.hadoop.compression.lzo.LzoCodec();
        codec.setConf(HADOOP_CONF);
        compressor = codec.createCompressor();
    }

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        return uncompressedSize + (uncompressedSize / 16) + 64 + 3;
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        compressor.reset();
        compressor.setInput(input, inputOffset, inputLength);
        compressor.finish();

        int offset = outputOffset;
        int outputLimit = outputOffset + maxOutputLength;
        while (!compressor.finished() && offset < outputLimit) {
            try {
                offset += compressor.compress(output, offset, outputLimit - offset);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        if (!compressor.finished()) {
            throw new RuntimeException("not enough space in output buffer");
        }

        return offset - outputOffset;
    }

    @Override
    public void compress(ByteBuffer input, ByteBuffer output)
    {
        byte[] in = new byte[input.remaining()];
        input.get(in);
        byte[] out = new byte[output.remaining()];

        // HACK: Assure JVM does not collect Slice wrappers while compressing, since the
        // collection may trigger freeing of the underlying memory resulting in a segfault
        // There is no other known way to signal to the JVM that an object should not be
        // collected in a block, and technically, the JVM is allowed to eliminate these locks.
        synchronized (input) {
            synchronized (output) {
                int written = compress(
                        in,
                        0,
                        in.length,
                        out,
                        0,
                        out.length);
                output.put(out, 0, written);
            }
        }
    }
}
