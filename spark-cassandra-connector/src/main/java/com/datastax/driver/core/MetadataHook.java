package com.datastax.driver.core;

import java.nio.ByteBuffer;

/**
 * Wraps the driver's metadata to add the ability to build tokens from partition keys.
 * <p/>
 * This class has to live in {@code com.datastax.driver.core} in order to access package-private fields.
 */
public class MetadataHook {
    /**
     * Builds a new {@link Token} from a partition key, according to the partitioner reported by the Cassandra nodes.
     *
     * @param metadata               the original driver's metadata.
     * @param partitionKeyComponents the serialized value of the columns composing the partition key.
     * @return the token.
     * @throws IllegalStateException if the token factory was not initialized. This would typically
     *                               happen if metadata was explicitly disabled with {@link QueryOptions#setMetadataEnabled(boolean)}
     *                               before startup.
     */
    public static Token newToken(Metadata metadata, ByteBuffer... partitionKeyComponents) {
        return metadata.tokenFactory().hash(compose(partitionKeyComponents));
    }

    // Copied from SimpleStatement
    private static ByteBuffer compose(ByteBuffer[] buffers) {
        if (buffers.length == 1)
            return buffers[0];

        int totalLength = 0;
        for (ByteBuffer bb : buffers)
            totalLength += 2 + bb.remaining() + 1;

        ByteBuffer out = ByteBuffer.allocate(totalLength);
        for (ByteBuffer buffer : buffers) {
            ByteBuffer bb = buffer.duplicate();
            putShortLength(out, bb.remaining());
            out.put(bb);
            out.put((byte) 0);
        }
        out.flip();
        return out;
    }

    private static void putShortLength(ByteBuffer bb, int length) {
        bb.put((byte) ((length >> 8) & 0xFF));
        bb.put((byte) (length & 0xFF));
    }
}
