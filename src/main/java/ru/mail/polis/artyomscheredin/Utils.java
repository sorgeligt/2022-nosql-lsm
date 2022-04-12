package ru.mail.polis.artyomscheredin;

import ru.mail.polis.BaseEntry;

import java.nio.ByteBuffer;

public final class Utils {

    private Utils() {
    }

    public record BufferPair(ByteBuffer data, ByteBuffer index) {
        //empty body
    }

    public record BufferSizePair(int dataBufferSize, int indexBufferSize) {
        //empty body
    }

    public static BaseEntry<ByteBuffer> readEntry(ByteBuffer dataBuffer, int sourceOffset) {
        int offset = sourceOffset;
        int keySize = dataBuffer.getInt(offset);
        offset += Integer.BYTES;
        ByteBuffer curKey = dataBuffer.slice(offset, keySize);
        offset += keySize;
        int valueSize = dataBuffer.getInt(offset);
        ByteBuffer curValue = null;
        if (valueSize != -1) {
            offset += Integer.BYTES;
            curValue = dataBuffer.slice(offset, valueSize);
        }
        return new BaseEntry<>(curKey, curValue);
    }
}
