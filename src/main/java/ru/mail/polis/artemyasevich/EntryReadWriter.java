package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;

//Solving allocations issue in process
public class EntryReadWriter {
    private volatile ByteBuffer buffer;
    private volatile CharBuffer searchedKeyBuffer;

    EntryReadWriter(int bufferSize) {
        this.buffer = ByteBuffer.allocate(bufferSize);
        this.searchedKeyBuffer = CharBuffer.allocate(bufferSize);
    }

    //keySize|key|valueSize|value or key|keySize if value == null
    int writeEntryInStream(DataOutputStream dataStream, BaseEntry<String> entry) throws IOException {
        int keySize = entry.key().length() * 2;
        int valueBlockSize = 0;
        dataStream.writeShort(keySize);
        dataStream.writeChars(entry.key());
        if (entry.value() != null) {
            int valueSize = entry.value().length() * 2;
            valueBlockSize = valueSize + Short.BYTES;
            dataStream.writeShort(valueSize);
            dataStream.writeChars(entry.value());
        }
        return keySize + Short.BYTES + valueBlockSize;
    }

    BaseEntry<String> readEntryFromChannel(DaoFile daoFile, int index) throws IOException {
        return readEntryFromChannel(daoFile.getChannel(), daoFile.entrySize(index), daoFile.getOffset(index));
    }

    int getEntryIndex(String key, DaoFile daoFile) throws IOException {
        int left = 0;
        int right = daoFile.getLastIndex();
        while (left <= right) {
            int middle = (right - left) / 2 + left;
            CharBuffer middleKey = bufferAsKeyOnly(daoFile, middle);
            CharBuffer keyToFind = fillAndGetKeyBuffer(key);
            int comparison = keyToFind.compareTo(middleKey);
            if (comparison < 0) {
                right = middle - 1;
            } else if (comparison > 0) {
                left = middle + 1;
            } else {
                return middle;
            }
        }
        return left;
    }

    void increaseBufferSize(int maxEntrySize) {
        int newCapacity = Math.max(buffer.capacity() * 2, maxEntrySize);
        buffer = ByteBuffer.allocate(newCapacity);
        searchedKeyBuffer = CharBuffer.allocate(newCapacity);
    }

    int maxKeyLength() {
        return (buffer.capacity() / 2 - Short.BYTES / 2);
    }

    static long sizeOfEntry(BaseEntry<String> entry) {
        int valueSize = entry.value() == null ? 0 : entry.value().length();
        return (entry.key().length() + valueSize) * 2L;
    }

    private BaseEntry<String> readEntryFromChannel(FileChannel channel, int entrySize, long offset) throws IOException {
        String key = bufferAsKeyOnly(channel, entrySize, offset).toString();
        buffer.limit(entrySize);
        String value = null;
        if (buffer.hasRemaining()) {
            short valueSize = buffer.getShort();
            value = valueSize == 0 ? "" : buffer.asCharBuffer().toString();
        }
        return new BaseEntry<>(key, value);
    }

    private CharBuffer bufferAsKeyOnly(DaoFile daoFile, int index) throws IOException {
        return bufferAsKeyOnly(daoFile.getChannel(), daoFile.entrySize(index), daoFile.getOffset(index));
    }

    private CharBuffer bufferAsKeyOnly(FileChannel channel, int entrySize, long offset) throws IOException {
        fillBufferWithEntry(channel, entrySize, offset);
        short keySize = buffer.getShort();
        buffer.limit(keySize + Short.BYTES);
        CharBuffer key = buffer.asCharBuffer();
        buffer.position(Short.BYTES + keySize);
        return key;
    }

    private CharBuffer fillAndGetKeyBuffer(String key) {
        searchedKeyBuffer.clear();
        searchedKeyBuffer.put(key);
        searchedKeyBuffer.flip();
        return searchedKeyBuffer;
    }

    private void fillBufferWithEntry(FileChannel channel, int entrySize, long offset) throws IOException {
        buffer.clear();
        buffer.limit(entrySize);
        channel.read(buffer, offset);
        buffer.flip();
    }

}
