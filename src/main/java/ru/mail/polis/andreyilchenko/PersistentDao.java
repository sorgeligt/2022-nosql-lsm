package ru.mail.polis.andreyilchenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Logger;

public class PersistentDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    private static final char DATA_SEPARATOR = '\u0017';
    private static final char KEY_VALUE_SEPARATOR = '\u001d';
    private static final int DEFAULT_ALLOCATE_BUFFER_SIZE = 0x400;
    private static final Logger log = Logger.getLogger(PersistentDao.class.getName());

    private final int allocateBufferSize;
    private final ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> entries = new ConcurrentSkipListMap<>();
    private final Path pathToData;

    public PersistentDao(Config config, int allocateBufferSize) {
        this.pathToData = config.basePath().toAbsolutePath();
        this.allocateBufferSize = allocateBufferSize;
        if (Files.notExists(pathToData)) {
            try {
                Files.createFile(pathToData);
            } catch (IOException e) {
                log.severe("FileSystem error, cannot create file");
            }
        }
    }

    public PersistentDao(Config config) {
        this(config, DEFAULT_ALLOCATE_BUFFER_SIZE);
    }

    @Override
    public void flush() throws IOException {
        if (Files.exists(pathToData)) {
            Files.delete(pathToData);
        }
        try (RandomAccessFile reader = new RandomAccessFile(pathToData.toFile(), "rw");
             FileChannel channel = reader.getChannel()) {
            reader.write(DATA_SEPARATOR); // separator at the beginning of the file
            for (BaseEntry<ByteBuffer> entry : entries.values()) {
                int keyLen = entry.key().remaining();
                int valueLen = entry.value().remaining();
                ByteBuffer bufferToWrite = ByteBuffer.allocate(keyLen + valueLen + 4);
                bufferToWrite.put(entry.key())
                        .putChar(KEY_VALUE_SEPARATOR)
                        .put(entry.value())
                        .putChar(DATA_SEPARATOR)
                        .flip();
                channel.write(bufferToWrite);
                bufferToWrite.clear();
            }
        }
    }

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) throws IOException {
        BaseEntry<ByteBuffer> entry = entries.get(key);
        return entry == null ? findInFile(key) : entry;
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        entries.put(entry.key(), entry);
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if (to == null && from == null) {
            return entries.values().iterator();
        }
        if (to == null) {
            return entries.tailMap(from).values().iterator();
        }
        if (from == null) {
            return entries.headMap(to).values().iterator();
        }
        return entries.subMap(from, to).values().iterator();
    }

    private BaseEntry<ByteBuffer> findInFile(ByteBuffer key) throws IOException {
        try (RandomAccessFile reader = new RandomAccessFile(pathToData.toFile(), "r")) {
            long fileLength = reader.length();
            if (fileLength == 0) { // File is empty
                return null;
            }
            return getValueUsingBinarySearch(key, reader, 0, fileLength);
        }
    }

    private BaseEntry<ByteBuffer> getValueUsingBinarySearch(
            ByteBuffer key, RandomAccessFile reader, long fileStartPosition, long fileLength) throws IOException {
        byte tempByte;
        long startIndex = fileStartPosition;
        long endIndex = fileLength;
        long midIndex;
        while (startIndex <= endIndex) {
            midIndex = (endIndex + startIndex) >> 1;
            reader.seek(midIndex);
            moveToNextDataSeparator(reader);
            try {
                tempByte = reader.readByte();
            } catch (EOFException e) { // the binary search part is not included in the file boundaries
                if (midIndex == 0) {
                    return checkFirstEntry(key, reader);
                }
                endIndex = midIndex;
                continue;
            }

            if (tempByte == key.get(0)) { // probably found the necessary key
                ByteBuffer possibleKey = readTheRemainingKey(reader, tempByte);
                int compareResult = possibleKey.compareTo(key);

                if (compareResult == 0) {
                    ByteBuffer value = readToSeparator(reader, DATA_SEPARATOR);
                    return new BaseEntry<>(key, value);
                } else if (compareResult > 0) {
                    endIndex = midIndex;
                } else {
                    startIndex = midIndex;
                }
            } else if (tempByte > key.get(0)) {
                endIndex = midIndex;
            } else {
                startIndex = midIndex;
            }
        }
        return null;
    }

    private void moveToNextDataSeparator(RandomAccessFile reader) throws IOException {
        byte tempByte;
        do {
            tempByte = reader.readByte();
        } while (tempByte != DATA_SEPARATOR);
    }

    private ByteBuffer readTheRemainingKey(RandomAccessFile reader, byte firstByte) throws IOException {
        ByteBuffer key = ByteBuffer.allocate(allocateBufferSize);
        key.put(firstByte);
        int i = 0;
        byte tempByte;
        while ((tempByte = reader.readByte()) != KEY_VALUE_SEPARATOR) {
            i++;
            key.put(tempByte);
        }
        key.position(0);
        key.limit(i);
        return key;
    }

    private ByteBuffer readToSeparator(RandomAccessFile raf, int separator) throws IOException {
        ByteBuffer value = ByteBuffer.allocate(allocateBufferSize);
        byte tempByte;
        int i = 0;
        while ((tempByte = raf.readByte()) != separator) {
            i++;
            value.put(tempByte);
        }
        value.position(0);
        value.limit(i - 1);
        return value;
    }

    private BaseEntry<ByteBuffer> checkFirstEntry(ByteBuffer key, RandomAccessFile randomAccessFile)
            throws IOException {
        randomAccessFile.seek(0);
        ByteBuffer probablyKey = readToSeparator(randomAccessFile, KEY_VALUE_SEPARATOR);
        if (probablyKey.equals(key)) {
            ByteBuffer value = readToSeparator(randomAccessFile, DATA_SEPARATOR);
            return new BaseEntry<>(probablyKey, value);
        }
        return null;
    }
}
