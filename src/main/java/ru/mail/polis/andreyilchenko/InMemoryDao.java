package ru.mail.polis.andreyilchenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    private static final char DATA_SEPARATOR = '\u0017';
    private static final char KEY_VALUE_SEPARATOR = '\u001d';
    private static final int DEFAULT_ALLOCATE_BUFFER_SIZE = 0x400;

    private final int allocateBufferSize;
    private final ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> entries = new ConcurrentSkipListMap<>();
    private final File file;

    public InMemoryDao(Config config, int allocateBufferSize) {
        this.file = config.basePath().toFile();
        this.allocateBufferSize = allocateBufferSize;
        if (Files.notExists(file.toPath())) {
            try {
                Files.createFile(file.toPath());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public InMemoryDao(Config config) {
        this(config, DEFAULT_ALLOCATE_BUFFER_SIZE);
    }

    @Override
    public synchronized void flush() throws IOException {
        try (RandomAccessFile reader = new RandomAccessFile(file, "rw");
             FileChannel channel = reader.getChannel()) {
            ByteBuffer bufferToWrite = ByteBuffer.allocate(allocateBufferSize);
            bufferToWrite.putChar(DATA_SEPARATOR); // separator at the beginning of the file
            for (BaseEntry<ByteBuffer> entry : entries.values()) {
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
        return entries.get(key) == null ? findInFile(key) : entries.get(key);
    }


    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        entries.put(entry.key(), entry);
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if (entries.isEmpty()) {
            return Collections.emptyIterator();
        }
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
        try (RandomAccessFile reader = new RandomAccessFile(file, "rw")) {
            byte tempByte;
            long startIndex = 0;
            long endIndex = reader.length();
            long midIndex;
            while (startIndex <= endIndex) {
                midIndex = (endIndex + startIndex) >> 1;
                reader.seek(midIndex);
                while (reader.readByte() != DATA_SEPARATOR) {/*skip bytes*/}
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
                    if (possibleKey.equals(key)) {
                        ByteBuffer value = readToSeparator(reader, DATA_SEPARATOR);
                        return new BaseEntry<>(key, value);
                    } else if (possibleKey.compareTo(key) > 0) {
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
