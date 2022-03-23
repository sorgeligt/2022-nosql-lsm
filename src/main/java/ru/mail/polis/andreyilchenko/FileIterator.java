package ru.mail.polis.andreyilchenko;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Iterator;

public class FileIterator implements Iterator<BaseEntry<ByteBuffer>> {
    private final Path dataPath;
    private final Path offsetsPath;
    private final ByteBuffer to;
    private final long maxOffsetsFilePointer;
    private BaseEntry<ByteBuffer> prevElem;
    private long offsetPointer;
    private boolean flagNotNext;

    public FileIterator(Path dataPath, Path offsetsPath, long startOffset, long maxOffsetsFilePointer, ByteBuffer to) {
        this.dataPath = dataPath;
        this.offsetsPath = offsetsPath;
        this.offsetPointer = startOffset;
        this.maxOffsetsFilePointer = maxOffsetsFilePointer;
        this.to = to;
        if (startOffset >= maxOffsetsFilePointer) {
            flagNotNext = true;
            return;
        }
        try (
                RandomAccessFile dataReader = new RandomAccessFile(dataPath.toFile(), "r");
                FileChannel dataChannel = dataReader.getChannel();
                RandomAccessFile offsetsReader = new RandomAccessFile(offsetsPath.toFile(), "r")
        ) {
            boolean isNull = false;
            offsetsReader.seek(offsetPointer);
            offsetPointer += 8;
            int keyStartOffset = offsetsReader.readInt();
            int valueStartOffset = offsetsReader.readInt();
            int valueEndOffset = offsetsReader.readInt();
            if (valueStartOffset == -1) {
                valueStartOffset = valueEndOffset;
                isNull = true;
            }
            ByteBuffer probableKey = ByteBuffer.allocate(valueStartOffset - keyStartOffset);
            dataChannel.read(probableKey, keyStartOffset);
            probableKey.flip();
            ByteBuffer value = ByteBuffer.allocate(valueEndOffset - valueStartOffset);
            dataChannel.read(value, valueStartOffset);
            value.flip();
            prevElem = new BaseEntry<>(probableKey, isNull ? null : value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean hasNext() {
        boolean b = true;
        if (to != null && !flagNotNext) {
            b = prevElem.key().compareTo(to) < 0;
        }
        return !flagNotNext && b && maxOffsetsFilePointer >= offsetPointer;
    }

    @Override
    public BaseEntry<ByteBuffer> next() {
        try (
                RandomAccessFile dataReader = new RandomAccessFile(dataPath.toFile(), "r");
                FileChannel dataChannel = dataReader.getChannel();
                RandomAccessFile offsetsReader = new RandomAccessFile(offsetsPath.toFile(), "r")
        ) {
            if (maxOffsetsFilePointer <= offsetPointer + 4) {
                offsetPointer += 8;
                return prevElem;
            }
            boolean isNull = false;
            offsetsReader.seek(offsetPointer);
            offsetPointer += 8;
            int keyStartOffset = offsetsReader.readInt();
            int valueStartOffset = offsetsReader.readInt();
            int valueEndOffset = offsetsReader.readInt();
            if (valueStartOffset == -1) {
                valueStartOffset = valueEndOffset;
                isNull = true;
            }
            ByteBuffer probableKey = ByteBuffer.allocate(valueStartOffset - keyStartOffset);
            dataChannel.read(probableKey, keyStartOffset);
            probableKey.flip();
            ByteBuffer value = ByteBuffer.allocate(valueEndOffset - valueStartOffset);
            dataChannel.read(value, valueStartOffset);
            value.flip();
            BaseEntry<ByteBuffer> returnElem = prevElem;
            prevElem = new BaseEntry<>(probableKey, isNull ? null : value);
            return returnElem;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
