package ru.mail.polis.daniilbakin;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static java.nio.file.StandardOpenOption.READ;
import static ru.mail.polis.daniilbakin.Storage.DATA_FILE_NAME;
import static ru.mail.polis.daniilbakin.Storage.INDEX_FILE_NAME;

public class MapsDeserializeStream implements Closeable {

    private final List<MappedByteBuffer> mapData = new ArrayList<>();
    private final List<MappedByteBuffer> indexesData = new ArrayList<>();
    private int numOfFiles;
    private final Method unmap;
    private final Object fieldValue;

    public MapsDeserializeStream(Config config, int numOfFiles, int startIndexOfFile) throws IOException {
        this.numOfFiles = numOfFiles;

        for (int i = startIndexOfFile; i < numOfFiles + startIndexOfFile; i++) {
            addMappedFile(i, config.basePath());
        }

        Collections.reverse(mapData);
        Collections.reverse(indexesData);

        try {
            unmap = Class.forName("sun.misc.Unsafe").getMethod("invokeCleaner", ByteBuffer.class);
            unmap.setAccessible(true);
            Field theUnsafeField = Class.forName("sun.misc.Unsafe").getDeclaredField("theUnsafe");
            theUnsafeField.setAccessible(true);
            fieldValue = theUnsafeField.get(null);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public void addMappedFile(int index, Path basePath) throws IOException {
        Path mapPath = basePath.resolve(DATA_FILE_NAME + index);
        Path indexesPath = basePath.resolve(INDEX_FILE_NAME + index);

        FileChannel mapChannel = (FileChannel) Files.newByteChannel(mapPath, Set.of(READ));
        FileChannel indexesChannel = (FileChannel) Files.newByteChannel(indexesPath, Set.of(READ));

        mapData.add(mapChannel.map(FileChannel.MapMode.READ_ONLY, 0, mapChannel.size()));
        indexesData.add(indexesChannel.map(FileChannel.MapMode.READ_ONLY, 0, indexesChannel.size()));

        mapChannel.close();
        indexesChannel.close();
    }

    public void updateNumOfFiles(int newNum) {
        numOfFiles = newNum;
    }

    @Override
    public void close() throws IOException {
        try {
            for (int i = 0; i < numOfFiles; i++) {
                unmap.invoke(fieldValue, mapData.get(i));
                unmap.invoke(fieldValue, indexesData.get(i));
            }
        } catch (ReflectiveOperationException e) {
            throw new IOException(e);
        }
        mapData.clear();
        indexesData.clear();
    }

    public BaseEntry<ByteBuffer> readByKey(ByteBuffer key) {
        for (int i = 0; i < numOfFiles; i++) {
            BaseEntry<ByteBuffer> entry = readByKey(key, i);
            if (entry != null) {
                return entry;
            }
        }
        return null;
    }

    public List<PeekIterator<BaseEntry<ByteBuffer>>> getIteratorsOfRange(ByteBuffer from, ByteBuffer to) {
        List<PeekIterator<BaseEntry<ByteBuffer>>> iterators = new ArrayList<>();
        for (int i = 0; i < numOfFiles; i++) {
            iterators.add(getIterator(from, to, i));
        }
        return iterators;
    }

    private PeekIterator<BaseEntry<ByteBuffer>> getIterator(ByteBuffer from, ByteBuffer to, int fileIndex) {
        MappedByteBuffer indexesBuffer = indexesData.get(fileIndex);
        MappedByteBuffer mapBuffer = mapData.get(fileIndex);
        int startIndex = (from == null) ? 0 : binarySearchIndex(from, indexesBuffer, mapBuffer, true);
        int endIndex = (to == null) ? indexesBuffer.capacity() / Integer.BYTES
                : binarySearchIndex(to, indexesBuffer, mapBuffer, true);

        return new PeekIterator<>(new FileIterator(startIndex, endIndex, indexesBuffer, mapBuffer), fileIndex);
    }

    private BaseEntry<ByteBuffer> readByKey(ByteBuffer key, int index) {
        MappedByteBuffer indexesBuffer = indexesData.get(index);
        MappedByteBuffer mapBuffer = mapData.get(index);
        int keyIndex = binarySearchIndex(key, indexesBuffer, mapBuffer, false);
        if (keyIndex == -1) {
            return null;
        }
        return readEntry(getInternalIndexByOrder(keyIndex, indexesBuffer), mapBuffer);
    }

    private int binarySearchIndex(
            ByteBuffer key, MappedByteBuffer indexesBuffer, MappedByteBuffer mapBuffer, boolean needClosestRight
    ) {
        int size = indexesBuffer.capacity() / Integer.BYTES;
        int first = 0;
        int last = size;
        int position = first + (last - first) / 2;

        ByteBuffer currKey = readNotNullByteBuffer(getInternalIndexByOrder(position, indexesBuffer), mapBuffer);

        int compare = currKey.compareTo(key);
        while ((compare != 0) && (first <= last)) {
            if (compare > 0) {
                last = position - 1;
            } else {
                first = position + 1;
            }

            position = first + (last - first) / 2;
            if (position == size) {
                break;
            }
            currKey = readNotNullByteBuffer(getInternalIndexByOrder(position, indexesBuffer), mapBuffer);
            compare = currKey.compareTo(key);
        }
        if (first <= last && position != size) {
            return position;
        }
        if (needClosestRight) {
            if (compare > 0 || position == size) {
                return position;
            } else {
                return position + 1;
            }
        }
        return -1;
    }

    /**
     * Position in bytes.
     */
    private BaseEntry<ByteBuffer> readEntry(int position, MappedByteBuffer mapBuffer) {
        ByteBuffer key = readNotNullByteBuffer(position, mapBuffer);
        ByteBuffer value = readByteBuffer(position + key.capacity() + Integer.BYTES, mapBuffer);
        return new BaseEntry<>(key.duplicate(), value);
    }

    private ByteBuffer readNotNullByteBuffer(int position, MappedByteBuffer mapBuffer) {
        ByteBuffer notNull = readByteBuffer(position, mapBuffer);
        assert notNull != null;
        return notNull;
    }

    /**
     * Position in bytes.
     */
    private ByteBuffer readByteBuffer(int position, MappedByteBuffer mapBuffer) {
        int length = mapBuffer.getInt(position);
        if (length == -1) {
            return null;
        }
        return mapBuffer.slice(position + Integer.BYTES, length);
    }

    private int getInternalIndexByOrder(int order, MappedByteBuffer indexesBuffer) {
        return indexesBuffer.getInt(order * Integer.BYTES);
    }

    private class FileIterator implements Iterator<BaseEntry<ByteBuffer>> {

        private final MappedByteBuffer indexesBuffer;
        private final MappedByteBuffer mapBuffer;
        private final int size;
        private int next;

        FileIterator(int starIndext, int endIndex, MappedByteBuffer indexesBuffer, MappedByteBuffer mapBuffer) {
            next = starIndext;
            size = endIndex;
            this.indexesBuffer = indexesBuffer;
            this.mapBuffer = mapBuffer;
        }

        @Override
        public boolean hasNext() {
            return next < size;
        }

        @Override
        public BaseEntry<ByteBuffer> next() {
            return readEntry(getInternalIndexByOrder(next++, indexesBuffer), mapBuffer);
        }
    }

}
