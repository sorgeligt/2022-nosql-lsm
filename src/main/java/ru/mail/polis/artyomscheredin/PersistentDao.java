package ru.mail.polis.artyomscheredin;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PersistentDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final SortedMap<ByteBuffer, BaseEntry<ByteBuffer>> inMemoryData =
            new ConcurrentSkipListMap<>(ByteBuffer::compareTo);

    private final Storage storage;

    public PersistentDao(Config config) throws IOException {
        if (config == null) {
            throw new IllegalArgumentException();
        }
        this.storage = new Storage(config.basePath());
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        lock.readLock().lock();
        try {
            List<PeekIterator> iterators = storage.getListOfOnDiskIterators(from, to);
            if (!inMemoryData.isEmpty()) {
                iterators.add(new PeekIterator(getInMemoryIterator(from, to), iterators.size() + 1));
            }
            return new MergeIterator(iterators);
        } finally {
            lock.readLock().unlock();
        }
    }

    private Iterator<BaseEntry<ByteBuffer>> getInMemoryIterator(ByteBuffer from, ByteBuffer to) {
        if ((from == null) && (to == null)) {
            return inMemoryData.values().iterator();
        } else if (from == null) {
            return inMemoryData.headMap(to).values().iterator();
        } else if (to == null) {
            return inMemoryData.tailMap(from).values().iterator();
        } else {
            return inMemoryData.subMap(from, to).values().iterator();
        }
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        if (entry == null) {
            throw new IllegalArgumentException();
        }
        lock.readLock().lock();
        try {
            inMemoryData.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        lock.writeLock().lock();
        try {
            if (inMemoryData.isEmpty()) {
                return;
            }
            storage.storeToTempFile(inMemoryData.values());
            storage.renameTempFile();
            storage.mapNextStorageUnit();
            inMemoryData.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    @Override
    public void compact() throws IOException {
        flush();
        if (storage.getMappedDataSize() == 1) {
            return;
        }
        Iterator<BaseEntry<ByteBuffer>> mergeIterator = get(null, null);
        if (!mergeIterator.hasNext()) {
            return;
        }
        storage.storeToTempFile(() -> get(null, null));

        Storage.cleanDiskExceptTempFile(storage.getBasePath());
        storage.cleanMappedData();
        storage.renameTempFile();
        storage.mapNextStorageUnit();
    }
}
