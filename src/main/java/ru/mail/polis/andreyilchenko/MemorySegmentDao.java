package ru.mail.polis.andreyilchenko;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private static final MemorySegment VERY_FIRST_KEY = MemorySegment.ofArray(new byte[]{});

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memory =
            new ConcurrentSkipListMap<>(MemorySegmentComparator.INSTANCE);

    private final Storage storage;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Config config;

    public MemorySegmentDao(Config config) throws IOException {
        this.config = config;
        this.storage = Storage.load(config);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        MemorySegment copyFrom = from;
        if (from == null) {
            copyFrom = VERY_FIRST_KEY;
        }

        List<IndexPeekIterator<Entry<MemorySegment>>> iterators = new ArrayList<>();
        int index = 0;
        iterators.add(new IndexPeekIterator<>(index++, getMemoryIterator(copyFrom, to)));
        for (Iterator<Entry<MemorySegment>> iterator : storage.sstablesIterate(copyFrom, to)) {
            iterators.add(new IndexPeekIterator<>(index++, iterator));
        }
        Iterator<Entry<MemorySegment>> iterator = MergeIterator.of(iterators, EntryKeyComparator.INSTANCE);
        return new Iterator<>() {
            private Entry<MemorySegment> current;

            @Override
            public boolean hasNext() {
                if (current != null) {
                    return true;
                }

                while (iterator.hasNext()) {
                    Entry<MemorySegment> entry = iterator.next();
                    if (entry.value() != null) {
                        current = entry;
                        return true;
                    }
                }

                return false;
            }

            @Override
            public Entry<MemorySegment> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Entry<MemorySegment> next = current;
                current = null;
                return next;
            }
        };
    }

    private Iterator<Entry<MemorySegment>> getMemoryIterator(MemorySegment from, MemorySegment to) {
        lock.readLock().lock();
        try {

            if (to == null) {
                return memory.tailMap(from).values().iterator();
            }
            return memory.subMap(from, to).values().iterator();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> result = memory.get(key);
        if (result == null) {
            result = storage.get(key);
        }

        return (result == null || result.value() == null) ? null : result;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        lock.readLock().lock();
        try {
            memory.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void compact() throws IOException {

        lock.writeLock();
        try {
            Storage.compact(config, storage, get(null, null), memory);
        } finally {
            lock.writeLock().lock();
        }
    }

    @Override
    public void close() throws IOException {
        if (storage.isClosed()) {
            return;
        }

        storage.close();
        lock.writeLock().lock();
        try {
            if (!storage.isClosed()) {
                throw new IllegalStateException("Previous storage is open for write");
            }
            Storage.save(config, memory.values());
        } finally {
            lock.writeLock().unlock();
        }
    }
}