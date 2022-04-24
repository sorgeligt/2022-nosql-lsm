package ru.mail.polis.andreyilchenko;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private static final MemorySegment VERY_FIRST_KEY = MemorySegment.ofArray(new byte[]{});

    private final ExecutorService executor = Executors.newSingleThreadExecutor();


    private final ReadWriteLock upsetLock = new ReentrantReadWriteLock();

    private final Config config;
    private volatile State state;

    public MemorySegmentDao(Config config) throws IOException {
        this.config = config;
        this.state = State.newState(config, Storage.load(config));
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        State state = this.state;
        if (state.storage.isClosed()) {
            throw new IllegalStateException("DAO IS CLOSED!!!!");
        }

        MemorySegment fromTmp = from;
        if (fromTmp == null) {
            fromTmp = VERY_FIRST_KEY;
        }

        List<Iterator<Entry<MemorySegment>>> iterators = state.storage.iterate(fromTmp, to);
        iterators.add(getMemoryIterator(fromTmp, to));
        iterators.add(getFlushMemoryIterator(fromTmp, to));

        Iterator<Entry<MemorySegment>> mergeIterator = MergeIterator.of(iterators, EntryKeyComparator.INSTANCE);

        return new TombstoneFilteringIterator(mergeIterator);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        State state = this.state;
        if (state.storage.isClosed()) {
            throw new IllegalStateException("DAO IS CLOSED!!!!");
        }
        Entry<MemorySegment> result = state.memory.get(key); // to get the following entry
        if (result == null) {
            result = state.storage.get(key);
        }
        return (result == null || result.isTombstone()) ? null : result;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        State state = this.state;
        if (state.storage.isClosed()) {
            throw new IllegalStateException("DAO IS CLOSED!!!!");
        }
        boolean runFlush;
        upsetLock.readLock().lock();
        try {
            runFlush = state.memory.put(entry.key(), entry);
        } finally {
            upsetLock.readLock().unlock();
        }

        if (runFlush) {
            flushInBg();
        }
    }

    private void flushInBg() {
        State state = this.state;
        executor.execute(() -> {
            try {

                upsetLock.writeLock().lock();
                try {
                    this.state = state.prepareForFlush();
                } finally {
                    upsetLock.writeLock().unlock();
                }
                Storage.save(config, state.storage, state.flushing.values());
                Storage load = Storage.load(config);
                upsetLock.writeLock().lock();
                try {
                    this.state = state.afterFlush(load);
                } finally {
                    upsetLock.writeLock().unlock();
                }
            } catch (IOException e) {
                //TODO LOG
            }
        });
    }

    @Override
    public void flush() throws IOException {
        State state = this.state;
        if (state.storage.isClosed()) {
            throw new IllegalStateException("DAO IS CLOSED!!!!");
        }
        if (state.memory.isEmpty()) {
            return;
        }

        upsetLock.writeLock().lock();
        try {
            this.state = state.prepareForFlush();
        } finally {
            upsetLock.writeLock().unlock();
        }
        Storage.save(config, state.storage, state.flushing.values());
        Storage load = Storage.load(config);
        upsetLock.writeLock().lock();

        try {
            this.state = state.afterFlush(load);
        } finally {
            upsetLock.writeLock().unlock();
        }
        state.storage.close();
    }

    @Override
    public void compact() throws IOException {
        State state = this.state;
        if (state.memory.isEmpty() && state.storage.isCompacted()) {
            return;
        }
        Storage.compact(config, () -> MergeIterator.of(
                state.storage.iterate(VERY_FIRST_KEY, null),
                EntryKeyComparator.INSTANCE
        ));

        Storage storage = Storage.load(config);
        upsetLock.writeLock().lock();
        try {
            this.state = state.afterCompact(storage);
        } finally {
            upsetLock.writeLock().unlock();
        }
        state.storage.close();
    }

    @Override
    public synchronized void close() throws IOException {
        State state = this.state;
        if (state.storage.isClosed()) {
            return;
        }
        executor.shutdown();
        try {
            executor.awaitTermination(30, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            throw new IllegalStateException();
        }
        state.storage.close();
        if (state.memory.isEmpty()) {
            return;
        }
        Storage.save(config, state.storage, state.memory.values());
    }

    private Iterator<Entry<MemorySegment>> getMemoryIterator(MemorySegment from, MemorySegment to) {
        upsetLock.readLock().lock();
        try {
            return state.memory.get(from, to);
        } finally {
            upsetLock.readLock().unlock();
        }
    }

    private Iterator<Entry<MemorySegment>> getFlushMemoryIterator(MemorySegment from, MemorySegment to) {
        upsetLock.readLock().lock();
        try {
            return state.flushing.get(from, to);
        } finally {
            upsetLock.readLock().unlock();
        }
    }
}
