package ru.mail.polis.andreyilchenko;

import jdk.incubator.foreign.MemorySegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final MemorySegment VERY_FIRST_KEY = MemorySegment.ofArray(new byte[]{});

    private final Logger logger = LoggerFactory.getLogger(MemorySegmentDao.class);

    private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r, "MemorySegmentDao");
        thread.setUncaughtExceptionHandler((t, e) -> {
            logger.error("Uncaught Exception occurred on thread: " + t.getName());
            logger.error("Exception message: " + e.getMessage());
        });
        return thread;
    });

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Config config;
    private volatile DaoState state;

    public MemorySegmentDao(Config config) throws IOException {
        this.config = config;
        this.state = DaoState.newState(config, Storage.load(config));
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        DaoState state = getDaoState();

        MemorySegment fromTmp = from;
        if (fromTmp == null) {
            fromTmp = VERY_FIRST_KEY;
        }

        List<Iterator<Entry<MemorySegment>>> iterators = state.storage.iterate(fromTmp, to);
        iterators.add(getMemoryIterator(fromTmp, to));
        iterators.add(getFlushMemoryIterator(fromTmp, to));

        return new TombstoneFilteringIterator(MergeIterator.of(iterators, EntryKeyComparator.INSTANCE));
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        DaoState state = getDaoState();
        Entry<MemorySegment> result = state.memory.get(key);
        if (result == null) {
            result = state.flushing.get(key);
            if (result == null) {
                result = state.storage.get(key);
            }
        }
        if (result == null) {
            return null;
        }
        return result.isTombstone() ? null : result;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        DaoState state = getDaoState();
        boolean flushFlag;
        lock.writeLock().lock();
        try {
            flushFlag = state.memory.put(entry.key(), entry);
        } finally {
            lock.writeLock().unlock();
        }
        if (flushFlag) {
            flushInBg();
        }
    }

    @Override
    public void flush() throws IOException {
        DaoState state = getDaoState();
        lock.writeLock().lock();
        try {
            if (state.memory.isEmpty()) {
                return;
            }
            this.state = new DaoState(
                    state.config, new MemoryWrapper(state.config.flushThresholdBytes()), state.memory, state.storage
            );
        } finally {
            lock.writeLock().unlock();
        }
        Storage.save(config, state.storage, state.flushing.values());
        newDaoStateWithLoadStorage(state);
    }

    @Override
    public void compact() throws IOException {
        DaoState state = this.state;
        if (state.memory.isEmpty() && state.storage.isCompacted()) {
            return;
        }
        Storage.compact(config, () -> MergeIterator.of(
                state.storage.iterate(VERY_FIRST_KEY, null),
                EntryKeyComparator.INSTANCE
        ));

        newDaoStateWithLoadStorage(state);
    }

    private void newDaoStateWithLoadStorage(DaoState state) throws IOException {
        Storage loadStorage = Storage.load(config);
        lock.writeLock().lock();
        try {
            this.state = new DaoState(state.config, state.memory, MemoryWrapper.EMPTY, loadStorage);
        } finally {
            lock.writeLock().unlock();
        }
        state.storage.close();
    }

    private DaoState getDaoState() {
        DaoState state = this.state;
        if (state.storage.isClosed()) {
            throw new IllegalStateException("dao is closed");
        }
        return state;
    }

    @Override
    public synchronized void close() throws IOException {
        DaoState state = this.state;
        if (state.storage.isClosed()) {
            return;
        }
        state.storage.close();
        try {
            executor.shutdown();
            boolean termination = executor.awaitTermination(24, TimeUnit.HOURS);
            if (!termination) {
                throw new TimeoutException();
            }
        } catch (InterruptedException e) {
            logger.error("close method InterruptedException");
            Thread.currentThread().interrupt();
        } catch (TimeoutException e) {
            throw new IOException(e);
        }
        if (state.memory.isEmpty()) {
            return;
        }
        Storage.save(config, state.storage, state.memory.values());
    }

    private void flushInBg() {
        DaoState state = this.state;
        executor.execute(() -> {
            lock.writeLock().lock();
            try {
                this.state = new DaoState(
                        state.config,
                        new MemoryWrapper(state.config.flushThresholdBytes()),
                        state.memory,
                        state.storage
                );
                Storage.save(config, state.storage, state.flushing.values());
                Storage loadStorage = Storage.load(config);
                this.state = new DaoState(state.config, state.memory, MemoryWrapper.EMPTY, loadStorage);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                lock.writeLock().unlock();
            }
        });
    }

    private Iterator<Entry<MemorySegment>> getMemoryIterator(MemorySegment from, MemorySegment to) {
        lock.readLock().lock();
        try {
            return state.memory.get(from, to);
        } finally {
            lock.readLock().unlock();
        }
    }

    private Iterator<Entry<MemorySegment>> getFlushMemoryIterator(MemorySegment from, MemorySegment to) {
        lock.readLock().lock();
        try {
            return state.flushing.get(from, to);
        } finally {
            lock.readLock().unlock();
        }
    }

    record DaoState(Config config, MemoryWrapper memory, MemoryWrapper flushing, Storage storage) {
        static DaoState newState(Config config, Storage storage) {
            return new DaoState(config, new MemoryWrapper(config.flushThresholdBytes()), MemoryWrapper.EMPTY, storage);
        }
    }
}
