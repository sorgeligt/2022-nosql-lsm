package ru.mail.polis.andreyilchenko;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private static final MemorySegment VERY_FIRST_KEY = MemorySegment.ofArray(new byte[]{});

    private ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memory =
            new ConcurrentSkipListMap<>(MemorySegmentComparator.INSTANCE);
    private ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> flushMemory =
            new ConcurrentSkipListMap<>(MemorySegmentComparator.INSTANCE);

    private final AtomicBoolean isClosed = new AtomicBoolean();
    private final AtomicLong memorySpending = new AtomicLong(0);

    private final ExecutorService flushExecutorService = Executors.newSingleThreadExecutor();
    private final ExecutorService compactExecutorService = Executors.newSingleThreadExecutor();

    private Storage storage;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Config config;

    public MemorySegmentDao(Config config) throws IOException {
        this.config = config;
        this.storage = Storage.load(config);
        isClosed.set(false);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (isClosed.get()) {
            throw new IllegalStateException("DAO IS CLOSED!!!!");
        }
        lock.readLock().lock();
        try {
            MemorySegment fromTmp = from;
            if (fromTmp == null) {
                fromTmp = VERY_FIRST_KEY;
            }

            List<Iterator<Entry<MemorySegment>>> iterators = storage.iterate(fromTmp, to);
            iterators.add(getMemoryIterator(fromTmp, to));
            iterators.add(getFlushMemoryIterator(fromTmp, to));

            Iterator<Entry<MemorySegment>> mergeIterator =
                    MergeIterator.of(iterators, EntryKeyComparator.INSTANCE);

            return new TombstoneFilteringIterator(mergeIterator);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        if (isClosed.get()) {
            throw new IllegalStateException("DAO IS CLOSED!!!");
        }
        lock.readLock().lock();
        try {
            Iterator<Entry<MemorySegment>> resultIterator = get(key, null); // to get the following entry
            if (!resultIterator.hasNext()) {
                return null;
            }
            Entry<MemorySegment> entry = resultIterator.next();
            return MemorySegmentComparator.INSTANCE.compare(entry.key(), key) == 0 ? entry : null;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (isClosed.get()) {
            throw new IllegalStateException("DAO IS CLOSED!!");
        }
        lock.readLock().lock();
        try {
            if (memorySpending.addAndGet(sizeEntry(entry)) >= config.flushThresholdBytes()) {
                memory = new ConcurrentSkipListMap<>(MemorySegmentComparator.INSTANCE);
                long pervMemorySpending = memorySpending.getAndSet(sizeEntry(entry));
                flushExecutorService.execute(this::serviceFlush);
                /*} catch (UncheckedIOException e) {
                    memorySpending.set(pervMemorySpending); ?
                }*/
            }
            memory.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        if (isClosed.get()) {
            throw new IllegalStateException("DAO IS CLOSED!");
        }
        lock.writeLock().lock();
        try {
            if (storage.isClosed() || memory.isEmpty()) {
                return;
            }
            storage.close();
            Storage.save(config, storage, memory.values());
            memory = new ConcurrentSkipListMap<>(MemorySegmentComparator.INSTANCE);
            storage = Storage.load(config);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void compact() throws IOException {
        compactExecutorService.execute(() -> {
                    synchronized (MemorySegmentDao.this) {
                        if (memory.isEmpty() && storage.isCompacted()) {
                            return;
                        }
                        try {
                            List<Iterator<Entry<MemorySegment>>> iterators = storage.iterate(null, null);
                            iterators.add(getFlushMemoryIterator(null, null));
                            Iterator<Entry<MemorySegment>> mergeIterator = MergeIterator.of(iterators, EntryKeyComparator.INSTANCE);
                            Storage.compact(config, () -> mergeIterator);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                }
        );
    }

    @Override
    public synchronized void close() throws IOException {
        lock.writeLock().lock();
        try {
            if (storage.isClosed() || isClosed.get()) {
                return;
            }
            storage.close();
            if (memory.isEmpty()) {
                return;
            }
            flushExecutorService.shutdown();
            compactExecutorService.shutdown();
            Storage.save(config, storage, memory.values());
            isClosed.set(true);
        } finally {
            lock.writeLock().unlock();
        }
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

    private Iterator<Entry<MemorySegment>> getFlushMemoryIterator(MemorySegment from, MemorySegment to) {
        lock.readLock().lock();
        try {
            if (to == null) {
                return flushMemory.tailMap(from).values().iterator();
            }
            return flushMemory.subMap(from, to).values().iterator();
        } finally {
            lock.readLock().unlock();
        }
    }

    private long sizeEntry(Entry<MemorySegment> entry) {
        return Long.BYTES + entry.key().byteSize() + Long.BYTES + (
                entry.value() == null
                        ? 0 : entry.key().byteSize()
        );
    }

    private void serviceFlush() {
        lock.writeLock().lock();
        try {
            if (storage.isClosed() || memory.isEmpty()) {
                return;
            }
            storage.close();
            Storage.save(config, storage, flushMemory.values());
            flushMemory = new ConcurrentSkipListMap<>(MemorySegmentComparator.INSTANCE);
            storage = Storage.load(config);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static class TombstoneFilteringIterator implements Iterator<Entry<MemorySegment>> {
        private final Iterator<Entry<MemorySegment>> iterator;
        private Entry<MemorySegment> current;

        public TombstoneFilteringIterator(Iterator<Entry<MemorySegment>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            if (current != null) {
                return true;
            }

            while (iterator.hasNext()) {
                Entry<MemorySegment> entry = iterator.next();
                if (!entry.isTombstone()) {
                    this.current = entry;
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
    }
}
