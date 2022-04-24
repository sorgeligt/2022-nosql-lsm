package ru.mail.polis.andreyilchenko;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Config;
import ru.mail.polis.Entry;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class State {
    final Config config;
    final Memory memory;
    final Memory flushing;
    final Storage storage;

    public State(Config config, Memory memory, Memory flushing, Storage storage) {
        this.config = config;
        this.memory = memory;
        this.flushing = flushing;
        this.storage = storage;
    }

    static State newState(Config config, Storage storage) {
        return new State(config, new Memory(config.flushThresholdBytes()), Memory.EMPTY, storage);
    }

    public State prepareForFlush() {
        if (this.flushing != Memory.EMPTY) {
            throw new IllegalStateException();
        }
        return new State(config, new Memory(config.flushThresholdBytes()), memory, storage);
    }

    public State afterFlush(Storage storage) {
        if (this.flushing == Memory.EMPTY) {
            throw new IllegalStateException();
        }
        return new State(config, memory, Memory.EMPTY, storage);
    }

    public State afterCompact(Storage storage) {
        if (this.flushing != Memory.EMPTY) {
            throw new IllegalStateException();
        }
        return new State(config, memory, Memory.EMPTY, storage);
    }

}

class Memory {
    static final Memory EMPTY = new Memory(-1);

    private final AtomicLong size = new AtomicLong();

    private final ConcurrentSkipListMap<MemorySegment, ru.mail.polis.Entry<MemorySegment>> delegate =
            new ConcurrentSkipListMap<>(MemorySegmentComparator.INSTANCE);

    private final long sizeThreshold;

    private final AtomicBoolean oversized = new AtomicBoolean();

    public Memory(long sizeThreshold) {
        this.sizeThreshold = sizeThreshold;
    }

    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    public Collection<ru.mail.polis.Entry<MemorySegment>> values() {
        return delegate.values();
    }

    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return to == null
                ? delegate.tailMap(from).values().iterator()
                : delegate.subMap(from, to).values().iterator();
    }

    public boolean put(MemorySegment key, ru.mail.polis.Entry<MemorySegment> entry) {
        if (sizeThreshold == -1) {
            throw new UnsupportedOperationException("read only");
        }
        ru.mail.polis.Entry<MemorySegment> segmentEntry = delegate.put(key, entry);
        long sizeData = Storage.getSizeOnDisk(entry);
        if (segmentEntry != null) {
            sizeData -= Storage.getSizeOnDisk(segmentEntry);
        }
        long newSize = size.addAndGet(sizeData);
        if (newSize > sizeThreshold) {
            return !oversized.getAndSet(true);
        }
        return false;
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        return delegate.get(key);
    }
}