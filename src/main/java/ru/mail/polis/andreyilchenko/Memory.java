package ru.mail.polis.andreyilchenko;


import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Entry;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

class Memory {
    static final Memory EMPTY = new Memory(-1);

    private final AtomicLong size = new AtomicLong();

    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> delegate =
            new ConcurrentSkipListMap<>(MemorySegmentComparator.INSTANCE);

    private final long sizeThreshold;

    private final AtomicBoolean oversized = new AtomicBoolean();

    public Memory(long sizeThreshold) {
        this.sizeThreshold = sizeThreshold;
    }

    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    public Collection<Entry<MemorySegment>> values() {
        return delegate.values();
    }

    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return to == null
                ? delegate.tailMap(from).values().iterator()
                : delegate.subMap(from, to).values().iterator();
    }

    public boolean put(MemorySegment key, Entry<MemorySegment> entry) {
        Entry<MemorySegment> segmentEntry = delegate.put(key, entry);
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