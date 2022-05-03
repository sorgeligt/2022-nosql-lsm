package ru.mail.polis.andreyilchenko;


import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Entry;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

class MemoryWrapper {
    static final MemoryWrapper EMPTY = new MemoryWrapper(-1);
    private final AtomicLong currentSize = new AtomicLong();
    private final AtomicBoolean memoryLimitFlag = new AtomicBoolean();

    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> map =
            new ConcurrentSkipListMap<>(MemorySegmentComparator.INSTANCE);

    private final long sizeThreshold;

    public MemoryWrapper(long sizeThreshold) {
        this.sizeThreshold = sizeThreshold;
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public Collection<Entry<MemorySegment>> values() {
        return map.values();
    }

    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (to == null) {
            return map.tailMap(from).values().iterator();
        } else {
            return map.subMap(from, to).values().iterator();
        }
    }

    public boolean put(MemorySegment key, Entry<MemorySegment> entry) {
        long sizeData = Storage.getSize(entry);
        // If we had an entry, we subtract the size of the past data
        long subtractData = putAndReturnSubtractIfContains(key, entry);
        sizeData -= subtractData;
        if (currentSize.addAndGet(sizeData) > sizeThreshold) {
            return !memoryLimitFlag.getAndSet(true);
        }
        return false;
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        return map.get(key);
    }

    private long putAndReturnSubtractIfContains(MemorySegment key, Entry<MemorySegment> entry) {
        Entry<MemorySegment> segmentEntry = map.put(key, entry);
        if (segmentEntry != null) {
            return Storage.getSize(segmentEntry);
        }
        return 0;
    }
}