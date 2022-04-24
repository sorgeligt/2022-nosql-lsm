package ru.mail.polis.andreyilchenko;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Entry;

import java.util.Iterator;
import java.util.NoSuchElementException;

class TombstoneFilteringIterator implements Iterator<Entry<MemorySegment>> {
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
        Entry<MemorySegment> entry;
        while (iterator.hasNext()) {
            entry = iterator.next();
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