package ru.mail.polis.levsaskov;

import ru.mail.polis.BaseEntry;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class StorageSystemIterator implements Iterator<BaseEntry<ByteBuffer>> {
    private final BinaryHeap binaryHeap;
    private BaseEntry<ByteBuffer> next;

    public StorageSystemIterator(BinaryHeap binaryHeap) {
        this.binaryHeap = binaryHeap;
    }

    @Override
    public boolean hasNext() {
        if (next == null) {
            next = getNext();
        }

        return next != null;
    }

    @Override
    public BaseEntry<ByteBuffer> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        BaseEntry<ByteBuffer> ans = next;
        next = null;

        return ans;
    }

    private BaseEntry<ByteBuffer> getNext() {
        BaseEntry<ByteBuffer> potentialNext = null;
        while (binaryHeap.getSize() > 0) {
            potentialNext = tryToGetNext();

            if (potentialNext != null) {
                break;
            }
        }

        return potentialNext;
    }

    private BaseEntry<ByteBuffer> tryToGetNext() {
        PeekIterator freshIterator = binaryHeap.popMin();
        BaseEntry<ByteBuffer> freshNext = freshIterator.next();

        while (binaryHeap.getSize() > 0 && freshNext.key().equals(binaryHeap.getMin().peek().key())) {
            PeekIterator dublicateIt = binaryHeap.popMin();
            BaseEntry<ByteBuffer> dublicateNext = dublicateIt.next();
            if (dublicateIt.getStoragePartN() > freshIterator.getStoragePartN()) {
                PeekIterator temp = freshIterator;
                freshIterator = dublicateIt;
                dublicateIt = temp;

                freshNext = dublicateNext;
            }

            if (dublicateIt.peek() != null) {
                binaryHeap.add(dublicateIt);
            }

        }

        if (freshIterator.peek() != null) {
            binaryHeap.add(freshIterator);
        }

        if (freshNext.value() != null) {
            return freshNext;
        }

        return null;
    }
}
