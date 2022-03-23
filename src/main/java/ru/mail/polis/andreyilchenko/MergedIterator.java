package ru.mail.polis.andreyilchenko;

import ru.mail.polis.BaseEntry;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;

public class MergedIterator implements Iterator<BaseEntry<ByteBuffer>> {
    private final Queue<PriorityIterator> queue = new PriorityQueue<>((x, y) -> {
        if (!x.hasNext() || !y.hasNext()) {
            return y.hasNext() ? 1 : -1;
        }
        int compareKeyResult = x.peek().key().compareTo(y.peek().key());
        return compareKeyResult == 0 ? Integer.compare(x.getPriority(), y.getPriority()) : compareKeyResult;
    });

    public MergedIterator(List<PriorityIterator> iteratorList) {
        queue.addAll(iteratorList);
    }

    @Override
    public boolean hasNext() {
        clearQueue(queue.iterator());
        while (!queue.isEmpty() && doublePeekQueue().value() == null) {
            updatePeekIterator(Objects.requireNonNull(queue.poll()));
        }
        return !queue.isEmpty();
    }

    @Override
    public BaseEntry<ByteBuffer> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        BaseEntry<ByteBuffer> nextElem = queue.isEmpty()
                ? queue.peek().next() : updatePeekIterator(queue.poll());
        return nextElem.value() == null ? null : nextElem;
    }

    private BaseEntry<ByteBuffer> updatePeekIterator(PriorityIterator nextIter) {
        BaseEntry<ByteBuffer> nextEntry = nextIter.next();
        addInQueueIfHasNext(nextIter);
        while (!queue.isEmpty() && nextEntry.key().equals(doublePeekQueue().key())) {
            PriorityIterator peek = queue.poll();
            peek.next();
            addInQueueIfHasNext(peek);
        }
        return nextEntry;
    }

    private void addInQueueIfHasNext(PriorityIterator peek) {
        if (peek.hasNext()) {
            queue.add(peek);
        }
    }

    private void clearQueue(Iterator<PriorityIterator> queueIter) {
        while (queueIter.hasNext()) {
            if (!queueIter.next().hasNext()) {
                queueIter.remove();
            }
        }
    }

    private BaseEntry<ByteBuffer> doublePeekQueue() {
        return queue.peek().peek();
    }
}


