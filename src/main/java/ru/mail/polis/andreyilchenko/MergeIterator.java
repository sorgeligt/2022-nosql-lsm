package ru.mail.polis.andreyilchenko;

import java.util.*;

public class MergeIterator<E> implements Iterator<E> {

    private final PriorityQueue<IndexPeekIterator<E>> iterators;
    private final Comparator<E> comparator;

    private MergeIterator(PriorityQueue<IndexPeekIterator<E>> iterators, Comparator<E> comparator) {
        this.iterators = iterators;
        this.comparator = comparator;
    }

    // iterators are strictly ordered by comparator (previous element always < next element)
    public static <E> Iterator<E> of(List<IndexPeekIterator<E>> iterators, Comparator<E> comparator) {
        switch (iterators.size()) {
            case 0:
                return Collections.emptyIterator();
            case 1:
                return iterators.get(0);
            default:
                // Just go on
        }

        PriorityQueue<IndexPeekIterator<E>> queue = new PriorityQueue<>(iterators.size(), (o1, o2) -> {
            int result = comparator.compare(o1.peek(), o2.peek());
            if (result != 0) {
                return result;
            }
            return Integer.compare(o1.index(), o2.index());
        });

        for (IndexPeekIterator<E> iterator : iterators) {
            if (iterator.hasNext()) {
                queue.add(iterator);
            }
        }

        return new MergeIterator<>(queue, comparator);
    }

    @Override
    public boolean hasNext() {
        return !iterators.isEmpty();
    }

    @Override
    public E next() {
        IndexPeekIterator<E> iterator = iterators.remove();
        E next = iterator.next();

        while (!iterators.isEmpty()) {
            IndexPeekIterator<E> candidate = iterators.peek();
            if (comparator.compare(next, candidate.peek()) != 0) {
                break;
            }

            iterators.remove();
            candidate.next();
            if (candidate.hasNext()) {
                iterators.add(candidate);
            }
        }

        if (iterator.hasNext()) {
            iterators.add(iterator);
        }

        return next;
    }
}
