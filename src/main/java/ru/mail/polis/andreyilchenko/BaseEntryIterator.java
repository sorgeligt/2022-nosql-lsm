package ru.mail.polis.andreyilchenko;

import ru.mail.polis.BaseEntry;

import java.util.Iterator;
import java.util.Map;

class BaseEntryIterator<T> implements Iterator<BaseEntry<T>> {
    private final Iterator<Map.Entry<T, T>> iterator;

    public BaseEntryIterator(Iterator<Map.Entry<T, T>> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public BaseEntry<T> next() {
        Map.Entry<T, T> next = iterator.next();
        return new BaseEntry<>(next.getKey(), next.getValue());
    }
}