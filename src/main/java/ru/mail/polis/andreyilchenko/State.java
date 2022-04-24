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
