package ru.mail.polis.alexanderkosnitskiy;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static ru.mail.polis.alexanderkosnitskiy.DaoUtility.getFiles;
import static ru.mail.polis.alexanderkosnitskiy.DaoUtility.mapFile;
import static ru.mail.polis.alexanderkosnitskiy.DaoUtility.safelyReplaceUnifiedFile;

public class PersistenceDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    public static final String FILE = "data";
    public static final String INDEX = "index";
    public static final String SAFE_EXTENSION = ".anime";
    public static final String IN_PROGRESS_EXTENSION = ".animerr";
    public static final String COMPOSITE_EXTENSION = ".ancord";
    private final Config config;
    private final ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> memory = new ConcurrentSkipListMap<>();
    private long amountOfFiles;
    private final List<FilePack> readers;

    public PersistenceDao(Config config) throws IOException {
        this.config = config;
        readers = getFiles(config);
        amountOfFiles = readers.size();
    }

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) throws IOException {
        BaseEntry<ByteBuffer> result = memory.get(key);
        if (result != null) {
            if (result.value() == null) {
                return null;
            }
            return result;
        }
        return findInFiles(key);
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        return new MergeIterator(from, to);
    }

    private Iterator<BaseEntry<ByteBuffer>> getMemory(ByteBuffer from, ByteBuffer to) {
        if (from == null && to == null) {
            return memory.values().iterator();
        }
        if (from == null) {
            return memory.headMap(to, false).values().iterator();
        }
        if (to == null) {
            return memory.tailMap(from, true).values().iterator();
        }
        return memory.subMap(from, true, to, false).values().iterator();
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        memory.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        store();
        memory.clear();
    }

    @Override
    public void compact() throws IOException {
        if (amountOfFiles == 0) {
            if (memory.isEmpty()) {
                return;
            }
            flush();
            return;
        }
        int count = 0;
        int size = 0;
        Iterator<BaseEntry<ByteBuffer>> iter = get(null, null);
        while (iter.hasNext()) {
            BaseEntry<ByteBuffer> entry = iter.next();
            count++;
            size += 2 * Integer.BYTES + entry.key().capacity() + entry.value().capacity();
        }
        try (DaoWriter out = new DaoWriter(config.basePath().resolve(FILE + IN_PROGRESS_EXTENSION),
                config.basePath().resolve(INDEX + IN_PROGRESS_EXTENSION))) {
            out.writeIterator(get(null, null), count, size);
        }

        safelyReplaceUnifiedFile(memory, config, amountOfFiles,
                INDEX + IN_PROGRESS_EXTENSION,
                FILE + IN_PROGRESS_EXTENSION);
        amountOfFiles = 1;

    }

    private void store() throws IOException {
        if (memory.isEmpty()) {
            return;
        }
        try (DaoWriter out = new DaoWriter(config.basePath().resolve(FILE + amountOfFiles + SAFE_EXTENSION),
                config.basePath().resolve(INDEX + amountOfFiles + SAFE_EXTENSION))) {
            out.writeMap(memory);
            readers.add(0, mapFile(config.basePath().resolve(FILE + amountOfFiles + SAFE_EXTENSION),
                    config.basePath().resolve(INDEX + amountOfFiles + SAFE_EXTENSION)));
            amountOfFiles++;
        }
    }

    private BaseEntry<ByteBuffer> findInFiles(ByteBuffer key) {
        BaseEntry<ByteBuffer> result;
        for (FilePack pack : readers) {
            result = pack.getReader().binarySearch(key);
            if (result != null) {
                if (result.value() == null) {
                    return null;
                }
                return result;
            }
        }
        return null;
    }

    private class MergeIterator implements Iterator<BaseEntry<ByteBuffer>> {
        private final PriorityQueue<PriorityConstruction> queue;
        private BaseEntry<ByteBuffer> nextElement;

        public MergeIterator(ByteBuffer from, ByteBuffer to) {
            List<Iterator<BaseEntry<ByteBuffer>>> iterators = new ArrayList<>();
            iterators.add(getMemory(from, to));
            for (FilePack pack : readers) {
                iterators.add(new FileIterator(pack.getReader(), from, to));
            }

            queue = new PriorityQueue<>((l, r) -> {
                int comparison = l.curEntry.key().compareTo(r.curEntry.key());
                if (comparison > 0) {
                    return 1;
                } else if (comparison < 0) {
                    return -1;
                }
                return Integer.compare(l.index, r.index);
            });
            int priority = 0;
            for (Iterator<BaseEntry<ByteBuffer>> iter : iterators) {
                if (iter.hasNext()) {
                    queue.add(new PriorityConstruction(priority++, iter));
                }
            }
            nextElement = getNextElement();
        }

        @Override
        public boolean hasNext() {
            return nextElement != null;
        }

        @Override
        public BaseEntry<ByteBuffer> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            BaseEntry<ByteBuffer> temp = nextElement;
            nextElement = getNextElement();
            return temp;
        }

        private BaseEntry<ByteBuffer> getNextEntry() {
            PriorityConstruction construction = queue.remove();
            BaseEntry<ByteBuffer> temp = construction.curEntry;
            if (construction.iterator.hasNext()) {
                construction.curEntry = construction.iterator.next();
                queue.add(construction);
            }
            return temp;
        }

        private BaseEntry<ByteBuffer> getNextElement() {
            if (queue.isEmpty()) {
                return null;
            }
            BaseEntry<ByteBuffer> curr = getNextEntry();
            while (!queue.isEmpty() && queue.peek().curEntry.key().equals(curr.key())) {
                getNextEntry();
            }
            while (curr.value() == null) {
                if (queue.isEmpty()) {
                    return null;
                }
                curr = getNextEntry();
                while (!queue.isEmpty() && queue.peek().curEntry.key().equals(curr.key())) {
                    getNextEntry();
                }
            }
            return curr;
        }

        private static class PriorityConstruction {
            int index;
            Iterator<BaseEntry<ByteBuffer>> iterator;
            BaseEntry<ByteBuffer> curEntry;

            PriorityConstruction(int index, Iterator<BaseEntry<ByteBuffer>> iterator) {
                this.index = index;
                this.iterator = iterator;
                curEntry = iterator.next();
            }
        }
    }

    static class FilePack {
        private final MappedByteBuffer valueFile;
        private final MappedByteBuffer indexFile;

        public FilePack(MappedByteBuffer valueFile, MappedByteBuffer indexFile) {
            this.valueFile = valueFile;
            this.indexFile = indexFile;
        }

        public DaoReader getReader() {
            return new DaoReader(valueFile, indexFile);
        }
    }
}
