package ru.mail.polis.dmitrykondraev;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import static ru.mail.polis.dmitrykondraev.Files.createFileIfNotExists;

final class SortedStringTable {
    public static final String INDEX_FILENAME = "index";
    public static final String DATA_FILENAME = "data";

    private final MemorySegment dataSegment;
    private final Index index;

    private SortedStringTable(MemorySegment dataSegment, Index index) {
        this.dataSegment = dataSegment;
        this.index = index;
    }

    /**
     * Constructs SortedStringTable.
     */
    public static SortedStringTable of(Path folderPath, ResourceScope scope) throws IOException {
        Path indexFile = folderPath.resolve(INDEX_FILENAME);
        Path dataFile = folderPath.resolve(DATA_FILENAME);
        Index index = new Index(MemorySegment.mapFile(
                indexFile,
                0L,
                Files.size(indexFile),
                FileChannel.MapMode.READ_ONLY,
                scope
        ));
        return new SortedStringTable(
                MemorySegment.mapFile(
                        dataFile,
                        0L,
                        index.dataSize(),
                        FileChannel.MapMode.READ_ONLY,
                        scope
                ),
                index);
    }

    public static void destroyFiles(Path folderPath) throws IOException, NoSuchFileException {
        Files.deleteIfExists(folderPath.resolve(DATA_FILENAME));
        Files.deleteIfExists(folderPath.resolve(INDEX_FILENAME));
        Files.delete(folderPath);
    }

    public static SortedStringTable written(
            Path folderPath,
            Collection<MemorySegmentEntry> entries,
            ResourceScope scope
    ) throws IOException {
        Path indexFile = folderPath.resolve(INDEX_FILENAME);
        Path dataFile = folderPath.resolve(DATA_FILENAME);
        Index index = Index.written(entries, indexFile, scope);
        MemorySegment dataSegment = MemorySegment.mapFile(
                createFileIfNotExists(dataFile),
                0L,
                index.dataSize(),
                FileChannel.MapMode.READ_WRITE,
                scope
        );
        int i = 0;
        for (MemorySegmentEntry entry : entries) {
            entry.copyTo(index.mappedEntrySegment(dataSegment, i));
            i++;
        }
        dataSegment.force();
        return new SortedStringTable(
                dataSegment.asReadOnly(),
                index
        );
    }

    public static SortedStringTable written(
            Path folderPath,
            Iterator<MemorySegmentEntry> iterator,
            ResourceScope scope
    ) throws IOException {
        // TODO проблема с загрузкой в память!!
        ArrayList<MemorySegmentEntry> entries = new ArrayList<>();
        while (iterator.hasNext()) {
            entries.add(iterator.next());
        }
        return written(folderPath, entries, scope);
    }

    /**
     * left binary search.
     *
     * @param first inclusive
     * @param last  exclusive
     * @return first index such that key of entry with that index is equal to key,
     *         if no such index exists, result < 0, in that case use
     *        {@link SortedStringTable#insertionPoint(int)} to recover insertion point
     */
    private int binarySearch(int first, int last, MemorySegment key) {
        int low = first;
        int high = last;
        while (low < high) {
            int mid = low + (high - low) / 2;
            int compare = MemorySegmentComparator.INSTANCE.compare(mappedEntry(mid).key(), key);
            if (compare < 0) {
                low = mid + 1;
            } else if (compare > 0) {
                high = mid;
            } else {
                return mid;
            }
        }
        return ~low;
    }

    private static int insertionPoint(int index) {
        if (index < 0) {
            return ~index;
        }
        return index;
    }

    public Iterator<MemorySegmentEntry> get(MemorySegment from, MemorySegment to) {
        int tableSize = index.entriesMapped();
        return new Iterator<>() {
            private int first = insertionPoint(binarySearch(0, tableSize, from));
            private final int last = to == null ? tableSize : insertionPoint(binarySearch(first, tableSize, to));

            @Override
            public boolean hasNext() {
                return first < last;
            }

            @Override
            public MemorySegmentEntry next() {
                return mappedEntry(first++);
            }
        };
    }

    /**
     * Get single entry.
     *
     * @return null if either indexFile or dataFile does not exist,
     *         null if key does not exist in table
     * @throws IOException if other I/O error occurs
     */
    public MemorySegmentEntry get(MemorySegment key) throws IOException {
        int size = index.entriesMapped();
        int entryIndex = binarySearch(0, size, key);
        return entryIndex < 0 ? null : mappedEntry(entryIndex);
    }

    private MemorySegmentEntry mappedEntry(long i) {
        return MemorySegmentEntry.of(index.mappedEntrySegment(dataSegment, i));
    }

    private static class Index {
        final MemorySegment indexSegment;

        private Index(MemorySegment indexSegment) {
            this.indexSegment = indexSegment;
        }

        /**
         * write offsets in format:
         * ┌─────────┬─────────────────┐
         * │size: int│array: long[size]│
         * └─────────┴─────────────────┘
         * where size is number of entries and
         * array represents offsets of entries in data file specified by methods
         * keyOffset, valueOffset, keySize and valueSize.
         */
        static Index written(
                Collection<MemorySegmentEntry> entries,
                Path indexFile,
                ResourceScope scope
        ) throws IOException {
            MemorySegment indexSegment = MemorySegment.mapFile(
                    createFileIfNotExists(indexFile),
                    0L,
                    Integer.BYTES + (1L + entries.size()) * Long.BYTES,
                    FileChannel.MapMode.READ_WRITE,
                    scope
            );
            MemoryAccess.setInt(indexSegment, entries.size());
            MemorySegment offsetsSegment = indexSegment.asSlice(Integer.BYTES);
            long currentOffset = 0L;
            long index = 0L;
            MemoryAccess.setLongAtIndex(offsetsSegment, index++, currentOffset);
            for (MemorySegmentEntry entry : entries) {
                currentOffset += entry.bytesSize();
                MemoryAccess.setLongAtIndex(offsetsSegment, index++, currentOffset);
            }
            indexSegment.force();
            return new Index(indexSegment.asReadOnly());
        }

        long entryOffset(long i) {
            return MemoryAccess.getLongAtOffset(indexSegment, Integer.BYTES + i * Long.BYTES);
        }

        long entrySize(long i) {
            return entryOffset(i + 1) - entryOffset(i);
        }

        int entriesMapped() {
            return MemoryAccess.getIntAtOffset(indexSegment, 0L);
        }

        long dataSize() {
            return entryOffset(entriesMapped());
        }

        MemorySegment mappedEntrySegment(MemorySegment dataSegment, long i) {
            return dataSegment.asSlice(entryOffset(i), entrySize(i));
        }
    }
}
