package ru.mail.polis.arturgaleev;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.Entry;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;

public class FileDBWriter implements Closeable {
    public static final String FILE_TMP = "file.tmp";
    public static final byte VALUE_FOR_HASH_NULL = (byte) -1;
    private final Path path;
    private final ResourceScope writeScope;

    public FileDBWriter(Path path) {
        this.writeScope = ResourceScope.newConfinedScope();
        this.path = path;
    }

    // first value is number of entries, second is byte size
    private static IteratorData getIteratorData(Iterator<Entry<MemorySegment>> iterator) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("System does not found SHA-256 algorithm", e);
        }
        long numberOfElements = 0;
        long byteSize = 0;
        while (iterator.hasNext()) {
            Entry<MemorySegment> entry = iterator.next();
            byteSize += getEntryLength(entry);
            updateHash(md, entry);
            numberOfElements++;
        }
        byteSize += Long.BYTES + numberOfElements * Long.BYTES;
        return new IteratorData(numberOfElements, byteSize, md.digest());
    }

    static void updateHash(MessageDigest md, Entry<MemorySegment> entry) {
        md.update(entry.key().asReadOnly().asByteBuffer());
        if (entry.value() == null) {
            md.update(VALUE_FOR_HASH_NULL);
        } else {
            md.update(entry.value().asReadOnly().asByteBuffer());
        }
    }

    private static long getEntryLength(Entry<MemorySegment> entry) {
        return entry.key().byteSize()
                + ((entry.value() == null) ? 0 : entry.value().byteSize()) + 2 * Long.BYTES;
    }

    private static long writeEntry(MemorySegment page, long posToWrite, Entry<MemorySegment> baseEntry) {
        long offset = 0;

        MemoryAccess.setLongAtOffset(page, posToWrite + offset, baseEntry.key().byteSize());
        offset += Long.BYTES;
        MemoryAccess.setLongAtOffset(page, posToWrite + offset,
                baseEntry.value() == null ? -1 : baseEntry.value().byteSize());
        offset += Long.BYTES;

        page.asSlice(posToWrite + offset, baseEntry.key().byteSize()).copyFrom(baseEntry.key());
        offset += baseEntry.key().byteSize();
        if (baseEntry.value() != null) {
            page.asSlice(posToWrite + offset, baseEntry.value().byteSize()).copyFrom(baseEntry.value());
            offset += baseEntry.value().byteSize();
        }
        return offset;
    }

    private static MemorySegment createTmpMemorySegmentPage(long mapByteSize,
                                                            Path tmpPath,
                                                            ResourceScope writeScope) throws IOException {
        Files.deleteIfExists(tmpPath);
        Files.createFile(tmpPath);
        return MemorySegment.mapFile(
                tmpPath,
                0,
                mapByteSize,
                FileChannel.MapMode.READ_WRITE,
                writeScope
        );
    }

    private static void writeIterable(
            MemorySegment page,
            long numberOfEntries,
            Iterator<Entry<MemorySegment>> iterator,
            byte[] sha256) {
        MemoryAccess.setLongAtOffset(page, 0, numberOfEntries);

        // offset for data
        long dataBeingOffset = Long.BYTES + (long) Long.BYTES * numberOfEntries;

        //offset for hash
        dataBeingOffset += Long.BYTES + sha256.length;

        long i = 0;
        long dataWriteOffset = 0;
        while (iterator.hasNext()) {
            Entry<MemorySegment> entry = iterator.next();
            MemoryAccess.setLongAtOffset(page, Long.BYTES + Long.BYTES * i++, dataWriteOffset);

            dataWriteOffset += writeEntry(page, dataBeingOffset + dataWriteOffset, entry);
        }

        MemoryAccess.setLongAtOffset(page, Long.BYTES + Long.BYTES * i++, sha256.length);
        page.asSlice(Long.BYTES + Long.BYTES * i, sha256.length).copyFrom(MemorySegment.ofArray(sha256));
    }

    public void writeIterable(
            Iterable<Entry<MemorySegment>> iterableCollection
    ) throws IOException {
        Iterator<Entry<MemorySegment>> iterator = iterableCollection.iterator();

        if (!iterator.hasNext()) {
            return;
        }
        IteratorData iteratorData = getIteratorData(iterator);

        iterator = iterableCollection.iterator();
        writeIteratorWithTempFile(iterator, iteratorData);
    }

    private void writeIteratorWithTempFile(Iterator<Entry<MemorySegment>> iterator,
                                           IteratorData iteratorData) throws IOException {
        byte[] sha256 = iteratorData.sha256();
        Path tmpPath = path.getParent().resolve(FILE_TMP);

        MemorySegment page = createTmpMemorySegmentPage(
                iteratorData.dataArraySize() + sha256.length + Long.BYTES,
                tmpPath, writeScope);

        writeIterable(page, iteratorData.numberOfEntries(), iterator, sha256);

        FileDBReader reader = new FileDBReader(page);
        if (reader.checkIfFileCorrupted()) {
            throw new FileSystemException("File with path: " + path + " has written incorrectly");
        }

        Files.move(tmpPath, path, StandardCopyOption.ATOMIC_MOVE);
    }

    @Override
    public void close() throws IOException {
        writeScope.close();
    }

    @SuppressWarnings("all")
    private record IteratorData(long numberOfEntries, long dataArraySize, byte[] sha256) {
    }
}
