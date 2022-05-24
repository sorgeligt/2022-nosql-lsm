package ru.mail.polis.alexanderkosnitskiy;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.stream.Stream;

public final class DaoUtility {
    public static final String FILE = PersistenceDao.FILE;
    public static final String INDEX = PersistenceDao.INDEX;
    public static final String SAFE_EXTENSION = PersistenceDao.SAFE_EXTENSION;
    public static final String IN_PROGRESS_EXTENSION = PersistenceDao.IN_PROGRESS_EXTENSION;
    public static final String COMPOSITE_EXTENSION = PersistenceDao.COMPOSITE_EXTENSION;

    private DaoUtility() {

    }

    public static void renameFile(Config config, String fileName, String newFileName) throws IOException {
        Path source = config.basePath().resolve(fileName);
        Files.move(source, source.resolveSibling(newFileName));
    }

    public static PersistenceDao.FilePack mapFile(Path fileName, Path indexName) throws IOException {
        try (FileChannel reader = FileChannel.open(fileName, StandardOpenOption.READ);
             FileChannel indexReader = FileChannel.open(indexName, StandardOpenOption.READ)) {
            return new PersistenceDao.FilePack(reader.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(fileName)),
                    indexReader.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(indexName)));
        }
    }

    public static List<PersistenceDao.FilePack> getFiles(Config config) throws IOException {
        long numberOfFiles;
        try (Stream<Path> files = Files.list(config.basePath())) {
            if (files == null) {
                numberOfFiles = 0;
            } else {
                if (Files.exists(config.basePath().resolve(INDEX + IN_PROGRESS_EXTENSION))) {
                    Files.deleteIfExists(config.basePath().resolve(FILE + COMPOSITE_EXTENSION));
                    Files.deleteIfExists(config.basePath().resolve(FILE + IN_PROGRESS_EXTENSION));
                    Files.deleteIfExists(config.basePath().resolve(INDEX + IN_PROGRESS_EXTENSION));
                }
                numberOfFiles = safelyCountDaoFiles(files.toList(), config) / 2;
            }
        } catch (NoSuchFileException e) {
            numberOfFiles = 0;
        }
        List<PersistenceDao.FilePack> readers = new ArrayList<>();
        for (long i = numberOfFiles - 1; i >= 0; i--) {
            readers.add(mapFile(config.basePath().resolve(FILE + i + SAFE_EXTENSION),
                    config.basePath().resolve(INDEX + i + SAFE_EXTENSION)));
        }
        return readers;
    }

    private static long safelyCountDaoFiles(List<Path> paths, Config config) throws IOException {
        long numberOfFiles = 0;
        for (Path path : paths) {
            if (path.toString().endsWith(INDEX + COMPOSITE_EXTENSION)) {
                deleteFiles(config, paths.size() / 2);
                if (!Files.exists(config.basePath().resolve(FILE + COMPOSITE_EXTENSION))) {
                    throw new IOException("The content file was missing, when expected to be present");
                }
                renameFile(config, FILE + COMPOSITE_EXTENSION, FILE + 0 + SAFE_EXTENSION);
                renameFile(config, INDEX + COMPOSITE_EXTENSION, INDEX + 0 + SAFE_EXTENSION);
                numberOfFiles = 2;
                break;
            } else if (path.toString().endsWith(SAFE_EXTENSION)) {
                ++numberOfFiles;
            }
        }
        return numberOfFiles;
    }

    public static void safelyReplaceUnifiedFile(ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> memory,
                                                 Config config, long amountOfFiles,
                                                 String indexFileName,
                                                 String dataFileName) throws IOException {
        renameFile(config, dataFileName, FILE + COMPOSITE_EXTENSION);
        renameFile(config, indexFileName, INDEX + COMPOSITE_EXTENSION);
        memory.clear();
        deleteFiles(config, amountOfFiles);
        renameFile(config,FILE + COMPOSITE_EXTENSION, FILE + 0 + SAFE_EXTENSION);
        renameFile(config,INDEX + COMPOSITE_EXTENSION, INDEX + 0 + SAFE_EXTENSION);
    }

    public static void deleteFiles(Config config, long amountOfFiles) throws IOException {
        for (long i = amountOfFiles - 1; i >= 0; i--) {
            Files.deleteIfExists(config.basePath().resolve(FILE + i + SAFE_EXTENSION));
            Files.deleteIfExists(config.basePath().resolve(INDEX + i + SAFE_EXTENSION));
        }
    }
}
