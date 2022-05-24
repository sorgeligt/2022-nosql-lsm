package ru.mail.polis.lutsenkodmitrii;

import ru.mail.polis.BaseEntry;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static java.nio.charset.StandardCharsets.UTF_8;

public class MergeIterator implements Iterator<BaseEntry<String>> {

    private final NavigableMap<String, BaseEntry<String>> tempData = new TreeMap<>();
    private final Map<String, Integer> tempDataPriorities = new HashMap<>();
    private final Map<String, List<FileInfo>> lastElementWithFilesMap = new HashMap<>();
    private final List<Map.Entry<Path, FileInputStream>> filesMapEntries = new ArrayList<>();
    private final Iterator<BaseEntry<String>> inMemoryIterator;
    private final String to;
    private final boolean isFromNull;
    private final boolean isToNull;
    private String inMemoryLastKey;
    private BaseEntry<String> polledEntry;
    private boolean hasNextCalled;
    private boolean hasNextResult;

    public MergeIterator(PersistenceRangeDao dao, String from, String to, boolean includingMemory) throws IOException {
        this.to = to;
        this.isFromNull = from == null;
        this.isToNull = to == null;
        this.filesMapEntries.addAll(dao.getFilesMap().entrySet());
        int priority = 1;
        for (Map.Entry<Path, FileInputStream> filesMapEntry : filesMapEntries) {
            filesMapEntry.getValue().getChannel().position(0);
            BufferedReader bufferedReader = new BufferedReader(
                    new InputStreamReader(filesMapEntry.getValue(), UTF_8),
                    131072
            );
            BaseEntry<String> firstEntry = isFromNull
                    ? DaoUtils.readEntry(bufferedReader)
                    : DaoUtils.ceilKey(filesMapEntry.getKey(), bufferedReader, from);
            if (firstEntry != null && (isToNull || firstEntry.key().compareTo(to) < 0)) {
                tempData.put(firstEntry.key(), firstEntry);
                tempDataPriorities.put(firstEntry.key(), priority);
                lastElementWithFilesMap
                        .computeIfAbsent(firstEntry.key(), files -> new ArrayList<>())
                        .add(new FileInfo(priority, bufferedReader));
                priority++;
            }
        }
        if (includingMemory) {
            inMemoryIterator = dao.inMemoryDataIterator(from, to);
            if (inMemoryIterator.hasNext()) {
                BaseEntry<String> entry = inMemoryIterator.next();
                tempData.put(entry.key(), entry);
                tempDataPriorities.put(entry.key(), Integer.MAX_VALUE);
                inMemoryLastKey = entry.key();
            }
        } else {
            inMemoryIterator = Collections.emptyIterator();
        }
    }

    @Override
    public boolean hasNext() {
        if (hasNextCalled) {
            return hasNextResult;
        }
        if (tempData.isEmpty()) {
            return false;
        }
        try {
            do {
                polledEntry = tempData.pollFirstEntry().getValue();
                readNextFromFiles(lastElementWithFilesMap.get(polledEntry.key()));
                readNextFromMemory();
                tempDataPriorities.remove(polledEntry.key());
            } while (!tempData.isEmpty() && polledEntry.value() == null);
        } catch (IOException e) {
            throw new RuntimeException("Fail to read new Entry after" + polledEntry, e);
        }
        hasNextCalled = true;
        hasNextResult = polledEntry.value() != null && (isToNull || polledEntry.key().compareTo(to) < 0);
        return hasNextResult;
    }

    @Override
    public BaseEntry<String> next() {
        if (hasNextCalled && hasNextResult) {
            hasNextCalled = false;
            return polledEntry;
        }
        if (!hasNextCalled && hasNext()) {
            return polledEntry;
        }
        return null;
    }

    private void readNextFromMemory() {
        if (inMemoryIterator.hasNext() && inMemoryLastKey.equals(polledEntry.key())) {
            BaseEntry<String> newEntry = inMemoryIterator.next();
            tempData.put(newEntry.key(), newEntry);
            tempDataPriorities.put(newEntry.key(), Integer.MAX_VALUE);
            inMemoryLastKey = newEntry.key();
        }
    }

    private void readNextFromFiles(List<FileInfo> filesToRead) throws IOException {
        if (filesToRead == null) {
            return;
        }
        for (FileInfo fileInfo : filesToRead) {
            BaseEntry<String> newEntry = DaoUtils.readEntry(fileInfo.bufferedReader);
            if (newEntry == null) {
                continue;
            }
            Integer currentFileNumber = tempDataPriorities.get(newEntry.key());
            if (currentFileNumber == null || fileInfo.fileNumber > currentFileNumber) {
                tempData.put(newEntry.key(), newEntry);
                tempDataPriorities.put(newEntry.key(), fileInfo.fileNumber);
            }
            lastElementWithFilesMap
                    .computeIfAbsent(newEntry.key(), files -> new ArrayList<>())
                    .add(fileInfo);
        }
        lastElementWithFilesMap.remove(polledEntry.key());
    }

    public List<Map.Entry<Path, FileInputStream>> getFilesMapEntries() {
        return filesMapEntries;
    }

    private static final class FileInfo {
        private final int fileNumber;
        private final BufferedReader bufferedReader;

        private FileInfo(int fileNumber, BufferedReader bufferedReader) {
            this.fileNumber = fileNumber;
            this.bufferedReader = bufferedReader;
        }
    }
}
