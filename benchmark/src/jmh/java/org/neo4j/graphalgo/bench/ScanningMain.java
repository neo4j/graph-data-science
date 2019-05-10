/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.bench;

import org.apache.commons.lang3.StringUtils;
import org.neo4j.function.Predicates;
import org.neo4j.graphalgo.core.huge.loader.AbstractStorePageCacheScanner;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.helper.ldbc.LdbcDownloader;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.io.File;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.core.utils.paged.AllocationTracker.humanReadable;

class ScanningMain<Record extends AbstractBaseRecord> extends BaseMain {

    private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();

    private static final MethodType NO_SCAN_TYPE = MethodType.methodType(
            long.class,
            GraphDatabaseAPI.class,
            RecordStore.class,
            int.class,
            long.class);

    private static final MethodType SCAN_TYPE = MethodType.methodType(
            long.class,
            int.class,
            GraphDatabaseAPI.class,
            RecordStore.class,
            int.class,
            long.class);

    private final AbstractStorePageCacheScanner.Access<Record> access;
    private final Collection<Action> methodsToRun = new ArrayList<>();

    ScanningMain(AbstractStorePageCacheScanner.Access<Record> access) {
        this.access = access;
    }

    @Override
    final void init(final Collection<String> args) {
        Collection<Integer> multiScanPrefetchSizes = new ArrayList<>();
        args.removeIf(arg -> {
            if (arg.startsWith("-prefetch=") || arg.startsWith("-preFetch=")) {
                String prefetchValues = arg.split("=")[1].trim();
                Arrays.stream(prefetchValues.split(","))
                        .map(String::trim)
                        .map(Integer::valueOf)
                        .forEach(multiScanPrefetchSizes::add);
                return true;
            }
            return false;
        });

        if (multiScanPrefetchSizes.isEmpty()) {
            multiScanPrefetchSizes.add(AbstractStorePageCacheScanner.DEFAULT_PREFETCH_SIZE);
        }

        final Collection<String> methodNames = Arrays.asList(
                "singleThreadAllRecords",
                "singleThreadOnlyPages",
                "singleThreadScanAllRecords",
                "multiThreadedAllRecords",
                "multiThreadedOnlyPages",
                "multiThreadedScanAllRecords"
        );

        methodNames
                .stream()
                .filter(matches(args))
                .flatMap(m -> find(multiScanPrefetchSizes, m))
                .forEach(methodsToRun::add);
    }

    @Override
    final Iterable<String> run(String graphToLoad, Log log) throws Throwable {
        final Collection<String> messages = new ArrayList<>(methodsToRun.size());
        for (Action methodAction : methodsToRun) {
            log.info("Running test: %s", methodAction);
            messages.add(doWork(graphToLoad, log, methodAction));
        }
        return messages;
    }

    private static Predicate<String> matches(Collection<String> provided) {
        if (provided.isEmpty()) {
            return name -> true;
        }
        return name -> matches(provided, name.toLowerCase());
    }

    private static boolean matches(Collection<String> provided, String name) {
        for (String s : provided) {
            if (name.equalsIgnoreCase(s) || name.startsWith(s) || name.endsWith(s)) {
                return true;
            }
            s = s.toLowerCase();
            if (name.equals(s) || name.startsWith(s) || name.endsWith(s)) {
                return true;
            }
        }
        return false;
    }

    private Stream<Action> find(Collection<Integer> prefetchSizes, String name) {
        try {
            return Stream.of(actionOf(LOOKUP.findVirtual(ScanningMain.class, name, NO_SCAN_TYPE)));
        } catch (NoSuchMethodException | IllegalAccessException e) {
            try {
                MethodHandle handle = LOOKUP.findVirtual(ScanningMain.class, name, SCAN_TYPE);
                String baseLabel = nameFromHandle(handle);
                String baseToString = toStringFromHandle(handle);
                return prefetchSizes.stream().map(s -> {
                    MethodHandle newHandle = MethodHandles.insertArguments(handle, 1, s);
                    return new Action(newHandle, baseLabel + ", prefetch=" + s, baseToString);
                });
            } catch (NoSuchMethodException | IllegalAccessException e1) {
                throw new IllegalArgumentException("Could not find method '" + name);
            }
        }
    }

    private String nameFromHandle(final MethodHandle handle) {
        String methodName = LOOKUP.revealDirect(handle).getName();
        return Arrays.stream(StringUtils.splitByCharacterTypeCamelCase(methodName))
                .filter(StringUtils::isNotBlank)
                .map(String::toLowerCase)
                .collect(Collectors.joining(" "));
    }

    private String toStringFromHandle(final MethodHandle handle) {
        Method method = LOOKUP.revealDirect(handle).reflectAs(Method.class, LOOKUP);
        StringBuilder sb = new StringBuilder();
        sb
                .append(method.getReturnType().getTypeName())
                .append(' ')
                .append(method.getName())
                .append('(');
        Class<?>[] types = method.getParameterTypes();
        for (Class<?> type : types) {
            sb.append(type.getTypeName()).append(',');
        }
        sb.setCharAt(sb.length() - 1, ')');
        return sb.toString();
    }

    private Action actionOf(final MethodHandle handle) {
        return new Action(handle, nameFromHandle(handle), toStringFromHandle(handle));
    }

    private final class Action {
        private final MethodHandle handle;
        private final String label;
        private final String toString;

        private Action(
                final MethodHandle handle,
                final String label,
                final String toString) {
            this.handle = handle;
            this.label = label;
            this.toString = toString;
        }

        long run(
                GraphDatabaseAPI db,
                RecordStore<Record> recordStore,
                int recordsPerPage,
                long highestRecordId) throws Throwable {
            return (long) handle.invokeExact(ScanningMain.this, db, recordStore, recordsPerPage, highestRecordId);
        }

        String label() {
            return label;
        }

        @Override
        public String toString() {
            return toString;
        }

    }

    private String doWork(String graphToLoad, Log log, Action action) throws Throwable {

        GraphDatabaseAPI db = LdbcDownloader.openDb(graphToLoad);
        DependencyResolver dep = db.getDependencyResolver();
        NeoStores neoStores = dep
                .resolveDependency(RecordStorageEngine.class)
                .testAccessNeoStores();
        RecordStore<Record> store = access.store(neoStores);
        String storeFileName = access.storeFileName();

        long recordsInUse = 1L + store.getHighestPossibleIdInUse();
        long recordsPerPage = (long) store.getRecordsPerPage();
        long idsInPages = ((recordsInUse + (recordsPerPage - 1L)) / recordsPerPage) * recordsPerPage;
        long requiredBytes = dep.resolveDependency(PageCache.class)
                .listExistingMappings()
                .stream()
                .map(PagedFile::file)
                .filter(f -> f.getName().equals(storeFileName))
                .mapToLong(File::length)
                .filter(size -> size >= 0L)
                .peek(fileSize -> log.info("Store size: %,d (%s)", fileSize, humanReadable(fileSize)))
                .findAny()
                .orElseGet(() -> {
                    long fileSize = idsInPages * (long) store.getRecordSize();
                    log.info("Store size (estimated): %,d (%s)", fileSize, humanReadable(fileSize));
                    return fileSize;
                });

        String label = action.label();

        System.gc();

        jprofBookmark("starting " + label);

        long start = System.nanoTime();
        long recordsImported = action.run(db, store, (int) recordsPerPage, idsInPages);
        long stop = System.nanoTime();
        long tookNanos = stop - start;

        BigInteger bigNanos = BigInteger.valueOf(tookNanos);
        BigInteger aBillion = BigInteger.valueOf(1_000_000_000L);
        double tookInSeconds = new BigDecimal(bigNanos)
                .divide(new BigDecimal(aBillion), 9, RoundingMode.CEILING)
                .doubleValue();
        long bytesPerSecond = aBillion.multiply(BigInteger.valueOf(requiredBytes)).divide(bigNanos).longValueExact();

        final String logMsg = String.format(
                "[%s] took %.3f s, overall %s/s (%,d bytes/s), imported %,d records (%,.2f/s)",
                label,
                tookInSeconds,
                humanReadable(bytesPerSecond),
                bytesPerSecond,
                recordsImported,
                (double) recordsImported / tookInSeconds
        );

        jprofBookmark("end " + label);

        log.info("after " + label);
        System.gc();

        db.shutdown();
        return logMsg;
    }

    // ================

    @SuppressWarnings("unused")
    private long singleThreadAllRecords(
            GraphDatabaseAPI db,
            RecordStore<Record> recordStore,
            int recordsPerPage,
            long highestRecordId) {
        final Record record = recordStore.newRecord();
        long total;
        for (total = 0L; total < highestRecordId; ++total) {
            recordStore.getRecord(total, record, RecordLoad.CHECK);
        }
        return total;
    }

    @SuppressWarnings("unused")
    private long singleThreadOnlyPages(
            GraphDatabaseAPI db,
            RecordStore<Record> recordStore,
            int recordsPerPage,
            long highestRecordId) {
        long total = 0L;
        final Record record = recordStore.newRecord();
        for (long i = 0L; i < highestRecordId; i += recordsPerPage) {
            recordStore.getRecord(i, record, RecordLoad.CHECK);
            ++total;
        }
        return total;
    }

    @SuppressWarnings("unused")
    private long singleThreadScanAllRecords(
            int prefetchSize,
            GraphDatabaseAPI db,
            RecordStore<Record> recordStore,
            int recordsPerPage,
            long highestRecordId) {
        long total = 0L;
        Predicate<Record> alwaysTrue = Predicates.alwaysTrue();
        AbstractStorePageCacheScanner<Record> scanner = access.newScanner(db, prefetchSize);
        try (AbstractStorePageCacheScanner<Record>.Cursor cursor = scanner.getCursor()) {
            while (cursor.next(alwaysTrue)) {
                ++total;
            }
        }
        return total;
    }

    @SuppressWarnings("unused")
    private long multiThreadedAllRecords(
            GraphDatabaseAPI db,
            RecordStore<Record> recordStore,
            int recordsPerPage,
            long highestRecordId) {
        final Record record = recordStore.newRecord();
        return loadRecords(
                recordsPerPage,
                highestRecordId,
                loadAllRecords(recordsPerPage, highestRecordId, record, recordStore),
                db);
    }

    @SuppressWarnings("unused")
    private long multiThreadedOnlyPages(
            GraphDatabaseAPI db,
            RecordStore<Record> recordStore,
            int recordsPerPage,
            long highestRecordId) {
        final Record record = recordStore.newRecord();
        return loadRecords(recordsPerPage, highestRecordId, loadRecords(record, recordStore), db);
    }

    @SuppressWarnings("unused")
    private long multiThreadedScanAllRecords(
            int prefetchSize,
            GraphDatabaseAPI db,
            RecordStore<Record> recordStore,
            int recordsPerPage,
            long highestRecordId) {
        Predicate<Record> alwaysTrue = Predicates.alwaysTrue();
        AbstractStorePageCacheScanner<Record> scanner = access.newScanner(db, prefetchSize);
        List<Future<Long>> futures = new ArrayList<>();
        for (int i = 0; i < Pools.DEFAULT_CONCURRENCY; i++) {
            futures.add(Pools.DEFAULT.submit(() -> {
                long total = 0L;
                try (AbstractStorePageCacheScanner<Record>.Cursor cursor = scanner.getCursor()) {
                    while (cursor.next(alwaysTrue)) {
                        ++total;
                    }
                }
                return total;
            }));
        }
        return removeDone(futures, true);
    }

    private static long loadRecords(
            int recordsPerPage,
            long highestRecordId,
            ToLongFunction<long[]> loader,
            GraphDatabaseAPI db) {
        long total = 0L;
        int batchSize = 100_000;
        long[] ids = new long[batchSize];
        int idx = 0;
        List<Future<Long>> futures = new ArrayList<>(100);
        for (long id = 0; id < highestRecordId; id += recordsPerPage) {
            ids[idx++] = id;
            if (idx == batchSize) {
                long[] submitted = ids.clone();
                idx = 0;
                futures.add(inTxFuture(db, loader, submitted));
            }
            total += removeDone(futures, false);
        }
        if (idx > 0) {
            long[] submitted = Arrays.copyOf(ids, idx);
            futures.add(inTxFuture(db, loader, submitted));
        }
        total += removeDone(futures, true);
        return total;
    }

    private ToLongFunction<long[]> loadRecords(Record record, RecordStore<Record> recordStore) {
        return submitted -> loadRecords(record, recordStore, submitted);
    }

    private long loadRecords(Record record, RecordStore<Record> recordStore, long[] submitted) {
        long total = 0L;
        for (long recordId : submitted) {
            total += loadSingleRecord(record, recordStore, recordId);
        }
        return total;
    }

    private ToLongFunction<long[]> loadAllRecords(
            int recordsPerPage,
            long highestRecordId,
            Record record,
            RecordStore<Record> recordStore) {
        return submitted -> loadAllRecords(recordsPerPage, highestRecordId, record, recordStore, submitted);
    }

    private long loadAllRecords(
            int recordsPerPage,
            long highestRecordId,
            Record record,
            RecordStore<Record> recordStore,
            long[] submitted) {
        long total = 0L;
        if (submitted.length != 0) {
            long last = Math.min(submitted[submitted.length - 1] + recordsPerPage, highestRecordId);
            for (long i = submitted[0]; i < last; i++) {
                total += loadSingleRecord(record, recordStore, i);
            }
        }
        return total;
    }

    private long loadSingleRecord(
            final Record record,
            final RecordStore<Record> recordStore,
            final long i) {
        record.setId(i);
        record.clear();
        try {
            recordStore.getRecord(i, record, RecordLoad.CHECK);
            return 1L;
        } catch (Exception ignore) {
            // ignore
        }
        return 0L;
    }

    private static long removeDone(List<Future<Long>> futures, boolean wait) {
        if (wait || futures.size() > 25) {
            Iterator<Future<Long>> it = futures.iterator();
            long total = 0L;
            while (it.hasNext()) {
                Future<Long> future = it.next();
                if (wait || future.isDone()) {
                    try {
                        total += future.get();
                    } catch (InterruptedException | ExecutionException e) {
                        // log.warn("Error during task execution", e);
                    }
                    it.remove();
                }
            }
            return total;
        }
        return 0L;
    }

    private static Future<Long> inTxFuture(GraphDatabaseService db, ToLongFunction<long[]> loader, long[] submitted) {
        try {
            return Pools.DEFAULT.submit(() -> {
                long imported;
                try (Transaction tx = db.beginTx()) {
                    imported = loader.applyAsLong(submitted);
                    tx.success();
                }
                return imported;
            });
        } catch (Exception e) {
            throw new RuntimeException("Error executing in separate transaction", e);
        }
    }
}
