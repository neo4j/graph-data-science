/*
 * Copyright (c) 2017-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.huge.loader.ImportingThreadPool.ImportResult;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.concurrent.ExecutorService;

import static org.neo4j.graphalgo.core.huge.loader.AbstractStorePageCacheScanner.DEFAULT_PREFETCH_SIZE;
import static org.neo4j.graphalgo.core.utils.paged.AllocationTracker.humanReadable;


abstract class ScanningRecordsImporter<Record extends AbstractBaseRecord, T> {

    private static final BigInteger A_BILLION = BigInteger.valueOf(1_000_000_000L);

    private final AbstractStorePageCacheScanner.Access<Record> access;
    private final String label;
    final GraphDatabaseAPI api;
    final GraphDimensions dimensions;
    private final ExecutorService threadPool;
    private final int concurrency;

    ScanningRecordsImporter(
            AbstractStorePageCacheScanner.Access<Record> access,
            String label,
            GraphDatabaseAPI api,
            GraphDimensions dimensions,
            ExecutorService threadPool,
            int concurrency) {
        this.access = access;
        this.label = label;
        this.api = api;
        this.dimensions = dimensions;
        this.threadPool = threadPool;
        this.concurrency = concurrency;
    }

    final T call(Log log) {
        long nodeCount = dimensions.hugeNodeCount();
        final ImportSizing sizing = ImportSizing.of(concurrency, nodeCount);
        int numberOfThreads = sizing.numberOfThreads();

        AbstractStorePageCacheScanner<Record> scanner =
                new AbstractStorePageCacheScanner<>(DEFAULT_PREFETCH_SIZE, api, access);

        ImportingThreadPool.CreateScanner creator = creator(nodeCount, sizing, scanner);
        ImportingThreadPool pool = new ImportingThreadPool(numberOfThreads, creator);
        ImportResult importResult = pool.run(threadPool);

        long requiredBytes = scanner.storeSize();
        long nodesImported = importResult.recordsImported;
        BigInteger bigNanos = BigInteger.valueOf(importResult.tookNanos);
        double tookInSeconds = new BigDecimal(bigNanos)
                .divide(new BigDecimal(A_BILLION), 9, RoundingMode.CEILING)
                .doubleValue();
        long bytesPerSecond = A_BILLION.multiply(BigInteger.valueOf(requiredBytes)).divide(bigNanos).longValueExact();

        log.info(
                "%s Store Scan: Imported %,d records from %s (%,d bytes); took %.3f s, %,.2f %1$ss/s, %s/s (%,d bytes/s) (per thread: %,.2f %1$ss/s, %s/s (%,d bytes/s))",
                label,
                nodesImported,
                humanReadable(requiredBytes),
                requiredBytes,
                tookInSeconds,
                (double) nodesImported / tookInSeconds,
                humanReadable(bytesPerSecond),
                bytesPerSecond,
                (double) nodesImported / tookInSeconds / numberOfThreads,
                humanReadable(bytesPerSecond / numberOfThreads),
                bytesPerSecond / numberOfThreads
        );

        return build();
    }

    abstract ImportingThreadPool.CreateScanner creator(
            long nodeCount,
            ImportSizing sizing,
            AbstractStorePageCacheScanner<Record> scanner);

    abstract T build();
}
