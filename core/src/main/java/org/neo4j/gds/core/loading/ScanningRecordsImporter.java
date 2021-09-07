/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.core.loading;

import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.TransactionContext;
import org.neo4j.gds.core.loading.InternalImporter.ImportResult;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.concurrent.ExecutorService;

import static org.neo4j.gds.core.utils.mem.MemoryUsage.humanReadable;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public abstract class ScanningRecordsImporter<Record, T> {

    static final BigInteger A_BILLION = BigInteger.valueOf(1_000_000_000L);

    private final StoreScanner.Factory<Record> factory;
    private final String label;

    protected final ExecutorService threadPool;
    protected final TransactionContext transaction;
    protected final GraphDimensions dimensions;
    protected final AllocationTracker allocationTracker;
    protected final ProgressTracker progressTracker;
    protected final int concurrency;

    public ScanningRecordsImporter(
        StoreScanner.Factory<Record> factory,
        String label,
        GraphLoaderContext loadingContext,
        GraphDimensions dimensions,
        ProgressTracker progressTracker,
        int concurrency
    ) {
        this.factory = factory;
        this.label = label;
        this.transaction = loadingContext.transactionContext();
        this.dimensions = dimensions;
        this.threadPool = loadingContext.executor();
        this.allocationTracker = loadingContext.allocationTracker();
        this.progressTracker = progressTracker;
        this.concurrency = concurrency;
    }

    public final T call() {
        long nodeCount = dimensions.nodeCount();
        final ImportSizing sizing = ImportSizing.of(concurrency, nodeCount);
        int numberOfThreads = sizing.numberOfThreads();

        try (StoreScanner<Record> scanner = factory.newScanner(StoreScanner.DEFAULT_PREFETCH_SIZE, transaction)) {
            progressTracker
                .progressLogger()
                .getLog()
                .debug("%s Store Scan: Start using %s", label, scanner.getClass().getSimpleName());

            InternalImporter.CreateScanner creator = creator(nodeCount, sizing, scanner);
            InternalImporter importer = new InternalImporter(numberOfThreads, creator);
            ImportResult importResult = importer.runImport(threadPool);

            long requiredBytes = scanner.storeSize(dimensions);
            long recordsImported = importResult.recordsImported;
            long propertiesImported = importResult.propertiesImported;
            BigInteger bigNanos = BigInteger.valueOf(importResult.tookNanos);
            double tookInSeconds = new BigDecimal(bigNanos)
                    .divide(new BigDecimal(A_BILLION), 9, RoundingMode.CEILING)
                    .doubleValue();
            long bytesPerSecond = A_BILLION
                .multiply(BigInteger.valueOf(requiredBytes))
                .divide(bigNanos)
                .longValueExact();

            progressTracker.progressLogger().getLog().debug(
                formatWithLocale(
                    "%s Store Scan (%s): Imported %,d records and %,d properties from %s (%,d bytes); took %.3f s, %,.2f %1$ss/s, %s/s (%,d bytes/s) (per thread: %,.2f %1$ss/s, %s/s (%,d bytes/s))",
                    label,
                    scanner.getClass().getSimpleName(),
                    recordsImported,
                    propertiesImported,
                    humanReadable(requiredBytes),
                    requiredBytes,
                    tookInSeconds,
                    (double) recordsImported / tookInSeconds,
                    humanReadable(bytesPerSecond),
                    bytesPerSecond,
                    (double) recordsImported / tookInSeconds / numberOfThreads,
                    humanReadable(bytesPerSecond / numberOfThreads),
                    bytesPerSecond / numberOfThreads
                )
            );
        }

        return build();
    }

    public abstract InternalImporter.CreateScanner creator(
        long nodeCount,
        ImportSizing sizing,
        StoreScanner<Record> scanner
    );

    public abstract T build();
}
