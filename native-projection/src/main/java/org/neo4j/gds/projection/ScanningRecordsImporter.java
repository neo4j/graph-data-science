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
package org.neo4j.gds.projection;

import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.loading.ImportSizing;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.transaction.TransactionContext;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.concurrent.ExecutorService;

import static org.neo4j.gds.mem.Estimate.humanReadable;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

abstract class ScanningRecordsImporter<Record, T> {

    private static final BigInteger A_BILLION = BigInteger.valueOf(1_000_000_000L);

    private final StoreScanner.Factory<Record> storeScannerFactory;

    protected final ExecutorService executorService;
    protected final TransactionContext transaction;
    protected final GraphDimensions dimensions;
    protected final ProgressTracker progressTracker;
    protected final Concurrency concurrency;

    ScanningRecordsImporter(
        StoreScanner.Factory<Record> storeScannerFactory,
        GraphLoaderContext loadingContext,
        GraphDimensions dimensions,
        ProgressTracker progressTracker,
        Concurrency concurrency
    ) {
        this.storeScannerFactory = storeScannerFactory;
        this.transaction = loadingContext.transactionContext();
        this.dimensions = dimensions;
        this.executorService = loadingContext.executor();
        this.progressTracker = progressTracker;
        this.concurrency = concurrency;
    }

    public final T call() {
        long nodeCount = dimensions.nodeCount();
        var sizing = ImportSizing.of(concurrency, nodeCount);
        int threadCount = sizing.threadCount();

        try (StoreScanner<Record> storeScanner = storeScannerFactory.newScanner(
            StoreScanner.DEFAULT_PREFETCH_SIZE,
            transaction
        )) {
            progressTracker.beginSubTask("Store Scan");

            progressTracker.logDebug(formatWithLocale("Start using %s", storeScanner.getClass().getSimpleName()));

            var taskFactory = recordScannerTaskFactory(nodeCount, sizing, storeScanner);
            var taskRunner = new RecordScannerTaskRunner(threadCount, taskFactory);

            var importResult = taskRunner.runImport(executorService);

            long requiredBytes = storeScanner.storeSize(dimensions);
            long recordsImported = importResult.importedRecords();
            long propertiesImported = importResult.importedProperties();
            BigInteger bigNanos = BigInteger.valueOf(importResult.durationNanos());
            double tookInSeconds = new BigDecimal(bigNanos)
                .divide(new BigDecimal(A_BILLION), 9, RoundingMode.CEILING)
                .doubleValue();
            long bytesPerSecond = A_BILLION
                .multiply(BigInteger.valueOf(requiredBytes))
                .divide(bigNanos)
                .longValueExact();

            progressTracker.logDebug(
                formatWithLocale(
                    "Imported %,d records and %,d properties from %s (%,d bytes);" +
                    " took %.3f s, %,.2f %1$ss/s, %s/s (%,d bytes/s) (per thread: %,.2f %1$ss/s, %s/s (%,d bytes/s))",
                    recordsImported,
                    propertiesImported,
                    humanReadable(requiredBytes),
                    requiredBytes,
                    tookInSeconds,
                    (double) recordsImported / tookInSeconds,
                    humanReadable(bytesPerSecond),
                    bytesPerSecond,
                    (double) recordsImported / tookInSeconds / threadCount,
                    humanReadable(bytesPerSecond / threadCount),
                    bytesPerSecond / threadCount
                )
            );

        } finally {
            progressTracker.endSubTask("Store Scan");
        }

        return build();
    }

    public abstract RecordScannerTaskRunner.RecordScannerTaskFactory recordScannerTaskFactory(
        long nodeCount,
        ImportSizing sizing,
        StoreScanner<Record> storeScanner
    );

    public abstract T build();
}
