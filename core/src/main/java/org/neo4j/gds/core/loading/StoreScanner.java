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

import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.kernel.api.KernelTransaction;

public interface StoreScanner<Reference> extends AutoCloseable {

    int DEFAULT_PREFETCH_SIZE = 100;

    interface RecordConsumer<Reference> {
        /**
         * Handles the given record and tells the caller,
         * if it can accept more records. Can also ignore
         * the record if it is not of interest.
         *
         * @param reference record
         * @return true, iff the consumer can consume more records
         */
        boolean offer(Reference reference);
    }

    interface Factory<Reference> {
        StoreScanner<Reference> newScanner(int prefetchSize, TransactionContext transaction);
    }

    interface ScanCursor<Reference> extends AutoCloseable {
        /**
         * Advances the underlying {@link org.neo4j.internal.kernel.api.EntityCursor}
         * to the next batch of records.
         *
         * @return true, iff the batch contains data and needs to be consumed
         */
        boolean scanBatch();

        /**
         * Consumes the current batch using the given consumer. The consumer is
         * typically an instance of {@link org.neo4j.gds.core.loading.RecordsBatchBuffer}.
         * <p/>
         * The method returns a {@code boolean} to indicate whether
         * the current batch has been completely consumed or not.
         * This gives the consumer the option to indicate that, e.g.
         * its buffer is full and needs to be flushed before it can
         * continue consuming.
         * <p/>
         * This is usually the case if the underlying scan returns
         * batches that exceed the configured size of the
         * {@link org.neo4j.gds.core.loading.RecordsBatchBuffer}.
         *
         * @param consumer A consumer for the records returned by the scan,
         *                 usually a {@link org.neo4j.gds.core.loading.RecordsBatchBuffer}.
         * @return true, iff the batch is completely consumed
         */
        boolean consumeBatch(RecordConsumer<Reference> consumer);

        @Override
        void close();
    }

    ScanCursor<Reference> createCursor(KernelTransaction transaction);

    long storeSize(GraphDimensions graphDimensions);

    int bufferSize();

    @Override
    void close();
}
