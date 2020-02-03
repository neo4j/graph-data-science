/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.loading;

import org.neo4j.graphalgo.ResolvedPropertyMapping;
import org.neo4j.graphalgo.ResolvedPropertyMappings;
import org.apache.commons.compress.utils.Lists;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.utils.ArrayUtil;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import java.util.stream.Collectors;

abstract class CypherRecordLoader<R> {

    enum QueryType {
        NODE, RELATIONSHIP
    }

    static final long NO_COUNT = -1L;

    private final String loadQuery;
    protected final GraphDatabaseAPI api;
    private final long recordCount;
    final GraphSetup setup;

    CypherRecordLoader(String loadQuery, long recordCount, GraphDatabaseAPI api, GraphSetup setup) {
        this.loadQuery = loadQuery;
        this.recordCount = recordCount;
        this.api = api;
        this.setup = setup;
    }

    final R load() {
        try {
            if (loadsInParallel()) {
                parallelLoad();
            } else {
                nonParallelLoad();
            }
            return result();
        } catch (AuthorizationViolationException ex) {
            throw new IllegalArgumentException(String.format("Query must be read only. Query: [%s]", loadQuery));
        }
    }

    abstract QueryType queryType();

    abstract BatchLoadResult loadOneBatch(
            long offset,
            int batchSize,
            int bufferSize
    );

    void validateProperties(
        Collection<String> propertyColumns,
        ResolvedPropertyMappings resolvedPropertyMappings,
        String elementType
    ) {
        List<String> invalidNodeProperties = resolvedPropertyMappings
            .mappings()
            .stream()
            .map(ResolvedPropertyMapping::neoPropertyKey)
            .filter(k -> !propertyColumns.contains(k))
            .collect(Collectors.toList());

        if (!invalidNodeProperties.isEmpty()) {
            throw new IllegalArgumentException(String.format(
                "%s properties not found: '%s'. Available properties from the %s query are: '%s'",
                elementType,
                String.join("', '", invalidNodeProperties),
                elementType.toLowerCase(),
                String.join("', '", propertyColumns)
            ));
        }
    }

    abstract void updateCounts(BatchLoadResult result);

    abstract R result();

    Set<String> getRequiredColumns() {
        return getReservedColumns();
    }

    abstract Set<String> getReservedColumns();

    Collection<String> getPropertyColumns(Result queryResult) {
        Predicate<String> contains = getReservedColumns()::contains;
        return queryResult
            .columns()
            .stream()
            .filter(contains.negate())
            .collect(Collectors.toList());
    }

    private boolean loadsInParallel() {
        return CypherLoadingUtils.canBatchLoad(setup.concurrency(), loadQuery);
    }

    private void parallelLoad() {
        ExecutorService pool = setup.executor();

        final int threads;
        final int batchSize;
        final int bufferSize;
        // if records are not counted we are a counting loader and must count
        // otherwise we are an importing node/relationship loader and must import
        if (recordCount == NO_COUNT) {
            threads = setup.concurrency();
            batchSize = ArrayUtil.MAX_ARRAY_LENGTH;
            bufferSize = RecordsBatchBuffer.DEFAULT_BUFFER_SIZE;
        } else {
            long optimalNumberOfThreads = BitUtil.ceilDiv(recordCount, ArrayUtil.MAX_ARRAY_LENGTH);
            threads = (int) Math.min(setup.concurrency(), optimalNumberOfThreads);
            long optimalBatchSize = BitUtil.ceilDiv(recordCount, threads);
            batchSize = (int) Math.min(optimalBatchSize, ArrayUtil.MAX_ARRAY_LENGTH);
            bufferSize = Math.min(RecordsBatchBuffer.DEFAULT_BUFFER_SIZE, batchSize);
        }

        long offset = 0;
        long lastOffset = 0;
        Deque<Future<BatchLoadResult>> futures = new ArrayDeque<>(threads);
        boolean working = true;
        do {
            long skip = offset;
            futures.add(pool.submit(() -> loadOneBatch(skip, batchSize, bufferSize)));
            offset += batchSize;
            if (futures.size() >= threads) {
                Future<BatchLoadResult> oldestTask = futures.removeFirst();
                BatchLoadResult result = CypherLoadingUtils.get(
                        "Error during loading relationships offset: " + (lastOffset + batchSize),
                        oldestTask);
                updateCounts(result);
                lastOffset = result.offset();
                working = result.rows() > 0;
            }
        } while (working);
        futures.forEach(f -> f.cancel(false));
    }

    private void nonParallelLoad() {
        int bufferSize = (int) Math.min(recordCount, RecordsBatchBuffer.DEFAULT_BUFFER_SIZE);
        BatchLoadResult result = loadOneBatch(0L, CypherLoadingUtils.NO_BATCHING, bufferSize);
        updateCounts(result);
    }

    Result runLoadingQuery(long offset, int batchSize) {
        Map<String, Object> parameters =
                batchSize == CypherLoadingUtils.NO_BATCHING
                        ? setup.params()
                        : CypherLoadingUtils.params(setup.params(), offset, batchSize);
        Result result = api.execute(loadQuery, parameters);
        validateRequiredColumns(Lists.newArrayList(result.columns().iterator()));
        return result;
    }

    private void validateRequiredColumns(List<String> allColumns) {
        Set<String> missingColumns = new HashSet<>(getRequiredColumns());
        missingColumns.removeAll(allColumns);
        if (!missingColumns.isEmpty()) {
            throw new IllegalArgumentException(String.format(
                "Invalid %s query, required column(s) not found: %s",
                queryType().toString().toLowerCase(),
                String.join(", ", missingColumns)
            ));
        }
    }

}
