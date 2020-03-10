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
package org.neo4j.graphalgo.core.write;

import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.LazyBatchCollection;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.utils.StatementApi;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.storable.Value;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;

public final class NodePropertyExporter extends StatementApi {

    static final long MIN_BATCH_SIZE = 10_000L;
    static final long MAX_BATCH_SIZE = 100_000L;

    private final TerminationFlag terminationFlag;
    private final ExecutorService executorService;
    private final ProgressLogger progressLogger;
    private final int concurrency;
    private final long nodeCount;
    private final LongUnaryOperator toOriginalId;
    private final LongAdder propertiesWritten;

    public static Builder of(GraphDatabaseAPI db, IdMapping idMapping, TerminationFlag terminationFlag) {
        return new Builder(db, idMapping, terminationFlag);
    }

    @ValueClass
    public interface NodeProperty<T> {
        String propertyKey();

        T data();

        PropertyTranslator<T> translator();

        default ResolvedNodeProperty resolveWith(int propertyToken) {
            if (propertyToken == -1) {
                throw new IllegalStateException("No write property token id is set.");
            }
            return ResolvedNodeProperty.of((NodeProperty<Object>) this, propertyToken);
        }
    }

    @ValueClass
    interface ResolvedNodeProperty extends NodeProperty<Object> {
        int propertyToken();

        static ResolvedNodeProperty of(NodeProperty<Object> nodeProperty, int propertyToken) {
            return ImmutableResolvedNodeProperty.of(
                nodeProperty.propertyKey(),
                nodeProperty.data(),
                nodeProperty.translator(),
                propertyToken
            );
        }
    }

    public static class Builder extends ExporterBuilder<NodePropertyExporter> {

        Builder(GraphDatabaseAPI db, IdMapping idMapping, TerminationFlag terminationFlag) {
            super(db, idMapping, terminationFlag);
        }

        @Override
        public NodePropertyExporter build() {
            ProgressLogger progressLogger = loggerAdapter == null
                ? ProgressLogger.NULL_LOGGER
                : loggerAdapter;
            return new NodePropertyExporter(
                db,
                nodeCount,
                toOriginalId,
                terminationFlag,
                progressLogger,
                writeConcurrency,
                executorService
            );
        }
    }

    public interface WriteConsumer {
        void accept(Write ops, long value) throws Exception;
    }

    private NodePropertyExporter(
        GraphDatabaseAPI db,
        long nodeCount,
        LongUnaryOperator toOriginalId,
        TerminationFlag terminationFlag,
        ProgressLogger log,
        int concurrency,
        ExecutorService executorService
    ) {
        super(db);
        this.nodeCount = nodeCount;
        this.toOriginalId = toOriginalId;
        this.terminationFlag = terminationFlag;
        this.progressLogger = log;
        this.concurrency = concurrency;
        this.executorService = executorService;
        this.propertiesWritten = new LongAdder();
    }

    public <T> void write(String property, T data, PropertyTranslator<T> translator) {
        write(ImmutableNodeProperty.of(property, data, translator));
    }

    public <T> void write(NodeProperty<T> nodeProperty) {
        write(Collections.singletonList(nodeProperty));
    }

    public void write(Collection<NodeProperty<?>> nodeProperties) {
        writeInternal(nodeProperties.stream()
            .map(desc -> desc.resolveWith(getOrCreatePropertyToken(desc.propertyKey())))
            .collect(Collectors.toList()));
    }

    private void writeInternal(List<ResolvedNodeProperty> nodeProperties) {
        if (ParallelUtil.canRunInParallel(executorService)) {
            writeParallel(nodeProperties);
        } else {
            writeSequential(nodeProperties);
        }
    }

    public long propertiesWritten() {
        return propertiesWritten.longValue();
    }

    private void writeSequential(List<ResolvedNodeProperty> nodeProperties) {
        writeSequential((ops, nodeId) -> doWrite(nodeProperties, ops, nodeId));
    }

    private void writeParallel(Iterable<ResolvedNodeProperty> nodeProperties) {
        writeParallel((ops, offset) -> doWrite(nodeProperties, ops, offset));
    }

    private void writeSequential(WriteConsumer writer) {
        acceptInTransaction(stmt -> {
            terminationFlag.assertRunning();
            long progress = 0L;
            Write ops = stmt.dataWrite();
            for (long i = 0L; i < nodeCount; i++) {
                writer.accept(ops, i);
                ++progress;
                if (progress % TerminationFlag.RUN_CHECK_NODE_COUNT == 0) {
                    progressLogger.logProgress(progress, nodeCount);
                    terminationFlag.assertRunning();
                }
            }
            progressLogger.logProgress(
                nodeCount,
                nodeCount
            );
        });
    }

    private void writeParallel(WriteConsumer writer) {
        final long batchSize = ParallelUtil.adjustedBatchSize(
            nodeCount,
            concurrency,
            MIN_BATCH_SIZE,
            MAX_BATCH_SIZE
        );
        final AtomicLong progress = new AtomicLong(0L);
        final Collection<Runnable> runnables = LazyBatchCollection.of(
            nodeCount,
            batchSize,
            (start, len) -> () -> {
                acceptInTransaction(stmt -> {
                    terminationFlag.assertRunning();
                    long end = start + len;
                    Write ops = stmt.dataWrite();
                    for (long currentNode = start; currentNode < end; currentNode++) {
                        writer.accept(ops, currentNode);

                        // Only log every 10_000 written nodes
                        if ((currentNode - start) % TerminationFlag.RUN_CHECK_NODE_COUNT == 0) {
                            long currentProgress = progress.addAndGet(TerminationFlag.RUN_CHECK_NODE_COUNT);
                            progressLogger.logProgress(
                                currentProgress,
                                nodeCount
                            );
                            terminationFlag.assertRunning();
                        }
                    }

                    // log progress for the last batch of written nodes
                    progressLogger.logProgress(
                        progress.addAndGet((end - start + 1) % TerminationFlag.RUN_CHECK_NODE_COUNT),
                        nodeCount
                    );
                });
            }
        );
        ParallelUtil.runWithConcurrency(
            concurrency,
            runnables,
            Integer.MAX_VALUE,
            10L,
            TimeUnit.MICROSECONDS,
            terminationFlag,
            executorService
        );
    }

    private void doWrite(Iterable<ResolvedNodeProperty> nodeProperties, Write ops, long nodeId) throws Exception {
        for (ResolvedNodeProperty nodeProperty : nodeProperties) {
            int propertyId = nodeProperty.propertyToken();
            final Value prop = nodeProperty.translator().toProperty(propertyId, nodeProperty.data(), nodeId);
            if (prop != null) {
                ops.nodeSetProperty(
                    toOriginalId.applyAsLong(nodeId),
                    propertyId,
                    prop
                );
                propertiesWritten.increment();
            }
        }
    }
}
