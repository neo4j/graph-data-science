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
package org.neo4j.graphalgo.core.write;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.LazyBatchCollection;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.StatementApi;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.storable.Value;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongUnaryOperator;

public final class NodeExporter extends StatementApi {

    static final long MIN_BATCH_SIZE = 10_000L;
    static final long MAX_BATCH_SIZE = 100_000L;

    private final TerminationFlag terminationFlag;
    private final ExecutorService executorService;
    private final ProgressLogger progressLogger;
    private final int concurrency;
    private final long nodeCount;
    private final LongUnaryOperator toOriginalId;

    public static Builder of(GraphDatabaseAPI db, IdMapping idMapping, TerminationFlag terminationFlag) {
        return new Builder(db, idMapping, terminationFlag);
    }

    public static class Builder extends ExporterBuilder<NodeExporter> {

        Builder(GraphDatabaseAPI db, IdMapping idMapping, TerminationFlag terminationFlag) {
            super(db, idMapping, terminationFlag);
        }

        @Override
        public NodeExporter build() {
            ProgressLogger progressLogger = loggerAdapter == null
                ? ProgressLogger.NULL_LOGGER
                : loggerAdapter;
            return new NodeExporter(
                db,
                nodeCount,
                toOriginalId,
                terminationFlag,
                progressLogger,
                writeConcurrency,
                executorService);
        }
    }

    public interface WriteConsumer {
        void accept(Write ops, long value) throws KernelException;
    }

    private NodeExporter(
            GraphDatabaseAPI db,
            long nodeCount,
            LongUnaryOperator toOriginalId,
            TerminationFlag terminationFlag,
            ProgressLogger log,
            int concurrency,
            ExecutorService executorService) {
        super(db);
        this.nodeCount = nodeCount;
        this.toOriginalId = toOriginalId;
        this.terminationFlag = terminationFlag;
        this.progressLogger = log;
        this.concurrency = concurrency;
        this.executorService = executorService;
    }

    public <T> void write(
            String property,
            T data,
            PropertyTranslator<T> translator) {
        final int propertyId = getOrCreatePropertyToken(property);
        if (propertyId == -1) {
            throw new IllegalStateException("no write property id is set");
        }
        if (ParallelUtil.canRunInParallel(executorService)) {
            writeParallel(propertyId, data, translator);
        } else {
            writeSequential(propertyId, data, translator);
        }
    }

    public <T, U> void write(
            String property1,
            T data1,
            PropertyTranslator<T> translator1,
            String property2,
            U data2,
            PropertyTranslator<U> translator2) {
        final int propertyId1 = getOrCreatePropertyToken(property1);
        if (propertyId1 == -1) {
            throw new IllegalStateException("no write property id is set");
        }
        final int propertyId2 = getOrCreatePropertyToken(property2);
        if (propertyId2 == -1) {
            throw new IllegalStateException("no write property id is set");
        }
        if (ParallelUtil.canRunInParallel(executorService)) {
            writeParallel(propertyId1, data1, translator1, propertyId2, data2, translator2);
        } else {
            writeSequential(propertyId1, data1, translator1, propertyId2, data2, translator2);
        }
    }

    private <T> void writeSequential(
            int propertyId,
            T data,
            PropertyTranslator<T> translator) {
        writeSequential((ops, offset) -> doWrite(propertyId, data, translator, ops, offset));
    }

    private <T, U> void writeSequential(
            int propertyId1,
            T data1,
            PropertyTranslator<T> translator1,
            int propertyId2,
            U data2,
            PropertyTranslator<U> translator2) {
        writeSequential((ops, offset) -> doWrite(
                propertyId1,
                data1,
                translator1,
                propertyId2,
                data2,
                translator2,
                ops,
                offset));
    }

    private <T> void writeParallel(
            int propertyId,
            T data,
            PropertyTranslator<T> translator) {
        writeParallel((ops, offset) -> doWrite(propertyId, data, translator, ops, offset));
    }

    private <T, U> void writeParallel(
            int propertyId1,
            T data1,
            PropertyTranslator<T> translator1,
            int propertyId2,
            U data2,
            PropertyTranslator<U> translator2) {
        writeParallel((ops, offset) -> doWrite(
                propertyId1,
                data1,
                translator1,
                propertyId2,
                data2,
                translator2,
                ops,
                offset));
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
                    nodeCount);
        });
    }

    private void writeParallel(WriteConsumer writer) {
        final long batchSize = ParallelUtil.adjustedBatchSize(
                nodeCount,
                concurrency,
                MIN_BATCH_SIZE,
                MAX_BATCH_SIZE);
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
                                        nodeCount);
                                terminationFlag.assertRunning();
                            }
                        }

                        // log progress for the last batch of written nodes
                        progressLogger.logProgress(
                                progress.addAndGet((end - start + 1) % TerminationFlag.RUN_CHECK_NODE_COUNT),
                                nodeCount);
                    });
                });
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

    private <T> void doWrite(
            int propertyId,
            T data,
            PropertyTranslator<T> trans,
            Write ops,
            long nodeId) throws KernelException {
        final Value prop = trans.toProperty(propertyId, data, nodeId);
        if (prop != null) {
            ops.nodeSetProperty(
                    toOriginalId.applyAsLong(nodeId),
                    propertyId,
                    prop
            );
        }
    }

    private <T, U> void doWrite(
            int propertyId1,
            T data1,
            PropertyTranslator<T> translator1,
            int propertyId2,
            U data2,
            PropertyTranslator<U> translator2,
            Write ops,
            long nodeId) throws KernelException {
        final long originalNodeId = toOriginalId.applyAsLong(nodeId);
        Value prop1 = translator1.toProperty(propertyId1, data1, nodeId);
        if (prop1 != null) {
            ops.nodeSetProperty(originalNodeId, propertyId1, prop1);
        }
        Value prop2 = translator2.toProperty(propertyId2, data2, nodeId);
        if (prop2 != null) {
            ops.nodeSetProperty(originalNodeId, propertyId2, prop2);
        }
    }
}
