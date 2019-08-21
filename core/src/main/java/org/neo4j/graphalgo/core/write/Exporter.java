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
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressLoggerAdapter;
import org.neo4j.graphalgo.core.utils.StatementApi;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.values.storable.Value;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;
import java.util.function.LongUnaryOperator;

public final class Exporter extends StatementApi {

    private static final long MIN_BATCH_SIZE = 10_000L;
    private static final long MAX_BATCH_SIZE = 100_000L;
    public static final String TASK_EXPORT = "EXPORT";

    private final TerminationFlag terminationFlag;
    private final ExecutorService executorService;
    private final ProgressLogger progressLogger;
    private final int concurrency;
    private final long nodeCount;
    private final LongUnaryOperator toOriginalId;

    public static Builder of(GraphDatabaseAPI db, IdMapping idMapping) {
        return new Builder(db, idMapping);
    }

    public static final class Builder {

        private final GraphDatabaseAPI db;
        private final LongUnaryOperator toOriginalId;
        private final long nodeCount;
        private TerminationFlag terminationFlag;
        private ExecutorService executorService;
        private ProgressLoggerAdapter loggerAdapter;
        private int concurrency;

        private Builder(GraphDatabaseAPI db, IdMapping idMapping) {
            Objects.requireNonNull(idMapping);
            this.db = Objects.requireNonNull(db);
            this.nodeCount = idMapping.nodeCount();
            this.toOriginalId = idMapping::toOriginalNodeId;
            this.concurrency = Pools.DEFAULT_CONCURRENCY;
        }

        public Builder withLog(Log log) {
            loggerAdapter = new ProgressLoggerAdapter(Objects.requireNonNull(log), TASK_EXPORT);
            return this;
        }

        public Builder withLogInterval(long time, TimeUnit unit) {
            if (loggerAdapter == null) {
                throw new IllegalStateException("no logger set");
            }
            final long logTime = unit.toMillis(time);
            if ((int) logTime != logTime) {
                throw new IllegalArgumentException("timespan too large");
            }
            loggerAdapter.withLogIntervalMillis((int) logTime);
            return this;
        }

        public Builder parallel(ExecutorService es, int concurrency, TerminationFlag flag) {
            this.executorService = es;
            this.concurrency = Pools.allowedConcurrency(concurrency);
            this.terminationFlag = flag;
            return this;
        }

        public Exporter build() {
            ProgressLogger progressLogger = loggerAdapter == null
                    ? ProgressLogger.NULL_LOGGER
                    : loggerAdapter;
            TerminationFlag flag = terminationFlag == null
                    ? TerminationFlag.RUNNING_TRUE
                    : terminationFlag;
            return new Exporter(db, nodeCount, toOriginalId, flag, progressLogger, concurrency, executorService);
        }
    }

    public interface WriteConsumer {
        void accept(Write ops, long value) throws KernelException;
    }

    public interface PropertyWriteConsumer {
        void accept(Write ops, int relationshipId, int propertyId) throws KernelException;
    }

    private Exporter(
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
        final int propertyId = getOrCreatePropertyId(property);
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
        final int propertyId1 = getOrCreatePropertyId(property1);
        if (propertyId1 == -1) {
            throw new IllegalStateException("no write property id is set");
        }
        final int propertyId2 = getOrCreatePropertyId(property2);
        if (propertyId2 == -1) {
            throw new IllegalStateException("no write property id is set");
        }
        if (ParallelUtil.canRunInParallel(executorService)) {
            writeParallel(propertyId1, data1, translator1, propertyId2, data2, translator2);
        } else {
            writeSequential(propertyId1, data1, translator1, propertyId2, data2, translator2);
        }
    }

    public void write(String property, IntFunction<WriteConsumer> createWriter) {
        final int propertyId = getOrCreatePropertyId(property);
        if (propertyId == -1) {
            throw new IllegalStateException("no write property id is set");
        }
        final WriteConsumer writer = createWriter.apply(propertyId);
        if (ParallelUtil.canRunInParallel(executorService)) {
            writeParallel(writer);
        } else {
            writeSequential(writer);
        }
    }

    public void writeRelationships(String relationship, WriteConsumer writer) {
        final int propertyId = getOrCreateRelationshipId(relationship);
        if (propertyId == -1) {
            throw new IllegalStateException("no write property id is set");
        }
        acceptInTransaction(stmt -> {
            Write write = stmt.dataWrite();
            writer.accept(write, propertyId);
        });
    }

    public void writeRelationshipAndProperty(String relationship, String property, PropertyWriteConsumer writer) {
        final int relationshipId = getOrCreateRelationshipId(relationship);
        final int propertyId = getOrCreatePropertyId(property);
        if (relationshipId == -1) {
            throw new IllegalStateException("no write property id is set");
        }
        acceptInTransaction(stmt -> writer.accept(stmt.dataWrite(), relationshipId, propertyId));
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
                if (progress % 10_000 == 0) {
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
        final long batchSize = ParallelUtil.adjustBatchSize(
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
                        for (long j = start; j < end; j++) {
                            writer.accept(ops, j);

                            // Only log every 10_000 written nodes
                            // add +1 to avoid logging on the first written node
                            if (((j + 1) - start) % 10_000 == 0) {
                                long currentProgress = progress.addAndGet(10_000);
                                progressLogger.logProgress(
                                        currentProgress,
                                        nodeCount);
                                terminationFlag.assertRunning();
                            }
                        }

                        // log progress for the last batch of written nodes
                        progressLogger.logProgress(
                                progress.addAndGet((end - start + 1) % 10_000),
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

    private int getOrCreatePropertyId(String propertyName) {
        return applyInTransaction(stmt -> stmt
                .tokenWrite()
                .propertyKeyGetOrCreateForName(propertyName));
    }

    private int getOrCreateRelationshipId(String propertyName) {
        return applyInTransaction(stmt -> stmt
                .tokenWrite()
                .relationshipTypeGetOrCreateForName(propertyName));
    }
}
