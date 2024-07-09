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
package org.neo4j.gds.core.write;


import org.jetbrains.annotations.Nullable;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.RelationshipIterator;
import org.neo4j.gds.api.RelationshipWithPropertyConsumer;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.Write;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.gds.utils.ExceptionUtil;
import org.neo4j.gds.utils.StatementApi;

import java.util.concurrent.TimeUnit;
import java.util.function.LongUnaryOperator;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;

public final class NativeRelationshipExporter extends StatementApi implements RelationshipExporter {

    private final Graph graph;
    private final LongUnaryOperator toOriginalId;
    private final RelationshipPropertyTranslator propertyTranslator;
    private final long batchSize;
    private final TerminationFlag terminationFlag;
    private final ProgressTracker progressTracker;

    public static RelationshipExporterBuilder builder(
        TransactionContext transactionContext,
        Graph graph,
        TerminationFlag terminationFlag
    ) {
        return builder(
            transactionContext,
            graph,
            graph,
            terminationFlag
        );
    }

    public static RelationshipExporterBuilder builder(
        TransactionContext transactionContext,
        IdMap idMap,
        Graph graph,
        TerminationFlag terminationFlag
    ) {
        return new NativeRelationshipExporterBuilder(transactionContext)
            .withGraph(graph)
            .withIdMappingOperator(idMap::toOriginalNodeId)
            .withTerminationFlag(terminationFlag);
    }

    NativeRelationshipExporter(
        TransactionContext transactionContext,
        Graph graph,
        LongUnaryOperator toOriginalId,
        RelationshipPropertyTranslator propertyTranslator,
        long batchSize,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker
    ) {
        super(transactionContext);
        this.graph = graph;
        this.toOriginalId = toOriginalId;
        this.propertyTranslator = propertyTranslator;
        this.batchSize = batchSize;
        this.terminationFlag = terminationFlag;
        this.progressTracker = progressTracker;
    }

    @Override
    public void write(String relationshipType) {
        var relationshipToken = getOrCreateRelationshipToken(relationshipType);
        write(relationshipToken, NO_SUCH_PROPERTY_KEY, null);
    }

    @Override
    public void write(String relationshipType, String propertyKey) {
        var relationshipTypeToken = getOrCreateRelationshipToken(relationshipType);
        var propertyKeyToken = getOrCreatePropertyToken(propertyKey);
        write(relationshipTypeToken, propertyKeyToken, null);
    }

    @Override
    public void write(
        String relationshipType,
        String propertyKey,
        @Nullable RelationshipWithPropertyConsumer afterWriteConsumer
    ) {
        var relationshipTypeToken = getOrCreateRelationshipToken(relationshipType);
        var propertyKeyToken = getOrCreatePropertyToken(propertyKey);
        write(relationshipTypeToken, propertyKeyToken, afterWriteConsumer);
    }

    private void write(int relationshipTypeToken, int propertyKeyToken, @Nullable RelationshipWithPropertyConsumer afterWriteConsumer) {
        var tasks = PartitionUtils.degreePartitionWithBatchSize(
            graph,
            batchSize,
            partition -> createBatchRunnable(
                relationshipTypeToken,
                propertyKeyToken,
                partition,
                afterWriteConsumer
            )
        );

        progressTracker.beginSubTask();
        try {
            RunWithConcurrency
                .builder()
                .concurrency(RelationshipExporterBuilder.TYPED_DEFAULT_WRITE_CONCURRENCY)
                .tasks(tasks)
                .maxWaitRetries(Integer.MAX_VALUE)
                .waitTime(10L, TimeUnit.MICROSECONDS)
                .terminationFlag(terminationFlag)
                .executor(DefaultPool.INSTANCE)
                .mayInterruptIfRunning(false)
                .run();
        } finally {
            progressTracker.endSubTask();
        }
    }

    private Runnable createBatchRunnable(
        int relationshipToken,
        int propertyToken,
        Partition partition,
        @Nullable RelationshipWithPropertyConsumer afterWrite
    ) {
        return () -> acceptInTransaction(stmt -> {
            terminationFlag.assertRunning();
            Write ops = Neo4jProxy.dataWrite(stmt);

            RelationshipWithPropertyConsumer writeConsumer = new WriteConsumer(
                toOriginalId,
                ops,
                propertyTranslator,
                relationshipToken,
                propertyToken,
                progressTracker
            );
            if (afterWrite != null) {
                writeConsumer = writeConsumer.andThen(afterWrite);
            }
            RelationshipIterator relationshipIterator = graph.concurrentCopy();
            RelationshipWithPropertyConsumer finalWriteConsumer = writeConsumer;
            var startNode = partition.startNode();
            partition.consume(nodeId -> {
                relationshipIterator.forEachRelationship(nodeId, Double.NaN, finalWriteConsumer);

                if ((nodeId - startNode) % TerminationFlag.RUN_CHECK_NODE_COUNT == 0) {
                    terminationFlag.assertRunning();
                }
            });
        });
    }

    private static class WriteConsumer implements RelationshipWithPropertyConsumer {
        @FunctionalInterface
        interface RelationshipWriteBehavior {
            void apply(long sourceNodeId, long targetNodeId, double property) throws KernelException;
        }

        private final LongUnaryOperator toOriginalId;
        private final Write ops;
        private final RelationshipPropertyTranslator propertyTranslator;
        private final int relTypeToken;
        private final int propertyToken;
        private final ProgressTracker progressTracker;
        private final RelationshipWriteBehavior relationshipWriteBehavior;

        WriteConsumer(
            LongUnaryOperator toOriginalId,
            Write ops,
            RelationshipPropertyTranslator propertyTranslator,
            int relTypeToken,
            int propertyToken,
            ProgressTracker progressTracker
        ) {
            this.toOriginalId = toOriginalId;
            this.ops = ops;
            this.propertyTranslator = propertyTranslator;
            this.relTypeToken = relTypeToken;
            this.propertyToken = propertyToken;
            this.progressTracker = progressTracker;
            if (propertyToken == NO_SUCH_PROPERTY_KEY) {
                relationshipWriteBehavior = this::writeWithoutProperty;
            } else {
                relationshipWriteBehavior = this::writeWithProperty;
            }
        }

        @Override
        public boolean accept(long sourceNodeId, long targetNodeId, double property) {
            try {
                relationshipWriteBehavior.apply(sourceNodeId, targetNodeId, property);
                return true;
            } catch (Exception e) {
                ExceptionUtil.throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        }

        private void writeWithoutProperty(long sourceNodeId, long targetNodeId, double property) throws
            KernelException {
            writeRelationship(sourceNodeId, targetNodeId);
            progressTracker.logProgress();
        }

        private void writeWithProperty(
            long sourceNodeId,
            long targetNodeId,
            double property
        ) throws KernelException {
            long relId = writeRelationship(sourceNodeId, targetNodeId);
            exportProperty(property, relId);
            progressTracker.logProgress();
        }

        private long writeRelationship(long sourceNodeId, long targetNodeId) throws KernelException {
            return ops.relationshipCreate(
                toOriginalId.applyAsLong(sourceNodeId),
                relTypeToken,
                toOriginalId.applyAsLong(targetNodeId)
            );
        }

        private void exportProperty(double property, long relId) throws KernelException {
            if (!Double.isNaN(property)) {
                ops.relationshipSetProperty(
                    relId,
                    propertyToken,
                    propertyTranslator.toValue(property)
                );
            }
        }
    }
}
