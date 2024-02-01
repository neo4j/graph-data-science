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


import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.CompositeRelationshipIterator;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.partition.DegreePartition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.gds.utils.ExceptionUtil;
import org.neo4j.gds.utils.StatementApi;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;

public class NativeRelationshipPropertiesExporter extends StatementApi implements RelationshipPropertiesExporter {

    private final GraphStore graphStore;

    private final RelationshipPropertyTranslator propertyTranslator;
    private final ProgressTracker progressTracker;

    private final int concurrency;
    private final long batchSize;

    private final TerminationFlag terminationFlag;

    NativeRelationshipPropertiesExporter(
        TransactionContext tx,
        GraphStore graphStore,
        RelationshipPropertyTranslator propertyTranslator,
        int concurrency,
        long batchSize,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        super(tx);
        this.graphStore = graphStore;
        this.propertyTranslator = propertyTranslator;
        this.concurrency = concurrency;
        this.batchSize = batchSize;
        this.progressTracker = progressTracker;
        this.terminationFlag = terminationFlag;
    }

    @Override
    public void write(
        String relationshipType,
        List<String> propertyKeys
    ) {
        var graph = graphStore.getGraph(RelationshipType.of(relationshipType));
        var relationshipToken = getOrCreateRelationshipToken(relationshipType);
        var propertyTokens = propertyKeys.stream()
            .map(this::getOrCreatePropertyToken)
            .collect(Collectors.toList());

        var relationshipIterator = graphStore.getCompositeRelationshipIterator(
            RelationshipType.of(relationshipType),
            propertyKeys
        );

        // We use MIN_BATCH_SIZE since writing relationships
        // is performed batch-wise, but single-threaded.
        var tasks = PartitionUtils.degreePartitionWithBatchSize(
            graph,
            batchSize,
            partition -> createBatchRunnable(
                relationshipToken,
                propertyTokens,
                partition,
                relationshipIterator.concurrentCopy(),
                graph::toOriginalNodeId
            )
        );

        progressTracker.beginSubTask();
        try {
            RunWithConcurrency
                .builder()
                .concurrency(concurrency)
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
        List<Integer> propertyTokens,
        DegreePartition partition,
        CompositeRelationshipIterator relationshipIterator,
        LongUnaryOperator toOriginalId
    ) {

        return () -> acceptInTransaction(stmt -> {
            terminationFlag.assertRunning();
            var ops = stmt.dataWrite();

            var writeConsumer = new WriteConsumer(toOriginalId, ops, propertyTranslator, relationshipToken, propertyTokens, progressTracker);

            partition.consume(nodeId -> {
                relationshipIterator.forEachRelationship(nodeId, writeConsumer);
            });

        });
    }

    private static final class WriteConsumer implements CompositeRelationshipIterator.RelationshipConsumer {

        @FunctionalInterface
        interface RelationshipWriteBehavior {
            void apply(long sourceNodeId, long targetNodeId, double[] properties) throws EntityNotFoundException, ConstraintValidationException;
        }

        private final LongUnaryOperator toOriginalId;
        private final Write ops;
        private final RelationshipPropertyTranslator propertyTranslator;
        private final int relationshipToken;
        private final List<Integer> propertyTokens;
        private final ProgressTracker progressTracker;
        private final RelationshipWriteBehavior relationshipWriteBehavior;

        private WriteConsumer(
            LongUnaryOperator toOriginalId,
            Write ops,
            RelationshipPropertyTranslator propertyTranslator,
            int relationshipToken,
            List<Integer> propertyTokens,
            ProgressTracker progressTracker
        ) {
            this.toOriginalId = toOriginalId;
            this.ops = ops;
            this.propertyTranslator = propertyTranslator;
            this.relationshipToken = relationshipToken;
            this.propertyTokens = propertyTokens;
            this.progressTracker = progressTracker;
            this.relationshipWriteBehavior = this::write;
        }

        @Override
        public boolean consume(long sourceNodeId, long targetNodeId, double[] properties) {
            try {
                relationshipWriteBehavior.apply(sourceNodeId, targetNodeId, properties);
                return true;
            } catch (Exception e) {
                ExceptionUtil.throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        }

        private void write(
            long source,
            long target,
            double[] properties
        ) throws EntityNotFoundException, ConstraintValidationException {
            var relationshipId = ops.relationshipCreate(
                toOriginalId.applyAsLong(source),
                relationshipToken,
                toOriginalId.applyAsLong(target)
            );

            for (int propertyIdx = 0; propertyIdx < properties.length; propertyIdx++) {
                ops.relationshipSetProperty(
                    relationshipId,
                    propertyTokens.get(propertyIdx),
                    propertyTranslator.toValue(properties[propertyIdx])
                );
            }

            progressTracker.logProgress();
        }
    }
}
