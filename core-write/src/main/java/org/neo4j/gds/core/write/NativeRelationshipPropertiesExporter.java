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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.CompositeRelationshipIterator;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.partition.DegreePartition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.gds.utils.ExceptionUtil;
import org.neo4j.gds.utils.StatementApi;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;

public class NativeRelationshipPropertiesExporter extends StatementApi implements RelationshipPropertiesExporter {

    private final GraphStore graphStore;

    private final ProgressTracker progressTracker;

    private final ExecutorService executorService;

    private final TerminationFlag terminationFlag;

    public NativeRelationshipPropertiesExporter(
        TransactionContext tx,
        GraphStore graphStore,
        ProgressTracker progressTracker,
        ExecutorService executorService,
        TerminationFlag terminationFlag
    ) {
        super(tx);
        this.graphStore = graphStore;
        this.progressTracker = progressTracker;
        this.executorService = executorService;
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
            NativeNodePropertyExporter.MIN_BATCH_SIZE,
            partition -> createBatchRunnable(
                relationshipToken,
                propertyTokens,
                partition,
                relationshipIterator,
                graph::toOriginalNodeId
            )
        );

        progressTracker.beginSubTask();
        try {
            tasks.forEach(runnable -> ParallelUtil.run(runnable, executorService));
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

            var writeConsumer = new WriteConsumer(toOriginalId, ops, Values::doubleValue, relationshipToken, propertyTokens, progressTracker);

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

        @SuppressFBWarnings(
            value = "BED_BOGUS_EXCEPTION_DECLARATION",
            justification = "`ConstraintValidationException` is actually thrown in 5.2.0"
        )
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
            var propertyValues = new IntObjectHashMap<Value>();
            for (int propertyIdx = 0; propertyIdx < properties.length; propertyIdx++) {
                propertyValues.put(
                    propertyTokens.get(propertyIdx),
                    propertyTranslator.toValue(properties[propertyIdx])
                );
            }
            ops.relationshipApplyChanges(
                relationshipId,
                propertyValues
            );

            progressTracker.logProgress();
        }
    }
}
