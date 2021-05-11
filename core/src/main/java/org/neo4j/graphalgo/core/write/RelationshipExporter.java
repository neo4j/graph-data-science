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
package org.neo4j.graphalgo.core.write;

import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.api.RelationshipWithPropertyConsumer;
import org.neo4j.graphalgo.core.SecureTransaction;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.partition.Partition;
import org.neo4j.graphalgo.core.utils.partition.PartitionUtils;
import org.neo4j.graphalgo.utils.StatementApi;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.values.storable.Values;

import java.util.concurrent.ExecutorService;

import static org.neo4j.graphalgo.core.concurrency.Pools.DEFAULT_SINGLE_THREAD_POOL;
import static org.neo4j.graphalgo.core.write.NodePropertyExporter.MIN_BATCH_SIZE;
import static org.neo4j.graphalgo.utils.ExceptionUtil.throwIfUnchecked;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;

public final class RelationshipExporter extends StatementApi {

    private final Graph graph;
    private final RelationshipPropertyTranslator propertyTranslator;
    private final TerminationFlag terminationFlag;
    private final ProgressLogger progressLogger;
    private final ExecutorService executorService;

    public static RelationshipExporter.Builder of(GraphDatabaseService db, Graph graph, TerminationFlag terminationFlag) {
        return of(SecureTransaction.of(db), graph, terminationFlag);
    }

    public static RelationshipExporter.Builder of(SecureTransaction tx, Graph graph, TerminationFlag terminationFlag) {
        return new RelationshipExporter.Builder(
            tx,
            graph,
            terminationFlag
        );
    }

    public static final class Builder extends ExporterBuilder<RelationshipExporter> {

        private final Graph graph;
        private RelationshipPropertyTranslator propertyTranslator;

        Builder(SecureTransaction tx, Graph graph, TerminationFlag terminationFlag) {
            super(tx, graph, terminationFlag);
            this.graph = graph;
            this.propertyTranslator = Values::doubleValue;
        }

        public Builder withRelationPropertyTranslator(RelationshipPropertyTranslator propertyTranslator) {
            this.propertyTranslator = propertyTranslator;
            return this;
        }

        @Override
        public RelationshipExporter build() {
            return new RelationshipExporter(
                tx,
                graph,
                propertyTranslator,
                terminationFlag,
                progressLogger
            );
        }

        @Override
        String taskName() {
            return "WriteRelationships";
        }

        @Override
        long taskVolume() {
            return graph.relationshipCount();
        }
    }

    private RelationshipExporter(
        SecureTransaction tx,
        Graph graph,
        RelationshipPropertyTranslator propertyTranslator,
        TerminationFlag terminationFlag,
        ProgressLogger progressLogger
    ) {
        super(tx);
        this.graph = graph;
        this.propertyTranslator = propertyTranslator;
        this.terminationFlag = terminationFlag;
        this.progressLogger = progressLogger;
        this.executorService = DEFAULT_SINGLE_THREAD_POOL;
    }

    public void write(String relationshipType) {
        var relationshipToken = getOrCreateRelationshipToken(relationshipType);
        write(relationshipToken, NO_SUCH_PROPERTY_KEY, null);
    }

    public void write(String relationshipType, @Nullable RelationshipWithPropertyConsumer afterWriteConsumer) {
        var relationshipToken = getOrCreateRelationshipToken(relationshipType);
        write(relationshipToken, NO_SUCH_PROPERTY_KEY, afterWriteConsumer);
    }

    public void write(String relationshipType, String propertyKey) {
        var relationshipTypeToken = getOrCreateRelationshipToken(relationshipType);
        var propertyKeyToken = getOrCreatePropertyToken(propertyKey);
        write(relationshipTypeToken, propertyKeyToken, null);
    }

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
        progressLogger.logStart();
        // We use MIN_BATCH_SIZE since writing relationships
        // is performed batch-wise, but single-threaded.
        PartitionUtils.degreePartitionWithBatchSize(graph, MIN_BATCH_SIZE, partition -> createBatchRunnable(
            relationshipTypeToken,
            propertyKeyToken,
            partition,
            afterWriteConsumer
        ))
            .forEach(runnable -> ParallelUtil.run(runnable, executorService));
        progressLogger.logFinish();
    }

    private Runnable createBatchRunnable(
        int relationshipToken,
        int propertyToken,
        Partition partition,
        @Nullable RelationshipWithPropertyConsumer afterWrite
    ) {
        return () -> acceptInTransaction(stmt -> {
            terminationFlag.assertRunning();
            Write ops = stmt.dataWrite();


            RelationshipWithPropertyConsumer writeConsumer = new WriteConsumer(
                graph,
                ops,
                propertyTranslator,
                relationshipToken,
                propertyToken,
                progressLogger
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
            void apply(long sourceNodeId, long targetNodeId, double property) throws EntityNotFoundException;
        }

        private final IdMapping idMapping;
        private final Write ops;
        private final RelationshipPropertyTranslator propertyTranslator;
        private final int relTypeToken;
        private final int propertyToken;
        private final ProgressLogger progressLogger;
        private final RelationshipWriteBehavior relationshipWriteBehavior;

        WriteConsumer(
            IdMapping idMapping,
            Write ops,
            RelationshipPropertyTranslator propertyTranslator,
            int relTypeToken,
            int propertyToken,
            ProgressLogger progressLogger
        ) {
            this.idMapping = idMapping;
            this.ops = ops;
            this.propertyTranslator = propertyTranslator;
            this.relTypeToken = relTypeToken;
            this.propertyToken = propertyToken;
            this.progressLogger = progressLogger;
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
                throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        }

        private void writeWithoutProperty(long sourceNodeId, long targetNodeId, double property) throws EntityNotFoundException {
            writeRelationship(sourceNodeId, targetNodeId);
            progressLogger.logProgress();
        }

        private void writeWithProperty(long sourceNodeId, long targetNodeId, double property) throws EntityNotFoundException {
            long relId = writeRelationship(sourceNodeId, targetNodeId);
            exportProperty(property, relId);
            progressLogger.logProgress();
        }

        private long writeRelationship(long sourceNodeId, long targetNodeId) throws EntityNotFoundException {
            return ops.relationshipCreate(
                idMapping.toOriginalNodeId(sourceNodeId),
                relTypeToken,
                idMapping.toOriginalNodeId(targetNodeId)
            );
        }

        private void exportProperty(double property, long relId) throws EntityNotFoundException {
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
