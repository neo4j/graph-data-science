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
import org.neo4j.graphalgo.core.utils.partition.PartitionUtils;
import org.neo4j.graphalgo.utils.StatementApi;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.values.storable.Values;

import java.util.Optional;
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
        write(relationshipType, Optional.empty(), null);
    }

    public void write(String relationshipType, String propertyKey) {
        write(relationshipType, Optional.of(propertyKey), null);
    }

    public void write(String relationshipType, Optional<String> maybePropertyKey) {
        write(relationshipType, maybePropertyKey, null);
    }

    public void write(
        String relationshipType,
        Optional<String> maybePropertyKey,
        @Nullable RelationshipWithPropertyConsumer afterWriteConsumer
    ) {
        final int relationshipToken = getOrCreateRelationshipToken(relationshipType);
        final int propertyKeyToken = maybePropertyKey.map(this::getOrCreatePropertyToken).orElse(NO_SUCH_PROPERTY_KEY);

        progressLogger.logStart();
        // We use MIN_BATCH_SIZE since writing relationships
        // is performed batch-wise, but single-threaded.
        PartitionUtils.degreePartition(graph, MIN_BATCH_SIZE, partition -> createBatchRunnable(
                relationshipToken,
                propertyKeyToken,
                partition.startNode(),
                partition.nodeCount(),
                afterWriteConsumer
            ))
            .forEach(runnable -> ParallelUtil.run(runnable, executorService));
        progressLogger.logFinish();
    }

    private Runnable createBatchRunnable(
        int relationshipToken,
        int propertyToken,
        long start,
        long length,
        @Nullable RelationshipWithPropertyConsumer afterWrite
    ) {
        return () -> acceptInTransaction(stmt -> {
            terminationFlag.assertRunning();
            long end = start + length;
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
            for (long currentNode = start; currentNode < end; currentNode++) {
                relationshipIterator.forEachRelationship(currentNode, Double.NaN, writeConsumer);

                if ((currentNode - start) % TerminationFlag.RUN_CHECK_NODE_COUNT == 0) {
                    terminationFlag.assertRunning();
                }
            }
        });
    }

    private static class WriteConsumer implements RelationshipWithPropertyConsumer {

        private final IdMapping idMapping;
        private final Write ops;
        private final RelationshipPropertyTranslator propertyTranslator;
        private final int relTypeToken;
        private final int propertyToken;
        private final ProgressLogger progressLogger;

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
        }

        @Override
        public boolean accept(long sourceNodeId, long targetNodeId, double property) {
            try {
                long relId = ops.relationshipCreate(
                    idMapping.toOriginalNodeId(sourceNodeId),
                    relTypeToken,
                    idMapping.toOriginalNodeId(targetNodeId)
                );
                progressLogger.logProgress();
                if (!Double.isNaN(property)) {
                    ops.relationshipSetProperty(
                        relId,
                        propertyToken,
                        propertyTranslator.toValue(property)
                    );
                }
            } catch (Exception e) {
                throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
            return true;
        }
    }
}
