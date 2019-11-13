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

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.api.RelationshipWithPropertyConsumer;
import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.StatementApi;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.storable.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.singletonList;
import static org.neo4j.graphalgo.core.write.NodeExporter.MIN_BATCH_SIZE;

public final class RelationshipExporter extends StatementApi {

    private final Graph graph;
    private final Direction readDirection;
    private final long nodeCount;
    private final TerminationFlag terminationFlag;
    private final ProgressLogger progressLogger;
    private final ExecutorService executorService;

    public static RelationshipExporter.Builder of(
        GraphDatabaseAPI db,
        Graph graph,
        Direction readDirection,
        TerminationFlag terminationFlag
    ) {
        return new RelationshipExporter.Builder(
            db,
            graph,
            readDirection,
            terminationFlag
        );
    }

    public static final class Builder extends ExporterBuilder<RelationshipExporter> {

        private final Graph graph;
        private final Direction readDirection;

        Builder(
            GraphDatabaseAPI db,
            Graph graph,
            Direction readDirection,
            TerminationFlag terminationFlag
        ) {
            super(db, graph, terminationFlag);
            this.graph = graph;
            this.readDirection = readDirection;
        }

        @Override
        public RelationshipExporter build() {
            ProgressLogger progressLogger = loggerAdapter == null
                ? ProgressLogger.NULL_LOGGER
                : loggerAdapter;

            return new RelationshipExporter(
                db,
                graph,
                readDirection,
                terminationFlag,
                progressLogger,
                executorService
            );
        }
    }

    private RelationshipExporter(
        GraphDatabaseAPI db,
        Graph graph,
        Direction readDirection,
        TerminationFlag terminationFlag,
        ProgressLogger progressLogger,
        ExecutorService executorService
    ) {
        super(db);
        this.nodeCount = graph.nodeCount();
        this.graph = graph;
        this.readDirection = readDirection;
        this.terminationFlag = terminationFlag;
        this.progressLogger = progressLogger;
        this.executorService = executorService;
    }

    public void write(String relationshipType, String propertyKey, double fallbackValue) {
        final AtomicLong progress = new AtomicLong(0L);

        final int relationshipToken = getOrCreateRelationshipToken(relationshipType);
        final int propertyToken = getOrCreatePropertyToken(propertyKey);

        // We use MIN_BATCH_SIZE since writing relationships
        // is performed batch-wise, but single-threaded.
        degreePartitionGraph(MIN_BATCH_SIZE, readDirection)
            .stream()
            .map(partition -> createBatchRunnable(
                fallbackValue,
                readDirection,
                progress,
                relationshipToken,
                propertyToken,
                partition.startNode,
                partition.nodeCount
            ))
            .forEach(this::writeBatch);
    }

    private Runnable createBatchRunnable(
        double fallbackValue,
        Direction direction,
        AtomicLong progress,
        int relationshipToken,
        int propertyToken,
        long start,
        long length
    ) {
        return () -> acceptInTransaction(stmt -> {
            terminationFlag.assertRunning();
            long end = start + length;
            Write ops = stmt.dataWrite();
            WriteConsumer writeConsumer = new WriteConsumer(graph, ops, relationshipToken, propertyToken);
            RelationshipIterator relationshipIterator = graph.concurrentCopy();
            for (long currentNode = start; currentNode < end; currentNode++) {
                relationshipIterator.forEachRelationship(currentNode, direction, fallbackValue, writeConsumer);

                // Only log after writing relationships for 10_000 nodes
                // add +1 to avoid logging on the first written node
                if (((currentNode + 1) - start) % TerminationFlag.RUN_CHECK_NODE_COUNT == 0) {
                    long currentProgress = progress.addAndGet(TerminationFlag.RUN_CHECK_NODE_COUNT);
                    progressLogger.logProgress(
                        currentProgress,
                        nodeCount
                    );
                    terminationFlag.assertRunning();
                }
            }

            // log progress for the last batch of written relationships
            progressLogger.logProgress(
                progress.addAndGet((end - start + 1) % TerminationFlag.RUN_CHECK_NODE_COUNT),
                nodeCount
            );
        });
    }

    private void writeBatch(Runnable batch) {
        ParallelUtil.run(singletonList(batch), true, executorService, null);
    }

    private List<Partition> degreePartitionGraph(long batchSize, Direction direction) {
        PrimitiveLongIterator nodes = graph.nodeIterator();
        List<Partition> partitions = new ArrayList<>();
        long start = 0L;
        while (nodes.hasNext()) {
            Partition partition = new Partition(
                nodes,
                graph,
                direction,
                start,
                batchSize
            );
            partitions.add(partition);
            start += partition.nodeCount;
        }
        return partitions;
    }

    private static final class Partition {

        // rough estimate of what capacity would still yield acceptable performance
        // per thread
        private static final int MAX_NODE_COUNT = (Integer.MAX_VALUE - 32) >> 1;

        private final long startNode;
        private final int nodeCount;

        Partition(
            PrimitiveLongIterator nodes,
            Degrees degrees,
            Direction direction,
            long startNode,
            long batchSize
        ) {
            assert batchSize > 0L;
            int nodeCount = 0;
            long partitionSize = 0L;
            while (nodes.hasNext() && partitionSize < batchSize && nodeCount < MAX_NODE_COUNT) {
                long nodeId = nodes.next();
                ++nodeCount;
                partitionSize += degrees.degree(nodeId, direction);
            }
            this.startNode = startNode;
            this.nodeCount = nodeCount;
        }
    }

    private static class WriteConsumer implements RelationshipWithPropertyConsumer {

        private final IdMapping idMapping;
        private final Write ops;
        private final int relTypeToken;
        private final int propertyToken;

        WriteConsumer(IdMapping idMapping, Write ops, int relTypeToken, int propertyToken) {
            this.idMapping = idMapping;
            this.ops = ops;
            this.relTypeToken = relTypeToken;
            this.propertyToken = propertyToken;
        }

        @Override
        public boolean accept(long sourceNodeId, long targetNodeId, double property) {
            try {
                final long relId = ops.relationshipCreate(
                    idMapping.toOriginalNodeId(sourceNodeId),
                    relTypeToken,
                    idMapping.toOriginalNodeId(targetNodeId)
                );
                ops.relationshipSetProperty(
                    relId,
                    propertyToken,
                    Values.doubleValue(property)
                );
            } catch (KernelException e) {
                ExceptionUtil.throwKernelException(e);
            }
            return true;
        }
    }
}
