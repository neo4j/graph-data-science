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
package org.neo4j.graphalgo.degree;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.partition.Partition;
import org.neo4j.graphalgo.core.utils.partition.PartitionUtils;

import java.util.concurrent.ExecutorService;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class DegreeCentrality extends Algorithm<DegreeCentrality, DegreeCentrality.DegreeFunction> {

    private static final double DEFAULT_WEIGHT = 0D;

    private Graph graph;
    private final ExecutorService executor;
    private final DegreeCentralityConfig config;
    private final AllocationTracker tracker;

    public interface DegreeFunction {
        double get(long nodeId);
    }

    public DegreeCentrality(
        Graph graph,
        ExecutorService executor,
        DegreeCentralityConfig config,
        ProgressLogger progressLogger,
        AllocationTracker tracker
    ) {
        this.graph = graph;
        this.executor = executor;
        this.config = config;
        this.progressLogger = progressLogger;
        this.tracker = tracker;
    }

    @Override
    public DegreeFunction compute() {
        progressLogger.logStart();

        DegreeFunction result;
        if (config.hasRelationshipWeightProperty()) {
            var degrees = HugeDoubleArray.newArray(graph.nodeCount(), tracker);
            var degreeBatchSize = BitUtil.ceilDiv(graph.relationshipCount(), config.concurrency());
            var tasks = PartitionUtils.degreePartition(
                graph,
                degreeBatchSize,
                partition -> new WeightedDegreeTask(graph.concurrentCopy(), degrees, partition, progressLogger)
            );
            ParallelUtil.runWithConcurrency(config.concurrency(), tasks, executor);
            result = degrees::get;
        } else {
            switch (config.orientation()) {
                case NATURAL:
                    result = graph::degree;
                    progressLogger.logProgress(graph.nodeCount());
                    break;
                case REVERSE:
                    var degrees = HugeAtomicLongArray.newArray(graph.nodeCount(), tracker);
                    var degreeBatchSize = BitUtil.ceilDiv(graph.relationshipCount(), config.concurrency());
                    var tasks = PartitionUtils.degreePartition(
                        graph,
                        degreeBatchSize,
                        partition -> new IndegreeTask(graph.concurrentCopy(), partition, degrees, progressLogger)
                    );
                    ParallelUtil.runWithConcurrency(config.concurrency(), tasks, executor);
                    result = degrees::get;
                    break;
                case UNDIRECTED:
                    throw new NotImplementedException();
                default:
                    throw new IllegalArgumentException(formatWithLocale(
                        "Orientation %s is not supported",
                        config.orientation()
                    ));
            }
        }

        progressLogger.logFinish();
        return result;
    }

    @Override
    public DegreeCentrality me() {
        return this;
    }

    @Override
    public void release() {
        graph = null;
    }

    private static class IndegreeTask implements Runnable {

        private final Graph graph;
        private final Partition partition;
        private final HugeAtomicLongArray degrees;
        private final ProgressLogger progressLogger;

        IndegreeTask(
            Graph graph,
            Partition partition,
            HugeAtomicLongArray degrees,
            ProgressLogger progressLogger
        ) {
            this.graph = graph;
            this.partition = partition;
            this.degrees = degrees;
            this.progressLogger = progressLogger;
        }

        @Override
        public void run() {
            partition.consume(node -> {
                graph.forEachRelationship(node, (sourceNodeId, targetNodeId) -> {
                    degrees.getAndAdd(targetNodeId, 1);
                    return true;
                });
            });
            progressLogger.logProgress(partition.nodeCount());
        }
    }

    private static class WeightedDegreeTask implements Runnable {

        private final HugeDoubleArray result;
        private final RelationshipIterator relationshipIterator;
        private final Partition partition;
        private final ProgressLogger progressLogger;

        WeightedDegreeTask(
            RelationshipIterator relationshipIterator,
            HugeDoubleArray result,
            Partition partition,
            ProgressLogger progressLogger
        ) {
            this.relationshipIterator = relationshipIterator;
            this.result = result;
            this.partition = partition;
            this.progressLogger = progressLogger;
        }

        @Override
        public void run() {
            partition.consume(nodeId -> {
                MutableDouble nodeWeight = new MutableDouble(0.0D);
                relationshipIterator.forEachRelationship(nodeId, DEFAULT_WEIGHT, (sourceNodeId, targetNodeId, weight) -> {
                    if (weight > 0.0D) {
                        nodeWeight.add(weight);
                    }
                    return true;
                });
                result.set(nodeId, nodeWeight.doubleValue());
            });
            progressLogger.logProgress(partition.nodeCount());
        }
    }
}
