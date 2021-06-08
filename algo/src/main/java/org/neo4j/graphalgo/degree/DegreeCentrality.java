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

import org.apache.commons.lang3.mutable.MutableDouble;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.api.RelationshipWithPropertyConsumer;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.partition.Partition;
import org.neo4j.graphalgo.core.utils.partition.PartitionUtils;

import java.util.concurrent.ExecutorService;
import java.util.function.Function;

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

        var result = config.hasRelationshipWeightProperty()
            ? computeWeighted()
            : computeUnweighted();

        progressLogger.logFinish();
        return result;
    }

    private DegreeFunction computeUnweighted() {
        switch (config.orientation()) {
            case NATURAL:
                progressLogger.logProgress(graph.nodeCount());
                return graph::degree;
            case REVERSE:
                return computeDegreeAtomic((partition, degrees) -> new ReverseDegreeTask(
                        graph.concurrentCopy(),
                        partition,
                        progressLogger,
                        (sourceNodeId, targetNodeId, weight) -> {
                            degrees.getAndAdd(targetNodeId, 1);
                            return true;
                        }
                    )
                );
            case UNDIRECTED:
                return computeDegreeAtomic((partition, degrees) -> new UndirectedDegreeTask(
                        graph.concurrentCopy(),
                        partition,
                        degrees,
                        progressLogger
                    )
                );
            default:
                throw new IllegalArgumentException(formatWithLocale(
                    "Orientation %s is not supported",
                    config.orientation()
                ));
        }
    }

    private DegreeFunction computeWeighted() {
        switch (config.orientation()) {
            case NATURAL:
                return computeDegree((partition, degrees) -> new NaturalWeightedDegreeTask(
                    graph.concurrentCopy(),
                    degrees,
                    partition,
                    progressLogger
                ));
            case REVERSE:
                return computeDegreeAtomic((partition, degrees) -> new ReverseDegreeTask(
                        graph.concurrentCopy(),
                        partition,
                        progressLogger,
                        (sourceNodeId, targetNodeId, weight) -> {
                            if (weight > 0.0D) {
                                degrees.getAndAdd(targetNodeId, weight);
                            }
                            return true;
                        }
                    )
                );
            case UNDIRECTED:
                return computeDegreeAtomic((partition, degrees) -> new UndirectedWeightedDegreeTask(
                    graph.concurrentCopy(),
                    partition,
                    degrees,
                    progressLogger
                ));
            default:
                throw new IllegalArgumentException(formatWithLocale(
                    "Orientation %s is not supported",
                    config.orientation()
                ));
        }
    }

    @FunctionalInterface
    interface TaskFunction {
        Runnable apply(Partition partition, HugeDoubleArray array);
    }

    @FunctionalInterface
    interface TaskFunctionAtomic {
        Runnable apply(Partition partition, HugeAtomicDoubleArray array);
    }

    private DegreeFunction computeDegree(TaskFunction taskFunction) {
        var degrees = HugeDoubleArray.newArray(graph.nodeCount(), tracker);
        var tasks = PartitionUtils.degreePartitionWithMinBatchSize(
            graph,
            config.concurrency(),
            config.minBatchSize(),
            partition -> taskFunction.apply(partition, degrees)
        );
        ParallelUtil.runWithConcurrency(config.concurrency(), tasks, executor);
        return degrees::get;
    }

    private DegreeFunction computeDegreeAtomic(TaskFunctionAtomic taskFunction) {
        var degrees = HugeAtomicDoubleArray.newArray(graph.nodeCount(), tracker);
        var tasks = PartitionUtils.degreePartitionWithMinBatchSize(
            graph,
            config.concurrency(),
            config.minBatchSize(),
            partition -> taskFunction.apply(partition, degrees)
        );
        ParallelUtil.runWithConcurrency(config.concurrency(), tasks, executor);
        return degrees::get;
    }

    @Override
    public DegreeCentrality me() {
        return this;
    }

    @Override
    public void release() {
        graph = null;
    }

    private static class NaturalWeightedDegreeTask implements Runnable {

        private final HugeDoubleArray result;
        private final RelationshipIterator relationshipIterator;
        private final Partition partition;
        private final ProgressLogger progressLogger;

        NaturalWeightedDegreeTask(
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
            var nodeWeight = new MutableDouble();
            partition.consume(nodeId -> {
                nodeWeight.setValue(0);
                relationshipIterator.forEachRelationship(
                    nodeId,
                    DEFAULT_WEIGHT,
                    (sourceNodeId, targetNodeId, weight) -> {
                        if (weight > 0.0D) {
                            nodeWeight.add(weight);
                        }
                        return true;
                    }
                );
                result.set(nodeId, nodeWeight.doubleValue());
            });
            progressLogger.logProgress(partition.nodeCount());
        }
    }

    private static class ReverseDegreeTask implements Runnable {

        private final Graph graph;
        private final Partition partition;
        private final ProgressLogger progressLogger;
        private final RelationshipWithPropertyConsumer consumer;

        ReverseDegreeTask(
            Graph graph,
            Partition partition,
            ProgressLogger progressLogger,
            RelationshipWithPropertyConsumer consumer
        ) {
            this.graph = graph;
            this.partition = partition;
            this.progressLogger = progressLogger;
            this.consumer = consumer;
        }

        @Override
        public void run() {
            partition.consume(node -> graph.forEachRelationship(node, DEFAULT_WEIGHT, consumer));
            progressLogger.logProgress(partition.nodeCount());
        }
    }

    private static class UndirectedDegreeTask implements Runnable {

        private final Graph graph;
        private final Partition partition;
        private final HugeAtomicDoubleArray degrees;
        private final ProgressLogger progressLogger;

        UndirectedDegreeTask(
            Graph graph,
            Partition partition,
            HugeAtomicDoubleArray degrees,
            ProgressLogger progressLogger
        ) {
            this.graph = graph;
            this.partition = partition;
            this.degrees = degrees;
            this.progressLogger = progressLogger;
        }

        @Override
        public void run() {
            Function<Long, Integer> degreeFn = graph::degree;

            partition.consume(node -> {
                // outgoing
                degrees.getAndAdd(node, degreeFn.apply(node));
                // incoming
                graph.forEachRelationship(node, (sourceNodeId, targetNodeId) -> {
                    degrees.getAndAdd(targetNodeId, 1);
                    return true;
                });
            });
            progressLogger.logProgress(partition.nodeCount());
        }
    }

    private static class UndirectedWeightedDegreeTask implements Runnable {

        private final Graph graph;
        private final Partition partition;
        private final HugeAtomicDoubleArray degrees;
        private final ProgressLogger progressLogger;

        UndirectedWeightedDegreeTask(
            Graph graph,
            Partition partition,
            HugeAtomicDoubleArray degrees,
            ProgressLogger progressLogger
        ) {
            this.graph = graph;
            this.partition = partition;
            this.degrees = degrees;
            this.progressLogger = progressLogger;
        }

        @Override
        public void run() {
            var nodeWeight = new MutableDouble();
            partition.consume(node -> {
                nodeWeight.setValue(0);
                graph.forEachRelationship(node, DEFAULT_WEIGHT, ((sourceNodeId, targetNodeId, weight) -> {
                    if (weight > 0.0D) {
                        // outgoing
                        nodeWeight.add(weight);
                        // incoming
                        degrees.getAndAdd(targetNodeId, weight);
                    }
                    return true;
                }));
                degrees.getAndAdd(node, nodeWeight.doubleValue());
            });
            progressLogger.logProgress(partition.nodeCount());
        }
    }
}
