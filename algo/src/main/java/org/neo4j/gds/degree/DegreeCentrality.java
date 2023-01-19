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
package org.neo4j.gds.degree;

import org.apache.commons.lang3.mutable.MutableDouble;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.RelationshipIterator;
import org.neo4j.gds.api.RelationshipWithPropertyConsumer;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class DegreeCentrality extends Algorithm<DegreeCentrality.DegreeFunction> {

    private static final double DEFAULT_WEIGHT = 0D;

    private Graph graph;
    private final ExecutorService executor;
    private final DegreeCentralityConfig config;

    public interface DegreeFunction {
        double get(long nodeId);
    }

    public DegreeCentrality(
        Graph graph,
        ExecutorService executor,
        DegreeCentralityConfig config,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.executor = executor;
        this.config = config;
    }

    @Override
    public DegreeFunction compute() {
        progressTracker.beginSubTask();

        var result = config.hasRelationshipWeightProperty()
            ? computeWeighted()
            : computeUnweighted();

        progressTracker.endSubTask();
        return result;
    }

    private DegreeFunction computeUnweighted() {
        switch (config.orientation()) {
            case NATURAL:
                progressTracker.logProgress(graph.nodeCount());
                return graph::degree;
            case REVERSE:
                return computeDegreeAtomic((partition, degrees) -> new ReverseDegreeTask(
                        graph.concurrentCopy(),
                        partition,
                        progressTracker,
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
                        progressTracker
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
                    progressTracker
                ));
            case REVERSE:
                return computeDegreeAtomic((partition, degrees) -> new ReverseDegreeTask(
                        graph.concurrentCopy(),
                        partition,
                        progressTracker,
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
                    progressTracker
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
        var degrees = HugeDoubleArray.newArray(graph.nodeCount());
        var tasks = PartitionUtils.degreePartition(
            graph,
            config.concurrency(),
            partition -> taskFunction.apply(partition, degrees),
            Optional.of(config.minBatchSize())
        );
        RunWithConcurrency.builder()
            .concurrency(config.concurrency())
            .tasks(tasks)
            .executor(executor)
            .run();
        return degrees::get;
    }

    private DegreeFunction computeDegreeAtomic(TaskFunctionAtomic taskFunction) {
        var degrees = HugeAtomicDoubleArray.newArray(graph.nodeCount());
        var tasks = PartitionUtils.degreePartition(
            graph,
            config.concurrency(),
            partition -> taskFunction.apply(partition, degrees),
            Optional.of(config.minBatchSize())
        );
        RunWithConcurrency.builder()
            .concurrency(config.concurrency())
            .tasks(tasks)
            .executor(executor)
            .run();
        return degrees::get;
    }

    private static class NaturalWeightedDegreeTask implements Runnable {

        private final HugeDoubleArray result;
        private final RelationshipIterator relationshipIterator;
        private final Partition partition;
        private final ProgressTracker progressTracker;

        NaturalWeightedDegreeTask(
            RelationshipIterator relationshipIterator,
            HugeDoubleArray result,
            Partition partition,
            ProgressTracker progressTracker
        ) {
            this.relationshipIterator = relationshipIterator;
            this.result = result;
            this.partition = partition;
            this.progressTracker = progressTracker;
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
            progressTracker.logProgress(partition.nodeCount());
        }
    }

    private static class ReverseDegreeTask implements Runnable {

        private final Graph graph;
        private final Partition partition;
        private final ProgressTracker progressTracker;
        private final RelationshipWithPropertyConsumer consumer;

        ReverseDegreeTask(
            Graph graph,
            Partition partition,
            ProgressTracker progressTracker,
            RelationshipWithPropertyConsumer consumer
        ) {
            this.graph = graph;
            this.partition = partition;
            this.progressTracker = progressTracker;
            this.consumer = consumer;
        }

        @Override
        public void run() {
            partition.consume(node -> graph.forEachRelationship(node, DEFAULT_WEIGHT, consumer));
            progressTracker.logProgress(partition.nodeCount());
        }
    }

    private static class UndirectedDegreeTask implements Runnable {

        private final Graph graph;
        private final Partition partition;
        private final HugeAtomicDoubleArray degrees;
        private final ProgressTracker progressTracker;

        UndirectedDegreeTask(
            Graph graph,
            Partition partition,
            HugeAtomicDoubleArray degrees,
            ProgressTracker progressTracker
        ) {
            this.graph = graph;
            this.partition = partition;
            this.degrees = degrees;
            this.progressTracker = progressTracker;
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
            progressTracker.logProgress(partition.nodeCount());
        }
    }

    private static class UndirectedWeightedDegreeTask implements Runnable {

        private final Graph graph;
        private final Partition partition;
        private final HugeAtomicDoubleArray degrees;
        private final ProgressTracker progressTracker;

        UndirectedWeightedDegreeTask(
            Graph graph,
            Partition partition,
            HugeAtomicDoubleArray degrees,
            ProgressTracker progressTracker
        ) {
            this.graph = graph;
            this.partition = partition;
            this.degrees = degrees;
            this.progressTracker = progressTracker;
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
            progressTracker.logProgress(partition.nodeCount());
        }
    }
}
