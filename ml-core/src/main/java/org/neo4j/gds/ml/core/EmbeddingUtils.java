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
package org.neo4j.gds.ml.core;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.partition.Partition;
import org.neo4j.graphalgo.core.utils.partition.PartitionUtils;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.DoublePredicate;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public final class EmbeddingUtils {

    private EmbeddingUtils() {}

    public static double[] getCheckedDoubleArrayNodeProperty(Graph graph, String propertyKey, long nodeId) {
        var propertyValue = graph.nodeProperties(propertyKey).doubleArrayValue(nodeId);
        if (propertyValue == null) {
            throw new IllegalArgumentException(formatWithLocale(
                "Missing node property for property key `%s` on node with id `%s`. Consider using a default value in the property projection.",
                propertyKey,
                graph.toOriginalNodeId(nodeId)
            ));
        }
        return propertyValue;
    }

    public static long[] getCheckedLongArrayNodeProperty(Graph graph, String propertyKey, long nodeId) {
        var propertyValue = graph.nodeProperties(propertyKey).longArrayValue(nodeId);
        if (propertyValue == null) {
            throw new IllegalArgumentException(formatWithLocale(
                "Missing node property for property key `%s` on node with id `%s`. Consider using a default value in the property projection.",
                propertyKey,
                graph.toOriginalNodeId(nodeId)
            ));
        }
        return propertyValue;
    }

    public static long[] getCheckedLongArrayNodeProperty(Graph graph, String propertyKey, long nodeId, int expectedLength) {
        var propertyValue = getCheckedLongArrayNodeProperty(graph, propertyKey, nodeId);
        if (propertyValue.length != expectedLength) {
            throw new IllegalArgumentException(formatWithLocale(
                "The property `%s` contains arrays of differing lengths `%s` and `%s`.",
                propertyKey,
                propertyValue.length,
                expectedLength
            ));
        }
        return propertyValue;
    }

    public static void validateRelationshipWeightPropertyValue(
        Graph graph,
        int concurrency,
        ExecutorService executorService
    ) {
        validateRelationshipWeightPropertyValue(graph, concurrency, weight -> !Double.isNaN(weight), "Consider using `defaultValue` when loading the graph.", executorService);
    }

    public static void validateRelationshipWeightPropertyValue(Graph graph, int concurrency, DoublePredicate validator, String errorDetails, ExecutorService executorService) {
        if (!graph.hasRelationshipProperty()) {
            throw new IllegalStateException("Expected a weighted graph");
        }

        ThreadLocal<Graph> concurrentGraph = ThreadLocal.withInitial(graph::concurrentCopy);
        var tasks = PartitionUtils.degreePartition(
            graph,
            concurrency,
            partition -> new RelationshipValidator(concurrentGraph, partition, validator, errorDetails),
            Optional.empty()
        );

        ParallelUtil.runWithConcurrency(concurrency, tasks, executorService);
    }

    private static class RelationshipValidator implements Runnable {

        private final ThreadLocal<Graph> concurrentGraph;
        private final Partition partition;
        private final DoublePredicate validator;
        private String errorDetails;

        RelationshipValidator(
            ThreadLocal<Graph> concurrentGraph,
            Partition partition,
            DoublePredicate validator,
            String errorDetails
        ) {
            this.concurrentGraph = concurrentGraph;
            this.partition = partition;
            this.validator = validator;
            this.errorDetails = errorDetails;
        }

        @Override
        public void run() {
            var partitionLocalGraph = concurrentGraph.get();
            partition.consume(nodeId -> {
                partitionLocalGraph.forEachRelationship(
                    nodeId,
                    Double.NaN,
                    (sourceNodeId, targetNodeId, property) -> {
                        if (!validator.test(property)) {
                            throw new RuntimeException(
                                formatWithLocale(
                                    "Found an invalid relationship between %d and %d with the property value of %f. %s",
                                    partitionLocalGraph.toOriginalNodeId(sourceNodeId),
                                    partitionLocalGraph.toOriginalNodeId(targetNodeId),
                                    property,
                                    errorDetails
                                )
                            );
                        }
                        return true;
                    }
                );
            });

        }
    }
}
