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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.DoublePredicate;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

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
        Concurrency concurrency,
        ExecutorService executorService
    ) {
        validateRelationshipWeightPropertyValue(graph, concurrency, weight -> !Double.isNaN(weight), "Consider using `defaultValue` when loading the graph.", executorService);
    }

    public static void validateRelationshipWeightPropertyValue(Graph graph, Concurrency concurrency, DoublePredicate validator, String errorDetails, ExecutorService executorService) {
        if (!graph.hasRelationshipProperty()) {
            throw new IllegalStateException("Expected a weighted graph");
        }

        var tasks = PartitionUtils.degreePartition(
            graph,
            concurrency.value(),
            partition -> new RelationshipValidator(graph, partition, validator, errorDetails),
            Optional.empty()
        );

        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(tasks)
            .executor(executorService)
            .run();
    }

    private static class RelationshipValidator implements Runnable {

        private final Graph graph;
        private final Partition partition;
        private final DoublePredicate validator;
        private final String errorDetails;

        RelationshipValidator(
            Graph graph,
            Partition partition,
            DoublePredicate validator,
            String errorDetails
        ) {
            this.graph = graph;
            this.partition = partition;
            this.validator = validator;
            this.errorDetails = errorDetails;
        }

        @Override
        public void run() {
            var partitionLocalGraph = graph.concurrentCopy();
            partition.consume(nodeId -> partitionLocalGraph.forEachRelationship(
                nodeId,
                Double.NaN,
                (sourceNodeId, targetNodeId, property) -> {
                    if (!validator.test(property)) {
                        throw new IllegalStateException(
                            formatWithLocale(
                                "Found an invalid relationship weight between nodes `%d` and `%d` with the property value of `%f`. %s",
                                partitionLocalGraph.toOriginalNodeId(sourceNodeId),
                                partitionLocalGraph.toOriginalNodeId(targetNodeId),
                                property,
                                errorDetails
                            )
                        );
                    }
                    return true;
                }
            ));

        }
    }
}
