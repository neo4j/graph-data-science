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
import org.neo4j.graphalgo.core.utils.partition.PartitionUtils;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

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

    public static void validateRelationshipWeightPropertyValue(Graph graph, int concurrency, ExecutorService executorService) {
        if (!graph.hasRelationshipProperty()) {
            throw new IllegalStateException("Expected a weighted graph");
        }

        var tasks = PartitionUtils.degreePartition(
            graph,
            concurrency,
            partition -> (Runnable) () -> {
                var concurrentGraph = graph.concurrentCopy();
                partition.consume(nodeId -> {
                    concurrentGraph.forEachRelationship(
                        nodeId,
                        Double.NaN,
                        (sourceNodeId, targetNodeId, property) -> {
                            if (Double.isNaN(property)) {
                                throw new RuntimeException(
                                    formatWithLocale("Found a relationship between %d and %d with no specified weight. Consider using `defaultValue` when loading the graph.",
                                        graph.toOriginalNodeId(sourceNodeId),
                                        graph.toOriginalNodeId(targetNodeId))
                                );
                            }
                            return true;
                        }
                    );
                });
            },
            Optional.empty()
        );

        ParallelUtil.run(tasks, executorService);
    }
}
