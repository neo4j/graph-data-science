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
package org.neo4j.gds.canonization;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static java.util.function.Predicate.not;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.gds.utils.StringJoining.join;

public final class CanonicalAdjacencyMatrix {

    private CanonicalAdjacencyMatrix() {}

    public static String canonicalize(Graph g) {
        // canonical nodes
        Map<Long, String> canonicalLabels = new HashMap<>();
        g.forEachNode(nodeId -> {
            String sortedLabels = g
                .nodeLabels(nodeId)
                .stream()
                .filter(not(NodeLabel.ALL_NODES::equals))
                .map(NodeLabel::name)
                .sorted()
                .collect(Collectors.joining(":"));

            String sortedProperties = g.nodeLabels(nodeId)
                .stream()
                .flatMap(label -> g.schema().nodeSchema().allProperties(label).stream())
                .distinct()
                .map(propertyKey -> {
                    var nodeProperties = g.nodeProperties(propertyKey);

                    switch (nodeProperties.valueType()) {
                        case DOUBLE:
                            return formatWithLocale("%s: %f", propertyKey, nodeProperties.doubleValue(nodeId));
                        case LONG:
                            return formatWithLocale("%s: %d", propertyKey, nodeProperties.longValue(nodeId));
                        case DOUBLE_ARRAY:
                            return formatWithLocale(
                                "%s: %s",
                                propertyKey,
                                Arrays.toString(nodeProperties.doubleArrayValue(nodeId))
                            );
                        case LONG_ARRAY:
                            return formatWithLocale(
                                "%s: %s",
                                propertyKey,
                                Arrays.toString(nodeProperties.longArrayValue(nodeId))
                            );
                        case FLOAT_ARRAY:
                            return formatWithLocale(
                                "%s: %s",
                                propertyKey,
                                Arrays.toString(nodeProperties.floatArrayValue(nodeId))
                            );
                        default:
                            throw new IllegalArgumentException(formatWithLocale(
                                "Unsupported type: %s",
                                nodeProperties.valueType()
                            ));
                    }
                })
                .sorted()
                .collect(Collectors.joining(", "));

            String canonicalNode = formatWithLocale("(%s%s)",
                sortedLabels.isEmpty() ? "" : formatWithLocale(":%s", sortedLabels),
                sortedProperties.isEmpty() ? "" : formatWithLocale(" { %s }", sortedProperties)
            );

            canonicalLabels.put(nodeId, canonicalNode);
            return true;
        });

        Map<Long, List<String>> outAdjacencies = new HashMap<>();
        Map<Long, List<String>> inAdjacencies = new HashMap<>();
        for (RelationshipType relationshipType : g.schema().relationshipSchema().availableTypes()) {
            var relTypeGraph = g.relationshipTypeFilteredGraph(Set.of(relationshipType));
            g.forEachNode(nodeId -> {
                g.forEachRelationship(nodeId, 1.0, (sourceId, targetId, propertyValue) -> {
                    outAdjacencies.compute(
                        sourceId,
                        canonicalRelationship(canonicalLabels.get(targetId), relationshipType.name(), propertyValue, "()-[:%s w: %f]->%s"));
                    inAdjacencies.compute(
                        targetId,
                        canonicalRelationship(canonicalLabels.get(sourceId), relationshipType.name(), propertyValue, "()<-[:%s w: %f]-%s"));
                    return true;
                });
                return true;
            });
        }

        Map<Long, String> canonicalOutAdjacencies = canonicalAdjacencies(outAdjacencies);
        Map<Long, String> canonicalInAdjacencies = canonicalAdjacencies(inAdjacencies);

        // canonical matrix
        return canonicalLabels.entrySet().stream()
            .map(entry -> formatWithLocale(
                "%s => out: %s in: %s",
                entry.getValue(),
                canonicalOutAdjacencies.getOrDefault(entry.getKey(), ""),
                canonicalInAdjacencies.getOrDefault(entry.getKey(), "")))
            .sorted()
            .collect(Collectors.joining(System.lineSeparator()));
    }

    private static Map<Long, String> canonicalAdjacencies(Map<Long, List<String>> outAdjacencies) {
        return outAdjacencies
            .entrySet()
            .stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> join(entry.getValue(), ", ")
            ));
    }

    private static BiFunction<Long, List<String>, List<String>> canonicalRelationship(
        String canonicalNodeLabel,
        String type,
        double relationshipProperty,
        String pattern
    ) {
        return (unused, list) -> {
            if (list == null) {
                list = new ArrayList<>();
            }
            list.add(formatWithLocale(pattern, type, relationshipProperty, canonicalNodeLabel));
            return list;
        };
    }
}
