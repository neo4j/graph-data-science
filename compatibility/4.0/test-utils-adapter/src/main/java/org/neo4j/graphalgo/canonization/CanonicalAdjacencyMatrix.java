/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.canonization;

import org.apache.commons.compress.utils.Lists;
import org.neo4j.graphalgo.annotation.IdenticalCompat;
import org.neo4j.graphalgo.api.Graph;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

@IdenticalCompat
public final class CanonicalAdjacencyMatrix {

    private CanonicalAdjacencyMatrix() {}

    public static String canonicalize(Graph g) {
        // canonical nodes
        Map<Long, String> canonicalNodeLabels = new HashMap<>();
        g.forEachNode(nodeId -> {
            String canonicalNodeLabel = g.availableNodeProperties().stream()
                    .map(propertyKey -> String.format(
                            "%s: %f",
                            propertyKey,
                            g.nodeProperties(propertyKey).nodeProperty(nodeId)))
                    .sorted()
                    .collect(Collectors.joining(", ", "({", "})"));
            canonicalNodeLabels.put(nodeId, canonicalNodeLabel);
            return true;
        });

        Map<Long, List<String>> outAdjacencies = new HashMap<>();
        Map<Long, List<String>> inAdjacencies = new HashMap<>();
        g.forEachNode(nodeId -> {
            g.forEachRelationship(nodeId, 1.0, (sourceId, targetId, propertyValue) -> {
                outAdjacencies.compute(
                        sourceId,
                        canonicalRelationship(canonicalNodeLabels.get(targetId), propertyValue, "()-[w: %f]->%s"));
                inAdjacencies.compute(
                        targetId,
                        canonicalRelationship(canonicalNodeLabels.get(sourceId), propertyValue, "()<-[w: %f]-%s"));
                return true;
            });
            return true;
        });
        Map<Long, String> canonicalOutAdjacencies = canonicalAdjacencies(outAdjacencies);
        Map<Long, String> canonicalInAdjacencies = canonicalAdjacencies(inAdjacencies);

        // canonical matrix
        return canonicalNodeLabels.entrySet().stream()
                .map(entry -> String.format(
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
                        entry -> entry.getValue().stream().sorted().collect(Collectors.joining(", "))));
    }

    private static BiFunction<Long, List<String>, List<String>> canonicalRelationship(
            String canonicalNodeLabel,
            double relationshipProperty,
            String pattern) {
        return (unused, list) -> {
            if (list == null) {
                list = Lists.newArrayList();
            }
            list.add(String.format(pattern, relationshipProperty, canonicalNodeLabel));
            return list;
        };
    }
}
