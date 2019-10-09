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

package org.neo4j.graphalgo.canonization;

import org.apache.commons.compress.utils.Lists;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class CanonicalAdjacencyMatrix {

    private CanonicalAdjacencyMatrix() {}

    public static String canonicalize(Graph g) {
        // nodes
        Map<Long, String> canonicalNodeLabels = new HashMap<>();
        g.forEachNode(nodeId -> {
            String canonicalNodeLabel = g.availableNodeProperties().stream()
                    .map(propertyKey -> String.format(
                            "%s: %f",
                            propertyKey,
                            g.nodeProperties(propertyKey).nodeWeight(nodeId)))
                    .sorted()
                    .collect(Collectors.joining(", ", "({", "})"));
            canonicalNodeLabels.put(nodeId, canonicalNodeLabel);
            return true;
        });

        // relationships
        Direction direction = (g.getLoadDirection() == Direction.BOTH || g.getLoadDirection() == Direction.OUTGOING)
                ? Direction.OUTGOING
                : Direction.INCOMING;

        Map<Long, List<String>> outAdjacencies = new HashMap<>();
        g.forEachNode(nodeId -> {
            g.forEachRelationship(nodeId, direction, 1.0, (source, target, weight) -> {
                long sourceId = (direction == Direction.OUTGOING) ? source : target;
                long targetId = (direction == Direction.OUTGOING) ? target : source;
                String canonicalRelLabel = String.format("()-[w: %f]->%s", weight, canonicalNodeLabels.get(targetId));
                outAdjacencies.compute(sourceId, (unused, list) -> {
                    if (list == null) {
                        list = Lists.newArrayList();
                    }
                    list.add(canonicalRelLabel);
                    return list;
                });
                return true;
            });
            return true;
        });

        Map<Long, String> canonicalOutAdjacencies = outAdjacencies
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().stream().sorted().collect(Collectors.joining(", "))));

        return canonicalNodeLabels.entrySet().stream()
                .map(entry -> String.format(
                        "%s => %s",
                        entry.getValue(),
                        canonicalOutAdjacencies.getOrDefault(entry.getKey(), "")))
                .sorted()
                .collect(Collectors.joining(System.lineSeparator()));
    }
}
