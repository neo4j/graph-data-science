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

package org.neo4j.graphalgo.equality;

import org.apache.commons.compress.utils.Lists;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class Equality {

    private Equality() {}

    public static boolean equals(Graph g1, Graph g2) {
        return canonicalize(g1).equals(canonicalize(g2));
    }

    public static String canonicalize(Graph g) {
        // nodes
        Map<Long, String> canonicalNodeProperties = new HashMap<>();
        g.forEachNode(nodeId -> {
            String nodeProperties = g.availableNodeProperties().stream()
                    .map(propertyKey -> String.format(
                            "%s: %f",
                            propertyKey,
                            g.nodeProperties(propertyKey).nodeWeight(nodeId)))
                    .sorted()
                    .collect(Collectors.joining(", ", "{", "}"));
            canonicalNodeProperties.put(nodeId, nodeProperties);
            return true;
        });

        // relationships
        Direction direction = (g.getLoadDirection() == Direction.BOTH || g.getLoadDirection() == Direction.OUTGOING)
                ? Direction.OUTGOING
                : Direction.INCOMING;

        Map<Long, List<String>> outEdges = new HashMap<>();
        g.forEachNode(nodeId -> {
            g.forEachRelationship(nodeId, direction, 1.0, (source, target, weight) -> {
                String relString = String.format("()-[w: %f]->()", weight);
                long idx = (direction == Direction.OUTGOING) ? source : target;
                outEdges.compute(idx, (unused, list) -> {
                    if (list == null) {
                        list = Lists.newArrayList();
                    }
                    list.add(relString);
                    return list;
                });
                return true;
            });
            return true;
        });

        Map<Long, String> canonicalOutEdges = outEdges
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().stream().sorted().collect(Collectors.joining(", "))));

        return canonicalNodeProperties.entrySet().stream()
                .map(entry -> String.format(
                        "%s => %s",
                        entry.getValue(),
                        canonicalOutEdges.getOrDefault(entry.getKey(), "")))
                .sorted()
                .collect(Collectors.joining(System.lineSeparator()));
    }
}
