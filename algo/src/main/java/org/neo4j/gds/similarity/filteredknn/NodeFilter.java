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
package org.neo4j.gds.similarity.filteredknn;

import org.neo4j.gds.api.IdMap;
import org.neo4j.graphdb.Node;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.LongPredicate;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class NodeFilter implements LongPredicate {

    public static NodeFilter create(Object input, IdMap idMap) {
        if (input instanceof NodeFilter) {
            return (NodeFilter) input;
        }

        if (input instanceof String) {
            // parse as label
            return parseFromString((String) input);
        }

        Set<Long> nodeIds = null;

        if (input instanceof List) {
            nodeIds = parseFromList((List) input, idMap);
        }

        if (input instanceof Long) {
            nodeIds = parseFromLong((Long) input, idMap);
        }

        if (input instanceof Node) {
            nodeIds = parseFromNode((Node) input, idMap);
        }

        if (nodeIds == null) {
            throw new IllegalArgumentException(
                String.format("Invalid scalar type. Expected Long or Node but found: %s", input.getClass().getSimpleName())
            );
        }

        return new NodeFilter(nodeIds);
    }

    private static NodeFilter parseFromString(String input) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    private static Set<Long> parseFromLong(Long input, IdMap idMap) {
        Set<Long> nodeIds = new HashSet<>();
        nodeIds.add(idMap.toMappedNodeId(input));
        return nodeIds;
    }

    private static Set<Long> parseFromNode(Node input, IdMap idMap) {
        Set<Long> nodeIds = new HashSet<>();
        nodeIds.add(idMap.toMappedNodeId(input.getId()));
        return nodeIds;
    }

    private static Set<Long> parseFromList(List input, IdMap idMap) {
        Set<Long> nodeIds = new HashSet<>();
        List<String> badTypes = new ArrayList<>();
        input.forEach(o -> {
            if (o instanceof Long) {
                nodeIds.add(idMap.toMappedNodeId((Long) o));
            } else if (o instanceof Node) {
                nodeIds.add(idMap.toMappedNodeId(((Node) o).getId()));
            } else {
                badTypes.add(o.getClass().getSimpleName());
            }
        });

        if (badTypes.isEmpty()) {
            return nodeIds;
        }

        throw new IllegalArgumentException(formatWithLocale(
            "Invalid types in list. Expected Longs or Nodes but found %s",
            badTypes
        ));
    }

    public static NodeFilter noOp() {
        return new NoOpNodeFilter(Set.of());
    }

    private final Set<Long> nodeIds;

    private NodeFilter(Set<Long> nodeIds) {
        this.nodeIds = nodeIds;
    }

    @Override
    public boolean test(long nodeId) {
        return nodeIds.contains(nodeId);
    }

    private static class NoOpNodeFilter extends NodeFilter {

        NoOpNodeFilter(Set<Long> nodeIds) {
            super(nodeIds);
        }

        @Override
        public boolean test(long nodeId) {
            return true;
        }
    }
}
