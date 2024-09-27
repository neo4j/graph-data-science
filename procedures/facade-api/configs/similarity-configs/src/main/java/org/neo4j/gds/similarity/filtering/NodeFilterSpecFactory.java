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
package org.neo4j.gds.similarity.filtering;

import org.neo4j.graphdb.Node;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class NodeFilterSpecFactory {

    private NodeFilterSpecFactory() {}

    /**
     * Create a {@link NodeFilterSpec} based on user input.
     *
     * User input can be a label represented as a {@link String}, or scalar or list of {@link Long} or {@link org.neo4j.graphdb.Node}.
     *
     * @param input One of {@link String}, {@link Long}, {@link org.neo4j.graphdb.Node}, {@link List} of {@link Long}, {@link List} of {@link Node}
     * @return A {@link NodeFilterSpec} that can be used to create a {@link NodeFilter} over an {@link org.neo4j.gds.api.IdMap}
     */
    public static NodeFilterSpec create(Object input) {
        if (input instanceof NodeFilterSpec) {
            return (NodeFilterSpec) input;
        }

        if (input instanceof String) {
            // parse as label
            return new LabelNodeFilterSpec((String) input);
        }

        Set<Long> nodeIds = null;

        if (input instanceof List) {
            nodeIds = parseFromList((List) input);
        }

        if (input instanceof Long) {
            nodeIds = parseFromLong((Long) input);
        }

        if (input instanceof Node) {
            nodeIds = parseFromNode((Node) input);
        }

        if (nodeIds == null) {
            throw new IllegalArgumentException(
                formatWithLocale("Invalid scalar type. Expected Long or Node but found: %s", input.getClass().getSimpleName())
            );
        }

        if (nodeIds.isEmpty()) {
            return NodeFilterSpec.noOp;
        }

        return new NodeIdNodeFilterSpec(nodeIds);
    }

    private static Set<Long> parseFromLong(Long input) {
        Set<Long> nodeIds = new HashSet<>();
        nodeIds.add(input);
        return nodeIds;
    }

    private static Set<Long> parseFromNode(Node input) {
        Set<Long> nodeIds = new HashSet<>();
        nodeIds.add(input.getId());
        return nodeIds;
    }

    private static Set<Long> parseFromList(List input) {
        Set<Long> nodeIds = new HashSet<>();
        List<String> badTypes = new ArrayList<>();
        input.forEach(o -> {
            if (o instanceof Long) {
                nodeIds.add((Long) o);
            } else if (o instanceof Node) {
                nodeIds.add(((Node) o).getId());
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

    public static String render(NodeFilterSpec spec) {
        return spec.render();
    }
}
