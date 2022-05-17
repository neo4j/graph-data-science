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

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

/**
 * This class serves to do as much parsing and validation as possible in the UI for creating a {@link NodeFilter}.
 *
 * We can
 * <ul>
 * <li>normalize {@code Long}, {@code List<Long>}, {@code Node} and {@code List<Node>} to {@code Set<Long>}</li>
 * <li>store the normalized {@code Set<Long>}, or the label {@code String}, as the case may be.</li>
 * </ul>
 * But we cannot
 * <ul>
 * <li>validated that the nodes or label exist in the graph.</li>
 * <li>translate node ids from Neo4j id space to the internal id space.</li>
 * </ul>
 *
 * The latter two have to happen later, when the {@link NodeFilterSpec} is turned into a {@link NodeFilter}.
 */
public class NodeFilterSpec {

    public static NodeFilterSpec create(Object input) {
        if (input instanceof NodeFilterSpec) {
            return (NodeFilterSpec) input;
        }

        if (input instanceof String) {
            // parse as label
            return new NodeFilterSpec((String) input);
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

        return new NodeFilterSpec(nodeIds);
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

    private final Set<Long> nodeIds;
    private final String labelString;

    NodeFilterSpec(Set<Long> nodeIds) {
        this.nodeIds = nodeIds;
        this.labelString = null;
    }

    NodeFilterSpec(String labelString) {
        this.nodeIds = null;
        this.labelString = labelString;
    }

    NodeFilter toNodeFilter(IdMap idMap) {
        if (nodeIds != null) {
            if (nodeIds.isEmpty()) {
                return NodeFilter.noOp();
            }
            return NodeFilter.create(nodeIds, idMap);
        }
        if (labelString != null) {
            return NodeFilter.create(labelString, idMap);
        }
        throw new IllegalStateException("This object is broken. This should not happen, says Jonatan.");
    }
}
