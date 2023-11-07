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
package org.neo4j.gds.config;

import org.neo4j.graphdb.Node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class NodeIdParser {
    private NodeIdParser() {}

    /**
     * User input is one of
     *     * {@code java.lang.Number}
     *     * {@code ogr.neo4j.graphdb.Node}
     *     * {@code java.util.List} with one or more elements, where each element is one of the above
     *
     * @return A {@code java.util.Collection<Long>} of node IDs.
     */
    public static List<Long> parseToListOfNodeIds(Object input, String parameterName) {
        var nodeIds = new ArrayList<Long>();

        if (input instanceof Iterable) {
            for (var item : (Iterable) input) {
                nodeIds.add(parseNodeId(item, parameterName));
            }
        } else {
            nodeIds.add(parseNodeId(input, parameterName));
        }

        return nodeIds;
    }

    private static Long parseNodeId(Object input, String parameterName) {
        if (input instanceof Node) {
            return ((Node) input).getId();
        } else if (input instanceof Number) {
            return ((Number) input).longValue();
        }

        throw new IllegalArgumentException(formatWithLocale(
            "Failed to parse `%s` as a List of node IDs. A Node, Number or collection of the same can be parsed, but this `%s` cannot.",
            parameterName,
            input.getClass().getSimpleName()
        ));
    }

    /**
     * User input is one of
     *     * {@code java.lang.Number}
     *     * {@code ogr.neo4j.graphdb.Node}
     *     * {@code java.util.Collection} with a single element, where that element is one of the above
     *
     * @return A {@code java.lang.Long} of node IDs.
     */
    public static long parseToSingleNodeId(Object input, String parameterName) {
        if (input instanceof Collection) {
            var collection = (Collection) input;
            if (collection.size() != 1) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Failed to parse `%s` as a single node ID. A collection can be parsed if it contains a single element, but this `%s` contains `%s` elements.",
                    parameterName,
                    collection.getClass().getSimpleName(),
                    collection.size()
                ));
            }
            input = collection.iterator().next();
        }
        if (input instanceof Number) {
            return ((Number) input).longValue();
        }
        if (input instanceof Node) {
            return ((Node) input).getId();
        }
        throw new IllegalArgumentException(formatWithLocale(
            "Failed to parse `%s` as a single node ID. A Node, a Number or a collection containing a single Node or Number can be parsed, but this `%s` cannot.",
            parameterName,
            input.getClass().getSimpleName()
        ));
    }
}
