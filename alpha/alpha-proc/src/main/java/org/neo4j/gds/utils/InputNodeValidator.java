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
package org.neo4j.gds.utils;

import org.neo4j.graphalgo.api.Graph;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public final class InputNodeValidator {

    public static void validateStartNode(long nodeId, Graph graph) throws IllegalArgumentException {
        validateNodeIsLoaded(nodeId, graph.toMappedNodeId(nodeId), "startNode");
    }

    public static void validateEndNode(long nodeId, Graph graph) throws IllegalArgumentException {
        validateNodeIsLoaded(nodeId, graph.toMappedNodeId(nodeId), "endNode");
    }

    private static void validateNodeIsLoaded(long nodeId, long mappedId, String nodeDescription) throws IllegalArgumentException {
        if (mappedId == -1) {
            throw new IllegalArgumentException(formatWithLocale(
                "%s with id %d was not loaded",
                nodeDescription,
                nodeId
            ));
        }
    }
}
