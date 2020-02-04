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
package org.neo4j.graphalgo;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class GetNodeFunc {
    @Context
    public GraphDatabaseAPI api;

    @UserFunction("gds.util.asNode")
    @Description("CALL gds.util.asNode(value) - return node for nodeId. null if none exists")
    public Node asNode(@Name(value = "nodeId") Number nodeId) {
        try {
            return api.getNodeById(nodeId.longValue());
        } catch (NotFoundException e) {
            return null;
        }
    }

    @UserFunction("gds.util.asNodes")
    @Description("CALL gds.util.asNodes(values) - return nodes for nodeIds. empty if none exists")
    public List<Node> asNodes(@Name(value = "nodeIds") List<Number> nodeIds) {
        return nodeIds.stream().map(nodeId -> {
            try {
                return api.getNodeById(nodeId.longValue());
            } catch (NotFoundException e) {
                return null;
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }

}
