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
package org.neo4j.gds.functions;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.graphdb.Node;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.List;

public class AsNodeFunc {
    @Context
    public GraphDataScienceProcedures facade;

    @Nullable
    @UserFunction("gds.util.asNode")
    @Description("RETURN gds.util.asNode(nodeId) - Return the node objects for the given node id or null if none exists.")
    public Node asNode(@Name(value = "nodeId") Number nodeId) {
        return facade.functions().asNode(nodeId);
    }

    @UserFunction("gds.util.asNodes")
    @Description("RETURN gds.util.asNodes(nodeIds) - Return the node objects for the given node ids or an empty list if none exists.")
    public List<Node> asNodes(@Name(value = "nodeIds") List<Number> nodeIds) {
        return facade.functions().asNodes(nodeIds);
    }
}
