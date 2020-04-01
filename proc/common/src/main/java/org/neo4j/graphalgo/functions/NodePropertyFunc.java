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
package org.neo4j.graphalgo.functions;

import org.neo4j.graphalgo.core.loading.GraphStore;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.Objects;

public class NodePropertyFunc {
    @Context
    public KernelTransaction transaction;

    @UserFunction("gds.util.nodeProperty")
    @Description("Returns a node property value from a named in-memory graph.")
    public double nodeProperty(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeId") Number nodeId,
        @Name(value = "propertyKey") String propertyKey
    ) {
        Objects.requireNonNull(graphName);
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(propertyKey);

        String username = transaction.subjectOrAnonymous().username();

        GraphStore graphStore = GraphStoreCatalog.get(username, graphName).graphStore();

        if (!graphStore.hasNodeProperty(propertyKey)) {
            throw new IllegalArgumentException(String.format("Node property with given name `%s` does not exist.", propertyKey));
        }

        long internalId = graphStore.nodes().toMappedNodeId(nodeId.longValue());

        if (internalId == -1) {
            throw new IllegalArgumentException(String.format("Node id %d does not exist.", nodeId.longValue()));
        }

        return graphStore
            .nodeProperty(propertyKey)
            .nodeProperty(internalId);
    }
}
