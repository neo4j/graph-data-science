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
package org.neo4j.gds.catalog;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.core.loading.GraphDropNodePropertiesResult;
import org.neo4j.gds.procedures.GraphDataScience;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.catalog.GraphCatalogProcedureConstants.DROP_NODE_PROPERTIES_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class GraphDropNodePropertiesProc {
    @Context
    public GraphDataScience facade;

    @SuppressWarnings("unused")
    @Procedure(name = "gds.graph.nodeProperties.drop", mode = READ)
    @Description(DROP_NODE_PROPERTIES_DESCRIPTION)
    public Stream<GraphDropNodePropertiesResult> dropNodeProperties(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeProperties") @NotNull Object nodeProperties,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.catalog().dropNodeProperties(graphName, nodeProperties, configuration);
    }

    @SuppressWarnings("unused")
    @Procedure(name = "gds.graph.removeNodeProperties", mode = READ, deprecatedBy = "gds.graph.nodeProperties.drop")
    @Description(DROP_NODE_PROPERTIES_DESCRIPTION)
    @Internal
    @Deprecated(forRemoval = true)
    public Stream<GraphDropNodePropertiesResult> removeNodeProperties(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeProperties") @NotNull Object nodeProperties,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade.deprecatedProcedures().called("gds.graph.removeNodeProperties");

        facade
            .log()
            .warn(
                "Procedure `gds.graph.removeNodeProperties` has been deprecated, please use `gds.graph.nodeProperties.drop`.");

        return facade.catalog().dropNodeProperties(
            graphName,
            nodeProperties,
            configuration
        );
    }
}
