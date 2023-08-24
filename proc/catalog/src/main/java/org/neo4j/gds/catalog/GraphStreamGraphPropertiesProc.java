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

import org.neo4j.gds.procedures.OpenGraphDataScience;
import org.neo4j.gds.procedures.catalog.StreamGraphPropertyResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.catalog.GraphCatalogProcedureConstants.STREAM_GRAPH_PROPERTY_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class GraphStreamGraphPropertiesProc {
    @Context
    public OpenGraphDataScience facade;

    @SuppressWarnings("unused")
    @Internal
    @Deprecated(forRemoval = true)
    @Procedure(
        name = "gds.alpha.graph.graphProperty.stream", mode = READ, deprecatedBy = "gds.graph.graphProperty.stream"
    )
    @Description(STREAM_GRAPH_PROPERTY_DESCRIPTION)
    public Stream<StreamGraphPropertyResult> alphaStreamProperty(
        @Name(value = "graphName") String graphName,
        @Name(value = "graphProperty") String graphProperty,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade.log()
            .warn(
                "Procedure `gds.alpha.graph.graphProperty.stream` has been deprecated, please use `gds.graph.graphProperty.stream`.");

        return streamProperty(graphName, graphProperty, configuration);
    }

    @SuppressWarnings("unused")
    @Procedure(name = "gds.graph.graphProperty.stream", mode = READ)
    @Description(STREAM_GRAPH_PROPERTY_DESCRIPTION)
    public Stream<StreamGraphPropertyResult> streamProperty(
        @Name(value = "graphName") String graphName,
        @Name(value = "graphProperty") String graphProperty,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.catalog().streamGraphProperty(graphName, graphProperty, configuration);
    }
}
