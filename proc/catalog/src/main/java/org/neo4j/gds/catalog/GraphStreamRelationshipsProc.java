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

import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.applications.graphstorecatalog.TopologyResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.catalog.GraphCatalogProcedureConstants.STREAM_RELATIONSHIPS_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class GraphStreamRelationshipsProc {
    @Context
    public GraphDataScienceProcedures facade;

    @Procedure(name = "gds.graph.relationships.stream", mode = READ)
    @Description(STREAM_RELATIONSHIPS_DESCRIPTION)
    public Stream<TopologyResult> streamRelationships(
        @Name(value = "graphName") String graphName,
        @Name(value = "relationshipTypes", defaultValue = "['*']") List<String> relationshipTypes,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.catalog().streamRelationships(graphName, relationshipTypes, configuration);
    }

    @Procedure(name = "gds.beta.graph.relationships.stream", mode = READ, deprecatedBy = "gds.graph.relationships.stream")
    @Description(STREAM_RELATIONSHIPS_DESCRIPTION)
    @Deprecated(forRemoval = true)
    @Internal
    public Stream<TopologyResult> betaStreamRelationships(
        @Name(value = "graphName") String graphName,
        @Name(value = "relationshipTypes", defaultValue = "['*']") List<String> relationshipTypes,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade.deprecatedProcedures().called("gds.beta.graph.relationships.stream");

        facade.log().warn("Procedure `gds.beta.graph.relationships.stream` has been deprecated, please use `gds.graph.relationships.stream`.");

        return facade.catalog().streamRelationships(graphName, relationshipTypes, configuration);
    }
}
