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

import org.neo4j.gds.applications.graphstorecatalog.GraphStreamRelationshipPropertiesResult;
import org.neo4j.gds.applications.graphstorecatalog.GraphStreamRelationshipPropertyResult;
import org.neo4j.gds.procedures.OpenGraphDataScience;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.catalog.GraphCatalogProcedureConstants.STREAM_RELATIONSHIP_PROPERTIES_DESCRIPTION;
import static org.neo4j.gds.catalog.GraphCatalogProcedureConstants.STREAM_RELATIONSHIP_PROPERTY_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class GraphStreamRelationshipPropertiesProc {
    @Context
    public OpenGraphDataScience facade;

    @SuppressWarnings("unused")
    @Procedure(name = "gds.graph.relationshipProperties.stream", mode = READ)
    @Description(STREAM_RELATIONSHIP_PROPERTIES_DESCRIPTION)
    public Stream<GraphStreamRelationshipPropertiesResult> streamRelationshipProperties(
        @Name(value = "graphName") String graphName,
        @Name(value = "relationshipProperties") List<String> relationshipProperties,
        @Name(value = "relationshipTypes", defaultValue = "['*']") List<String> relationshipTypes,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.catalog().streamRelationshipProperties(
            graphName,
            relationshipProperties, relationshipTypes, configuration
        );
    }

    @SuppressWarnings("unused")
    @Procedure(name = "gds.graph.streamRelationshipProperties", mode = READ, deprecatedBy = "gds.graph.relationshipProperties.stream")
    @Description(STREAM_RELATIONSHIP_PROPERTIES_DESCRIPTION)
    @Internal
    @Deprecated(forRemoval = true)
    public Stream<GraphStreamRelationshipPropertiesResult> deprecatedStreamRelationshipProperties(
        @Name(value = "graphName") String graphName,
        @Name(value = "relationshipProperties") List<String> relationshipProperties,
        @Name(value = "relationshipTypes", defaultValue = "['*']") List<String> relationshipTypes,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {

        facade
            .log()
            .warn(
                "Procedure `gds.graph.streamRelationshipProperties` has been deprecated, please use `gds.graph.relationshipProperties.stream`.");

        return facade.catalog().streamRelationshipProperties(
            graphName,
            relationshipProperties, relationshipTypes, configuration
        );
    }

    @SuppressWarnings("unused")
    @Procedure(name = "gds.graph.relationshipProperty.stream", mode = READ)
    @Description(STREAM_RELATIONSHIP_PROPERTY_DESCRIPTION)
    public Stream<GraphStreamRelationshipPropertyResult> streamRelationshipProperty(
        @Name(value = "graphName") String graphName,
        @Name(value = "relationshipProperty") String relationshipProperty,
        @Name(value = "relationshipTypes", defaultValue = "['*']") List<String> relationshipTypes,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.catalog().streamRelationshipProperty(
            graphName,
            relationshipProperty,
            relationshipTypes,
            configuration
        );
    }

    @SuppressWarnings("unused")
    @Procedure(name = "gds.graph.streamRelationshipProperty", mode = READ, deprecatedBy = "gds.graph.relationshipProperty.stream")
    @Description(STREAM_RELATIONSHIP_PROPERTY_DESCRIPTION)
    @Internal
    @Deprecated(forRemoval = true)
    public Stream<GraphStreamRelationshipPropertyResult> deprecatedStreamRelationshipProperty(
        @Name(value = "graphName") String graphName,
        @Name(value = "relationshipProperties") String relationshipProperty,
        @Name(value = "relationshipTypes", defaultValue = "['*']") List<String> relationshipTypes,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade
            .log()
            .warn(
                "Procedure `gds.graph.streamRelationshipProperty` has been deprecated, please use `gds.graph.relationshipProperty.stream`.");

        return facade.catalog().streamRelationshipProperty(
            graphName,
            relationshipProperty,
            relationshipTypes,
            configuration
        );
    }
}
