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

import org.neo4j.gds.core.loading.GraphDropRelationshipResult;
import org.neo4j.gds.procedures.OpenGraphDataScience;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

import static org.neo4j.gds.catalog.GraphCatalogProcedureConstants.DROP_RELATIONSHIPS_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class GraphDropRelationshipProc {
    @Context
    public OpenGraphDataScience facade;

    @SuppressWarnings("unused")
    @Procedure(name = "gds.graph.relationships.drop", mode = READ)
    @Description(DROP_RELATIONSHIPS_DESCRIPTION)
    public Stream<GraphDropRelationshipResult> dropRelationships(
        @Name(value = "graphName") String graphName,
        @Name(value = "relationshipType") String relationshipType
    ) {
        return facade.catalog().dropRelationships(graphName, relationshipType);
    }

    @SuppressWarnings("unused")
    @Procedure(name = "gds.graph.deleteRelationships", mode = READ, deprecatedBy = "gds.graph.relationships.drop")
    @Description(DROP_RELATIONSHIPS_DESCRIPTION)
    @Internal
    @Deprecated(forRemoval = true)
    public Stream<GraphDropRelationshipResult> deleteRelationships(
        @Name(value = "graphName") String graphName,
        @Name(value = "relationshipType") String relationshipType
    ) {
        facade
            .log()
            .warn(
                "Procedure `gds.graph.deleteRelationships` has been deprecated, please use `gds.graph.relationships.drop`.");
        
        return facade.catalog().dropRelationships(graphName, relationshipType);
    }
}
