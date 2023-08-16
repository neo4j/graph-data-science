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

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.core.loading.GraphProjectCypherResult;
import org.neo4j.gds.core.loading.GraphProjectNativeResult;
import org.neo4j.gds.core.loading.GraphFilterResult;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.procedures.GraphDataScienceProcedureFacade;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.BaseProc.ESTIMATE_DESCRIPTION;
import static org.neo4j.gds.procedures.catalog.GraphCatalogProcedureConstants.PROJECT_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class GraphProjectProc {
    @Context
    public GraphDataScienceProcedureFacade facade;

    public GraphProjectProc() {
    }

    GraphProjectProc(GraphDataScienceProcedureFacade facade) {
        this.facade = facade;
    }

    @Procedure(name = "gds.graph.project", mode = READ)
    @Description(PROJECT_DESCRIPTION)
    public Stream<GraphProjectNativeResult> project(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeProjection") @Nullable Object nodeProjection,
        @Name(value = "relationshipProjection") @Nullable Object relationshipProjection,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.catalog().nativeProject(
            graphName,
            nodeProjection,
            relationshipProjection,
            configuration
        );
    }

    @Procedure(name = "gds.graph.project.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> projectEstimate(
        @Name(value = "nodeProjection") @Nullable Object nodeProjection,
        @Name(value = "relationshipProjection") @Nullable Object relationshipProjection,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.catalog().estimateNativeProject(nodeProjection, relationshipProjection, configuration);
    }

    @Procedure(
        name = "gds.graph.project.cypher", mode = READ,
        deprecatedBy = "gds.graph.project Cypher projection as an aggregation function"
    )
    @Description(PROJECT_DESCRIPTION)
    public Stream<GraphProjectCypherResult> projectCypher(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeQuery") String nodeQuery,
        @Name(value = "relationshipQuery") String relationshipQuery,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.catalog().cypherProject(graphName, nodeQuery, relationshipQuery, configuration);
    }

    @Procedure(name = "gds.graph.project.cypher.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> projectCypherEstimate(
        @Name(value = "nodeQuery") String nodeQuery,
        @Name(value = "relationshipQuery") String relationshipQuery,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.catalog().estimateCypherProject(nodeQuery, relationshipQuery, configuration);
    }

    @Internal
    @Deprecated(forRemoval = true)
    @Procedure(name = "gds.beta.graph.project.subgraph", mode = READ, deprecatedBy = "gds.graph.filter")
    @Description(PROJECT_DESCRIPTION)
    public Stream<GraphFilterResult> projectSubgraph(
        @Name(value = "graphName") String graphName,
        @Name(value = "fromGraphName") String fromGraphName,
        @Name(value = "nodeFilter") String nodeFilter,
        @Name(value = "relationshipFilter") String relationshipFilter,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade.log().warn("Procedure `gds.beta.graph.project.subgraph` has been deprecated, please use `gds.graph.filter`.");
        return facade.catalog().subGraphProject(graphName, fromGraphName, nodeFilter, relationshipFilter, configuration);
    }
}
