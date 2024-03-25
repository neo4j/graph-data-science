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
package org.neo4j.gds.paths.spanningtree;

import org.neo4j.gds.procedures.GraphDataScience;
import org.neo4j.gds.procedures.algorithms.pathfinding.SpanningTreeStreamResult;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.paths.spanningtree.Constants.SPANNING_TREE_DESCRIPTION;
import static org.neo4j.gds.procedures.ProcedureConstants.MEMORY_ESTIMATION_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class SpanningTreeStreamProc {
    @Context
    public GraphDataScience facade;

    @Procedure(value = "gds.spanningTree.stream", mode = READ)
    @Description(SPANNING_TREE_DESCRIPTION)
    public Stream<SpanningTreeStreamResult> spanningTree(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.pathFinding().spanningTreeStream(graphName, configuration);
    }

    @Procedure(value = "gds.spanningTree.stream" + ".estimate", mode = READ)
    @Description(MEMORY_ESTIMATION_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphName,
        @Name(value = "algoConfiguration") Map<String, Object> configuration
    ) {
        return facade.pathFinding().spanningTreeStreamEstimate(graphName, configuration);
    }

    @Procedure(value = "gds.beta.spanningTree.stream", mode = READ, deprecatedBy = "gds.spanningTree.stream")
    @Description(SPANNING_TREE_DESCRIPTION)
    @Internal
    @Deprecated
    public Stream<SpanningTreeStreamResult> betaSpanningTree(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade.deprecatedProcedures().called("gds.beta.spanningTree.stream");
        facade
            .log()
            .warn("Procedure `gds.beta.spanningTree.stream` has been deprecated, please use `gds.spanningTree.stream`.");
        return spanningTree(graphName, configuration);
    }

    @Procedure(value = "gds.beta.spanningTree.stream" + ".estimate", mode = READ, deprecatedBy = "gds.spanningTree.stream" + ".estimate")
    @Description(MEMORY_ESTIMATION_DESCRIPTION)
    @Internal
    @Deprecated
    public Stream<MemoryEstimateResult> betaEstimate(
        @Name(value = "graphNameOrConfiguration") Object graphName,
        @Name(value = "algoConfiguration") Map<String, Object> configuration
    ) {
        facade.deprecatedProcedures().called("gds.beta.spanningTree.stream" + ".estimate");
        facade
            .log()
            .warn(
                "Procedure `gds.beta.spanningTree.stream.estimate` has been deprecated, please use `gds.spanningTree.stream.estimate`.");
        return estimate(graphName, configuration);
    }
}
