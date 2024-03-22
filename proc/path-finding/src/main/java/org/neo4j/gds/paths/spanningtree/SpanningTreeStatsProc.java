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

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.executor.MemoryEstimationExecutor;
import org.neo4j.gds.executor.ProcedureExecutor;
import org.neo4j.gds.procedures.ProcedureConstants;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.paths.spanningtree.Constants.SPANNING_TREE_DESCRIPTION;
import static org.neo4j.gds.procedures.ProcedureConstants.MEMORY_ESTIMATION_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class SpanningTreeStatsProc extends BaseProc {
    @Procedure(value = "gds.spanningTree.stats", mode = READ)
    @Description(SPANNING_TREE_DESCRIPTION)
    public Stream<StatsResult> spanningTree(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return new ProcedureExecutor<>(
            new SpanningTreeStatsSpec(),
            executionContext()
        ).compute(graphName, configuration);
    }

    @Procedure(value = "gds.spanningTree.stats" + ".estimate", mode = READ)
    @Description(MEMORY_ESTIMATION_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphName,
        @Name(value = "algoConfiguration") Map<String, Object> configuration
    ) {
        var spec = new SpanningTreeStatsSpec();
        return new MemoryEstimationExecutor<>(
            spec,
            executionContext(),
            transactionContext()
        ).computeEstimate(graphName, configuration);
    }

    @Procedure(value = "gds.beta.spanningTree.stats", mode = READ, deprecatedBy = "gds.spanningTree.stats")
    @Description(SPANNING_TREE_DESCRIPTION)
    @Internal
    @Deprecated
    public Stream<StatsResult> betaSpanningTree(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        executionContext()
            .metricsFacade()
            .deprecatedProcedures().called("gds.beta.spanningTree.stats");

        executionContext()
            .log()
            .warn("Procedure `gds.beta.spanningTree.stats` has been deprecated, please use `gds.spanningTree.stats`.");
        return spanningTree(graphName, configuration);
    }

    @Procedure(value = "gds.beta.spanningTree.stats" + ".estimate", mode = READ, deprecatedBy = "gds.spanningTree.stats" + ".estimate")
    @Description(MEMORY_ESTIMATION_DESCRIPTION)
    @Internal
    @Deprecated
    public Stream<MemoryEstimateResult> betaEstimate(
        @Name(value = "graphNameOrConfiguration") Object graphName,
        @Name(value = "algoConfiguration") Map<String, Object> configuration
    ) {
        executionContext()
            .metricsFacade()
            .deprecatedProcedures().called("gds.beta.spanningTree.stats" + ".estimate");
        executionContext()
            .log()
            .warn(
                "Procedure `gds.beta.spanningTree.stats.estimate` has been deprecated, please use `gds.spanningTree.stats.estimate`.");
        return estimate(graphName, configuration);
    }
}
