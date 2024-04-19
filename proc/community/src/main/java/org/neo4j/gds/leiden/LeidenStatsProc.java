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
package org.neo4j.gds.leiden;

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.procedures.community.leiden.LeidenStatsResult;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.procedures.ProcedureConstants.MEMORY_ESTIMATION_DESCRIPTION;
import static org.neo4j.gds.procedures.ProcedureConstants.STATS_MODE_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class LeidenStatsProc extends BaseProc {

    @Context
    public GraphDataScienceProcedures facade;
    @Procedure(value = "gds.leiden.stats", mode = READ)
    @Description(STATS_MODE_DESCRIPTION)
    public Stream<LeidenStatsResult> stats(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.community().leidenStats(graphName, configuration);
    }

    @Procedure(value = "gds.leiden.stats.estimate", mode = READ)
    @Description(MEMORY_ESTIMATION_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphName,
        @Name(value = "algoConfiguration") Map<String, Object> configuration
    ) {

        return facade.community().leidenEstimateStats(graphName, configuration);

    }

    @Deprecated(forRemoval = true)
    @Internal
    @Procedure(value = "gds.beta.leiden.stats", mode = READ, deprecatedBy = "gds.leiden.stats")
    @Description(STATS_MODE_DESCRIPTION)
    public Stream<LeidenStatsResult> statsBeta(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade.deprecatedProcedures().called("gds.beta.leiden.stats");

        executionContext()
            .log()
            .warn("Procedure `gds.beta.leiden.stats` has been deprecated, please use `gds.leiden.stats`.");

        return stats(graphName, configuration);
    }

    @Deprecated(forRemoval = true)
    @Internal
    @Procedure(value = "gds.beta.leiden.stats.estimate", mode = READ, deprecatedBy = "gds.leiden.stats.estimate")
    @Description(MEMORY_ESTIMATION_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimateBeta(
        @Name(value = "graphNameOrConfiguration") Object graphName,
        @Name(value = "algoConfiguration") Map<String, Object> configuration
    ) {
        facade.deprecatedProcedures().called("gds.beta.leiden.stats.estimate");

        executionContext()
            .log()
            .warn("Procedure `gds.beta.leiden.stats.estimate` has been deprecated, please use `gds.leiden.stats.estimate`.");

        return estimate(graphName, configuration);
    }
}
