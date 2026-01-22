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
package org.neo4j.gds.hits;

import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.procedures.algorithms.centrality.HitsStatsResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.hits.Constants.HITS_DESCRIPTION;
import static org.neo4j.gds.procedures.ProcedureConstants.MEMORY_ESTIMATION_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class HitsStatsProc {

    @Context
    public GraphDataScienceProcedures facade;

    @Procedure(name = "gds.hits.stats", mode = READ)
    @Description(HITS_DESCRIPTION)
    public Stream<HitsStatsResult> stats(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.algorithms().centrality().hitsStats(graphName, configuration);
    }

    @Procedure(name = "gds.hits.stats.estimate", mode = READ)
    @Description(MEMORY_ESTIMATION_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.algorithms().centrality().hitsStatsEstimate(graphName, configuration);
    }

    @Procedure(
        name = "gds.alpha.hits.stats",
        mode = READ,
        deprecatedBy = "gds.hits.stats"
    )
    @Internal
    @Description(HITS_DESCRIPTION)
    @Deprecated
    public Stream<HitsStatsResult> alphastats(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade.deprecatedProcedures().called("gds.alpha.hits.stats");
        facade
            .log()
            .warn(
                "Procedure `gds.alpha.hits.stats has been deprecated, please use `gds.hits.stats`.");

        return stats(graphName, configuration);
    }

    @Procedure(
        name = "gds.alpha.hits.stats.estimate",
        mode = READ,
        deprecatedBy = "gds.hits.stats.estimate"
    )
    @Internal
    @Description(MEMORY_ESTIMATION_DESCRIPTION)
    @Deprecated
    public Stream<MemoryEstimateResult> alphaEstimate(
        @Name(value = "graphName") Object graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade.deprecatedProcedures().called("gds.alpha.hits.stats.estimate");
        facade.log()
            .warn(
                "Procedure `gds.alpha.hits.stats.estimate has been deprecated, please use `gds.hits.stats.estimate`.");

        return estimate(graphName, configuration);
    }
}
