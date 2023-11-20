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
package org.neo4j.gds.influenceMaximization;

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.executor.MemoryEstimationExecutor;
import org.neo4j.gds.executor.ProcedureExecutor;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class CELFStatsProc extends BaseProc {

    @Procedure(value = "gds.influenceMaximization.celf.stats", mode = READ)
    @Description(STATS_DESCRIPTION)
    public Stream<StatsResult> stats(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return new ProcedureExecutor<>(
            new CELFStatsSpec(),
            executionContext()
        ).compute(graphName, configuration);
    }

    @Procedure(name = "gds.influenceMaximization.celf.stats.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        var statsSpec = new CELFStatsSpec();

        return new MemoryEstimationExecutor<>(
            statsSpec,
            executionContext(),
            transactionContext()
        ).computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Procedure(
        value = "gds.beta.influenceMaximization.celf.stats",
        mode = READ,
        deprecatedBy = "gds.influenceMaximization.celf.stats"
    )
    @Description(STATS_DESCRIPTION)
    @Internal
    @Deprecated
    public Stream<StatsResult> betaStats(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        executionContext()
            .metricsFacade()
            .deprecatedProcedures().called("gds.beta.influenceMaximization.celf.stats");

        executionContext()
            .log()
            .warn(
                "Procedure `gds.beta.influenceMaximization.celf.stats has been deprecated, please use `gds.influenceMaximization.celf.stats`.");
        return stats(graphName, configuration);
    }

    @Procedure(
        name = "gds.beta.influenceMaximization.celf.stats.estimate",
        mode = READ,
        deprecatedBy = "gds.influenceMaximization.celf.stats.estimate"
    )
    @Description(ESTIMATE_DESCRIPTION)
    @Internal
    @Deprecated
    public Stream<MemoryEstimateResult> betaEstimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        executionContext()
            .metricsFacade()
            .deprecatedProcedures().called("gds.beta.influenceMaximization.celf.stats.estimate");

        executionContext()
            .log()
            .warn(
                "Procedure `gds.beta.influenceMaximization.celf.stats.estimate has been deprecated, please use `gds.influenceMaximization.celf.stats.estimate`.");
        return estimate(graphNameOrConfiguration, algoConfiguration);
    }

}
