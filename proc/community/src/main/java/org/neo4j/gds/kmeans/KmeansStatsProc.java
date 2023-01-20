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
package org.neo4j.gds.kmeans;

import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.MemoryEstimationExecutor;
import org.neo4j.gds.executor.ProcedureExecutor;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class KmeansStatsProc extends AlgoBaseProc<Kmeans, KmeansResult, KmeansStatsConfig, StatsResult> {

    @Procedure(value = "gds.beta.kmeans.stats", mode = READ)
    @Description(STATS_DESCRIPTION)
    public Stream<StatsResult> stats(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var statsSpec = new KmeansStatsSpec();
        return new ProcedureExecutor<>(
            statsSpec,
            executionContext()
        ).compute(graphName, configuration, true);
    }

    @Procedure(value = "gds.beta.kmeans.stats.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphName,
        @Name(value = "algoConfiguration") Map<String, Object> configuration
    ) {
        var writeSpec = new KmeansStatsSpec();

        return new MemoryEstimationExecutor<>(
            writeSpec,
            executionContext()
        ).computeEstimate(graphName, configuration);
    }


    @Override
    public AlgorithmFactory<?, Kmeans, KmeansStatsConfig> algorithmFactory() {
        return new KmeansStatsSpec().algorithmFactory();
    }

    @Override
    public ComputationResultConsumer<Kmeans, KmeansResult, KmeansStatsConfig, Stream<StatsResult>> computationResultConsumer() {
        return new KmeansStatsSpec().computationResultConsumer();
    }

    @Override
    protected KmeansStatsConfig newConfig(String username, CypherMapWrapper config) {
        return new KmeansStatsSpec().newConfigFunction().apply(username, config);
    }
}
