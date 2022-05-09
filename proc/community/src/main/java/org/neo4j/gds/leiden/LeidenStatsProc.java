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

import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ProcedureExecutor;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class LeidenStatsProc extends AlgoBaseProc<Leiden, LeidenResult, LeidenStatsConfig, StatsResult> {

    @Procedure(value = "gds.alpha.leiden.stats", mode = READ)
    @Description(STATS_DESCRIPTION)
    public Stream<StatsResult> stats(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var statsSpec = new LeidenStatsSpec();
        return new ProcedureExecutor<>(
            statsSpec,
            executionContext()
        ).compute(graphName, configuration, true, true);
    }



    @Override
    @Deprecated
    public AlgorithmFactory<?, Leiden, LeidenStatsConfig> algorithmFactory() {
        return null;
    }

    @Override
    @Deprecated
    public <T extends ComputationResultConsumer<Leiden, LeidenResult, LeidenStatsConfig, Stream<StatsResult>>> T computationResultConsumer() {
        return null;
    }

    @Override
    @Deprecated
    protected LeidenStatsConfig newConfig(String username, CypherMapWrapper config) {
        return null;
    }
}
