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

import static org.neo4j.gds.kmeans.KmeansStreamProc.KMEANS_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class KmeansMutateProc extends AlgoBaseProc<Kmeans, KmeansResult, KmeansMutateConfig, MutateResult> {

    @Procedure(value = "gds.beta.kmeans.mutate", mode = READ)
    @Description(KMEANS_DESCRIPTION)
    public Stream<MutateResult> mutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var mutateSpec = new KmeansMutateSpec();

        return new ProcedureExecutor<>(
            mutateSpec,
            executionContext()
        ).compute(graphName, configuration, true, true);
    }

    @Procedure(value = "gds.beta.kmeans.mutate.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphName,
        @Name(value = "algoConfiguration") Map<String, Object> configuration
    ) {
        var writeSpec = new KmeansMutateSpec();

        return new MemoryEstimationExecutor<>(
            writeSpec,
            executionContext()
        ).computeEstimate(graphName, configuration);
    }

    @Override
    public AlgorithmFactory<?, Kmeans, KmeansMutateConfig> algorithmFactory() {
        return new KmeansMutateSpec().algorithmFactory();
    }

    @Override
    public ComputationResultConsumer<Kmeans, KmeansResult, KmeansMutateConfig, Stream<MutateResult>> computationResultConsumer() {
        return new KmeansMutateSpec().computationResultConsumer();
    }

    @Override
    protected KmeansMutateConfig newConfig(String username, CypherMapWrapper config) {
        return new KmeansMutateSpec().newConfigFunction().apply(username, config);
    }
}
