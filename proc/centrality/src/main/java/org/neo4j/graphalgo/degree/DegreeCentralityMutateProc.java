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
package org.neo4j.graphalgo.degree;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.degree.DegreeCentrality;
import org.neo4j.gds.degree.DegreeCentralityFactory;
import org.neo4j.gds.degree.DegreeCentralityMutateConfig;
import org.neo4j.gds.result.AbstractCentralityResultBuilder;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.graphalgo.MutatePropertyProc;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.results.StandardMutateResult;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.degree.DegreeCentralityProc.DEGREE_CENTRALITY_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class DegreeCentralityMutateProc extends MutatePropertyProc<DegreeCentrality, DegreeCentrality.DegreeFunction, DegreeCentralityMutateProc.MutateResult, DegreeCentralityMutateConfig> {

    @Procedure(value = "gds.degree.mutate", mode = READ)
    @Description(DEGREE_CENTRALITY_DESCRIPTION)
    public Stream<DegreeCentralityMutateProc.MutateResult> mutate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return mutate(compute(graphNameOrConfig, configuration));
    }

    @Procedure(value = "gds.degree.mutate.estimate", mode = READ)
    @Description(DEGREE_CENTRALITY_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected DegreeCentralityMutateConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return DegreeCentralityMutateConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<DegreeCentrality, DegreeCentralityMutateConfig> algorithmFactory() {
        return new DegreeCentralityFactory<>();
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(ComputationResult<DegreeCentrality, DegreeCentrality.DegreeFunction, DegreeCentralityMutateConfig> computeResult) {
        return DegreeCentralityProc.resultBuilder(
            new DegreeCentralityMutateProc.MutateResult.Builder(callContext, computeResult.config().concurrency()),
            computeResult
        );
    }

    @Override
    protected NodeProperties nodeProperties(ComputationResult<DegreeCentrality, DegreeCentrality.DegreeFunction, DegreeCentralityMutateConfig> computationResult) {
        return DegreeCentralityProc.nodeProperties(computationResult);
    }

    public static final class MutateResult extends StandardMutateResult {

        public final long nodePropertiesWritten;
        public final Map<String, Object> centralityDistribution;

        MutateResult(
            long nodePropertiesWritten,
            long createMillis,
            long computeMillis,
            long postProcessingMillis,
            long mutateMillis,
            @Nullable Map<String, Object> centralityDistribution,
            Map<String, Object> config
        ) {
            super(
                createMillis,
                computeMillis,
                postProcessingMillis,
                mutateMillis,
                config
            );
            this.nodePropertiesWritten = nodePropertiesWritten;
            this.centralityDistribution = centralityDistribution;
        }

        static final class Builder extends AbstractCentralityResultBuilder<DegreeCentralityMutateProc.MutateResult> {

            Builder(ProcedureCallContext context, int concurrency) {
                super(context, concurrency);
            }

            @Override
            public DegreeCentralityMutateProc.MutateResult buildResult() {
                return new DegreeCentralityMutateProc.MutateResult(
                    nodePropertiesWritten,
                    createMillis,
                    computeMillis,
                    postProcessingMillis,
                    mutateMillis,
                    centralityHistogramOrNull(),
                    config.toMap()
                );
            }
        }
    }
}
