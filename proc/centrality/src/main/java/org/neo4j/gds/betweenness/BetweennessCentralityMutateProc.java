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
package org.neo4j.gds.betweenness;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.MutatePropertyProc;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.validation.ValidationConfiguration;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class BetweennessCentralityMutateProc extends MutatePropertyProc<BetweennessCentrality, HugeAtomicDoubleArray, BetweennessCentralityMutateProc.MutateResult, BetweennessCentralityMutateConfig> {

    @Procedure(value = "gds.betweenness.mutate", mode = READ)
    @Description(BetweennessCentralityProc.BETWEENNESS_DESCRIPTION)
    public Stream<MutateResult> mutate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return mutate(compute(graphNameOrConfig, configuration));
    }

    @Procedure(value = "gds.betweenness.mutate.estimate", mode = READ)
    @Description(BetweennessCentralityProc.BETWEENNESS_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected BetweennessCentralityMutateConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return BetweennessCentralityMutateConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    public ValidationConfiguration<BetweennessCentralityMutateConfig> getValidationConfig() {
        return BetweennessCentralityProc.getValidationConfig();
    }

    @Override
    protected AlgorithmFactory<BetweennessCentrality, BetweennessCentralityMutateConfig> algorithmFactory() {
        return BetweennessCentralityProc.algorithmFactory();
    }

    @Override
    protected NodeProperties nodeProperties(AlgoBaseProc.ComputationResult<BetweennessCentrality, HugeAtomicDoubleArray, BetweennessCentralityMutateConfig> computationResult) {
        return BetweennessCentralityProc.nodeProperties(computationResult);
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(ComputationResult<BetweennessCentrality, HugeAtomicDoubleArray, BetweennessCentralityMutateConfig> computeResult) {
        return BetweennessCentralityProc.resultBuilder(new MutateResult.Builder(callContext, computeResult.config().concurrency()), computeResult);
    }

    @SuppressWarnings("unused")
    public static final class MutateResult extends BetweennessCentralityStatsProc.StatsResult {

        public final long nodePropertiesWritten;
        public final long mutateMillis;

        MutateResult(
            long nodePropertiesWritten,
            long createMillis,
            long computeMillis,
            long postProcessingMillis,
            long mutateMillis,
            @Nullable Map<String, Object> centralityDistribution,
            double scoreSum,
            double minimumScore,
            double maximumScore,
            Map<String, Object> config
        ) {
            super(
                centralityDistribution,
                scoreSum,
                minimumScore,
                maximumScore,
                createMillis,
                computeMillis,
                postProcessingMillis,
                config
            );
            this.nodePropertiesWritten = nodePropertiesWritten;
            this.mutateMillis = mutateMillis;
        }

        static final class Builder extends BetweennessCentralityProc.BetweennessCentralityResultBuilder<MutateResult> {

            Builder(ProcedureCallContext context, int concurrency) {
                super(context, concurrency);
            }

            @Override
            public MutateResult buildResult() {
                return new MutateResult(
                    nodePropertiesWritten,
                    createMillis,
                    computeMillis,
                    postProcessingMillis,
                    mutateMillis,
                    centralityHistogram,
                    sumCentrality,
                    minCentrality,
                    maxCentrality,
                    config.toMap()
                );
            }
        }
    }
}
