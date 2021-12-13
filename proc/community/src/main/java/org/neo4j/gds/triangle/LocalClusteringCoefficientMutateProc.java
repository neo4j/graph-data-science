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
package org.neo4j.gds.triangle;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.MutatePropertyProc;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.pipeline.validation.ValidationConfiguration;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.triangle.LocalClusteringCoefficientCompanion.DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class LocalClusteringCoefficientMutateProc extends MutatePropertyProc<LocalClusteringCoefficient, LocalClusteringCoefficient.Result, LocalClusteringCoefficientMutateProc.MutateResult, LocalClusteringCoefficientMutateConfig> {

    @Procedure(value = "gds.localClusteringCoefficient.mutate", mode = READ)
    @Description(DESCRIPTION)
    public Stream<MutateResult> write(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return mutate(compute(graphName, configuration));
    }

    @Procedure(value = "gds.localClusteringCoefficient.mutate.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected LocalClusteringCoefficientMutateConfig newConfig(String username, CypherMapWrapper config) {
        return LocalClusteringCoefficientMutateConfig.of(config);
    }

    @Override
    protected GraphAlgorithmFactory<LocalClusteringCoefficient, LocalClusteringCoefficientMutateConfig> algorithmFactory() {
        return new LocalClusteringCoefficientFactory<>();
    }

    @Override
    public ValidationConfiguration<LocalClusteringCoefficientMutateConfig> getValidationConfig() {
        return LocalClusteringCoefficientCompanion.getValidationConfig(log);
    }

    @Override
    protected NodeProperties nodeProperties(
        ComputationResult<LocalClusteringCoefficient, LocalClusteringCoefficient.Result, LocalClusteringCoefficientMutateConfig> computationResult
    ) {
        return LocalClusteringCoefficientCompanion.nodeProperties(computationResult);
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(ComputationResult<LocalClusteringCoefficient, LocalClusteringCoefficient.Result, LocalClusteringCoefficientMutateConfig> computeResult) {
        return LocalClusteringCoefficientCompanion.resultBuilder(
            new LocalClusteringCoefficientMutateBuilder(
                callContext,
                computeResult.config().concurrency(),
                allocationTracker()
            ),
            computeResult
        );
    }

    @SuppressWarnings("unused")
    public static class MutateResult extends LocalClusteringCoefficientStatsProc.StatsResult {

        public long mutateMillis;
        public long nodePropertiesWritten;

        MutateResult(
            double averageClusteringCoefficient,
            long nodeCount,
            long createMillis,
            long computeMillis,
            long mutateMillis,
            long nodePropertiesWritten,
            Map<String, Object> configuration
        ) {
            super(
                averageClusteringCoefficient,
                nodeCount,
                createMillis,
                computeMillis,
                configuration
            );
            this.mutateMillis = mutateMillis;
            this.nodePropertiesWritten = nodePropertiesWritten;
        }
    }

    static class LocalClusteringCoefficientMutateBuilder extends LocalClusteringCoefficientCompanion.ResultBuilder<MutateResult> {

        LocalClusteringCoefficientMutateBuilder(ProcedureCallContext callContext, int concurrency, AllocationTracker allocationTracker) {
            super(callContext, concurrency, allocationTracker);
        }

        @Override
        protected MutateResult buildResult() {
            return new MutateResult(
                averageClusteringCoefficient,
                nodeCount,
                createMillis,
                computeMillis,
                mutateMillis,
                nodePropertiesWritten,
                config.toMap()
            );
        }
    }
}
