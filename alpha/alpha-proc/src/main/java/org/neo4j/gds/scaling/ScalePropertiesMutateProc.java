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
package org.neo4j.gds.scaling;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.MutatePropertyProc;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.StandardMutateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;
import static org.neo4j.gds.scaling.ScalePropertiesProc.SCALE_PROPERTIES_DESCRIPTION;

@GdsCallable(name = "gds.alpha.scaleProperties.mutate", description = SCALE_PROPERTIES_DESCRIPTION, executionMode = MUTATE_NODE_PROPERTY)
public class ScalePropertiesMutateProc extends MutatePropertyProc<ScaleProperties, ScaleProperties.Result, ScalePropertiesMutateProc.MutateResult, ScalePropertiesMutateConfig> {

    @Procedure("gds.alpha.scaleProperties.mutate")
    @Description(SCALE_PROPERTIES_DESCRIPTION)
    public Stream<MutateResult> mutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return mutate(compute(graphName, configuration));
    }

    @Override
    protected NodePropertyValues nodeProperties(ComputationResult<ScaleProperties, ScaleProperties.Result, ScalePropertiesMutateConfig> computationResult) {
        return ScalePropertiesProc.nodeProperties(computationResult);
    }

    @Override
    protected ScalePropertiesMutateConfig newConfig(String username, CypherMapWrapper config) {
        return ScalePropertiesMutateConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<ScaleProperties, ScalePropertiesMutateConfig> algorithmFactory() {
        return new ScalePropertiesFactory<>();
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(
        ComputationResult<ScaleProperties, ScaleProperties.Result, ScalePropertiesMutateConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return new MutateResult.Builder().withScalerStatistics(computeResult.result().scalerStatistics());
    }

    public static final class MutateResult extends StandardMutateResult {

        public final Map<String, Map<String, List<Double>>> scalerStatistics;
        public final long nodePropertiesWritten;

        MutateResult(
            Map<String, Map<String, List<Double>>> scalerStatistics,
            long preProcessingMillis,
            long computeMillis,
            long mutateMillis,
            long nodePropertiesWritten,
            Map<String, Object> configuration
        ) {
            super(
                preProcessingMillis,
                computeMillis,
                0L,
                mutateMillis,
                configuration
            );
            this.scalerStatistics = scalerStatistics;
            this.nodePropertiesWritten = nodePropertiesWritten;
        }

        static class Builder extends AbstractResultBuilder<MutateResult> {

            private Map<String, Map<String, List<Double>>> scalerStatistics;

            Builder withScalerStatistics(Map<String, Map<String, List<Double>>> stats) {
                this.scalerStatistics = stats;
                return this;
            }

            @Override
            public MutateResult build() {
                return new MutateResult(
                    scalerStatistics,
                    preProcessingMillis,
                    computeMillis,
                    mutateMillis,
                    nodePropertiesWritten,
                    config.toMap()
                );
            }
        }
    }
}
