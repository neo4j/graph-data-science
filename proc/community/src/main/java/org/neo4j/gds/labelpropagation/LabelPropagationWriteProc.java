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
package org.neo4j.gds.labelpropagation;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.WriteProc;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.WRITE_NODE_PROPERTY;
import static org.neo4j.gds.labelpropagation.LabelPropagation.LABEL_PROPAGATION_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

@GdsCallable(name = "gds.labelPropagation.write", description = LABEL_PROPAGATION_DESCRIPTION, executionMode = WRITE_NODE_PROPERTY)
public class LabelPropagationWriteProc extends WriteProc<LabelPropagation, LabelPropagationResult, LabelPropagationWriteProc.WriteResult, LabelPropagationWriteConfig> {

    @Procedure(value = "gds.labelPropagation.write", mode = WRITE)
    @Description(LABEL_PROPAGATION_DESCRIPTION)
    public Stream<WriteResult> write(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return write(compute(graphName, configuration));
    }

    @Procedure(value = "gds.labelPropagation.write.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected NodePropertyValues nodeProperties(ComputationResult<LabelPropagation, LabelPropagationResult, LabelPropagationWriteConfig> computationResult) {
        return LabelPropagationProc.nodeProperties(
            computationResult,
            computationResult.config().writeProperty()
        );
    }

    @Override
    protected AbstractResultBuilder<WriteResult> resultBuilder(
        ComputationResult<LabelPropagation, LabelPropagationResult, LabelPropagationWriteConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return LabelPropagationProc.resultBuilder(
            new WriteResult.Builder(executionContext.returnColumns(), computeResult.config().concurrency()),
            computeResult
        );
    }

    @Override
    public LabelPropagationWriteConfig newConfig(String username, CypherMapWrapper config) {
        return LabelPropagationWriteConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<LabelPropagation, LabelPropagationWriteConfig> algorithmFactory() {
        return new LabelPropagationFactory<>();
    }

    @SuppressWarnings("unused")
    public static class WriteResult extends StatsResult {

        public final long writeMillis;
        public final long nodePropertiesWritten;

        WriteResult(
            long ranIterations,
            boolean didConverge,
            long communityCount,
            Map<String, Object> communityDistribution,
            long preProcessingMillis,
            long computeMillis,
            long postProcessingMillis,
            long writeMillis,
            long nodePropertiesWritten,
            Map<String, Object> configuration
        ) {
            super(
                ranIterations,
                didConverge,
                communityCount,
                communityDistribution,
                preProcessingMillis,
                computeMillis,
                postProcessingMillis,
                configuration
            );
            this.writeMillis = writeMillis;
            this.nodePropertiesWritten = nodePropertiesWritten;
        }


        static class Builder extends LabelPropagationResultBuilder<WriteResult> {

            Builder(ProcedureReturnColumns returnColumns, int concurrency) {
                super(returnColumns, concurrency);
            }

            @Override
            protected WriteResult buildResult() {
                return new WriteResult(
                    ranIterations,
                    didConverge,
                    maybeCommunityCount.orElse(0L),
                    communityHistogramOrNull(),
                    preProcessingMillis,
                    computeMillis,
                    postProcessingDuration,
                    writeMillis,
                    nodePropertiesWritten,
                    config.toMap()
                );
            }
        }
    }

}
