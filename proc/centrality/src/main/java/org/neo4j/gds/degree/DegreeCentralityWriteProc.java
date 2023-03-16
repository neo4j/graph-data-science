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
package org.neo4j.gds.degree;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.WriteProc;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.result.AbstractCentralityResultBuilder;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.results.StandardWriteResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.degree.DegreeCentrality.DEGREE_CENTRALITY_DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.WRITE_NODE_PROPERTY;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

@GdsCallable(name = "gds.degree.write", description = DEGREE_CENTRALITY_DESCRIPTION, executionMode = WRITE_NODE_PROPERTY)
public class DegreeCentralityWriteProc extends WriteProc<DegreeCentrality, DegreeCentrality.DegreeFunction, DegreeCentralityWriteProc.WriteResult, DegreeCentralityWriteConfig> {

    @Procedure(value = "gds.degree.write", mode = WRITE)
    @Description(DEGREE_CENTRALITY_DESCRIPTION)
    public Stream<WriteResult> write(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return write(compute(graphName, configuration));
    }

    @Procedure(value = "gds.degree.write.estimate", mode = READ)
    @Description(DEGREE_CENTRALITY_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected NodePropertyValues nodeProperties(ComputationResult<DegreeCentrality, DegreeCentrality.DegreeFunction, DegreeCentralityWriteConfig> computationResult) {
        return DegreeCentralityProc.nodeProperties(computationResult);
    }

    @Override
    protected DegreeCentralityWriteConfig newConfig(String username, CypherMapWrapper config) {
        return DegreeCentralityWriteConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<DegreeCentrality, DegreeCentralityWriteConfig> algorithmFactory() {
        return new DegreeCentralityFactory<>();
    }

    @Override
    protected AbstractResultBuilder<WriteResult> resultBuilder(
        ComputationResult<DegreeCentrality, DegreeCentrality.DegreeFunction, DegreeCentralityWriteConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return DegreeCentralityProc.resultBuilder(
            new WriteResult.Builder(executionContext.returnColumns(), computeResult.config().concurrency()),
            computeResult
        );
    }

    @SuppressWarnings("unused")
    public static final class WriteResult extends StandardWriteResult {

        public final long nodePropertiesWritten;
        public final Map<String, Object> centralityDistribution;

        WriteResult(
            long nodePropertiesWritten,
            long preProcessingMillis,
            long computeMillis,
            long postProcessingMillis,
            long writeMillis,
            @Nullable Map<String, Object> centralityDistribution,
            Map<String, Object> config
        ) {
            super(preProcessingMillis, computeMillis, postProcessingMillis, writeMillis, config);
            this.nodePropertiesWritten = nodePropertiesWritten;
            this.centralityDistribution = centralityDistribution;
        }

        static class Builder extends AbstractCentralityResultBuilder<WriteResult> {

            Builder(ProcedureReturnColumns returnColumns, int concurrency) {
                super(returnColumns, concurrency);
            }

            @Override
            protected WriteResult buildResult() {
                return new WriteResult(
                    nodePropertiesWritten,
                    preProcessingMillis,
                    computeMillis,
                    postProcessingMillis,
                    writeMillis,
                    centralityHistogram,
                    config.toMap()
                );
            }
        }
    }
}
