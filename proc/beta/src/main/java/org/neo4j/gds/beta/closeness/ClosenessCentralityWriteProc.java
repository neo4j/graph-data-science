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
package org.neo4j.gds.beta.closeness;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.WriteProc;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.validation.ValidationConfiguration;
import org.neo4j.gds.result.AbstractCentralityResultBuilder;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.StandardWriteResult;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.beta.closeness.ClosenessCentralityProc.DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.WRITE_NODE_PROPERTY;
import static org.neo4j.procedure.Mode.WRITE;

@GdsCallable(name = "gds.beta.closeness.write", description = DESCRIPTION, executionMode = WRITE_NODE_PROPERTY)
public class ClosenessCentralityWriteProc extends WriteProc<ClosenessCentrality, ClosenessCentralityResult, ClosenessCentralityWriteProc.WriteResult, ClosenessCentralityWriteConfig> {

    @Override
    public String name() {
        return "ClosenessCentrality";
    }

    @Procedure(value = "gds.beta.closeness.write", mode = WRITE)
    @Description(DESCRIPTION)
    public Stream<WriteResult> write(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return write(compute(graphName, configuration));
    }

    @Override
    protected ClosenessCentralityWriteConfig newConfig(String username, CypherMapWrapper config) {
        return ClosenessCentralityWriteConfig.of(config);
    }

    @Override
    public ValidationConfiguration<ClosenessCentralityWriteConfig> validationConfig() {
        return ClosenessCentralityProc.getValidationConfig();
    }

    @Override
    public GraphAlgorithmFactory<ClosenessCentrality, ClosenessCentralityWriteConfig> algorithmFactory() {
        return ClosenessCentralityProc.algorithmFactory();
    }

    @Override
    protected NodePropertyValues nodeProperties(ComputationResult<ClosenessCentrality, ClosenessCentralityResult, ClosenessCentralityWriteConfig> computationResult) {
        return ClosenessCentralityProc.nodeProperties(computationResult);
    }

    @Override
    protected AbstractResultBuilder<WriteResult> resultBuilder(
        ComputationResult<ClosenessCentrality, ClosenessCentralityResult, ClosenessCentralityWriteConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return ClosenessCentralityProc.resultBuilder(
            new WriteResult.Builder(executionContext.callContext(), computeResult.config().concurrency()).withWriteProperty(computeResult
                .config()
                .writeProperty()),
            computeResult
        );
    }

    @SuppressWarnings("unused")
    public static final class WriteResult extends StandardWriteResult {

        public final long nodePropertiesWritten;
        public final String writeProperty;
        public final Map<String, Object> centralityDistribution;

        WriteResult(
            long nodePropertiesWritten,
            long preProcessingMillis,
            long computeMillis,
            long postProcessingMillis,
            long writeMillis,
            String writeProperty,
            @Nullable Map<String, Object> centralityDistribution,
            Map<String, Object> config
        ) {
            super(preProcessingMillis, computeMillis, postProcessingMillis, writeMillis, config);
            this.writeProperty = writeProperty;
            this.centralityDistribution = centralityDistribution;
            this.nodePropertiesWritten = nodePropertiesWritten;
        }

        static final class Builder extends AbstractCentralityResultBuilder<WriteResult> {
            public String writeProperty;

            private Builder(ProcedureCallContext callContext, int concurrency) {
                super(callContext, concurrency);
            }

            public Builder withWriteProperty(String writeProperty) {
                this.writeProperty = writeProperty;
                return this;
            }

            @Override
            public WriteResult buildResult() {
                return new WriteResult(
                    nodePropertiesWritten,
                    preProcessingMillis,
                    computeMillis,
                    postProcessingMillis,
                    writeMillis,
                    writeProperty,
                    centralityHistogram,
                    config.toMap()
                );
            }
        }
    }
}
