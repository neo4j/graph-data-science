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
package org.neo4j.gds.embeddings.fastrp;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.WriteProc;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.embeddings.fastrp.FastRPCompanion.DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

@GdsCallable(name = "gds.fastRP.write", description = DESCRIPTION, executionMode = ExecutionMode.WRITE_NODE_PROPERTY)
public class FastRPWriteProc extends WriteProc<FastRP, FastRP.FastRPResult, FastRPWriteProc.WriteResult, FastRPWriteConfig> {

    @Procedure(value = "gds.fastRP.write", mode = WRITE)
    @Description(DESCRIPTION)
    public Stream<WriteResult> write(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<FastRP, FastRP.FastRPResult, FastRPWriteConfig> computationResult = compute(
            graphName,
            configuration
        );
        return write(computationResult);
    }

    @Procedure(value = "gds.fastRP.write.estimate", mode = READ)
    @Description(DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected FastRPWriteConfig newConfig(String username, CypherMapWrapper config) {
        return FastRPWriteConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<FastRP, FastRPWriteConfig> algorithmFactory() {
        return new FastRPFactory<>();
    }

    @Override
    protected NodePropertyValues nodeProperties(ComputationResult<FastRP, FastRP.FastRPResult, FastRPWriteConfig> computationResult) {
        return FastRPCompanion.nodeProperties(computationResult);
    }

    @Override
    protected AbstractResultBuilder<WriteResult> resultBuilder(
        ComputationResult<FastRP, FastRP.FastRPResult, FastRPWriteConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return new WriteResult.Builder();
    }

    @SuppressWarnings("unused")
    public static final class WriteResult {

        public final long nodeCount;
        public final long nodePropertiesWritten;
        public final long preProcessingMillis;
        public final long computeMillis;
        public final long writeMillis;
        public final Map<String, Object> configuration;

        WriteResult(
            long nodeCount,
            long nodePropertiesWritten,
            long preProcessingMillis,
            long computeMillis,
            long writeMillis,
            Map<String, Object> configuration
        ) {
            this.nodeCount = nodeCount;
            this.nodePropertiesWritten = nodePropertiesWritten;
            this.preProcessingMillis = preProcessingMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.configuration = configuration;
        }

        static class Builder extends AbstractResultBuilder<WriteResult> {

            @Override
            public WriteResult build() {
                return new WriteResult(
                    nodeCount,
                    nodePropertiesWritten,
                    preProcessingMillis,
                    computeMillis,
                    writeMillis,
                    config.toMap()
                );
            }
        }
    }

}
