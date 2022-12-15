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
package org.neo4j.gds.embeddings.hashgnn;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.MutatePropertyProc;
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

import static org.neo4j.gds.embeddings.hashgnn.HashGNNProcCompanion.DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.beta.hashgnn.mutate", description = DESCRIPTION, executionMode = MUTATE_NODE_PROPERTY)
public class HashGNNMutateProc extends MutatePropertyProc<HashGNN, HashGNN.HashGNNResult, HashGNNMutateProc.MutateResult, HashGNNMutateConfig> {

    @Procedure(value = "gds.beta.hashgnn.mutate", mode = READ)
    @Description(DESCRIPTION)
    public Stream<MutateResult> mutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<HashGNN, HashGNN.HashGNNResult, HashGNNMutateConfig> computationResult = compute(
            graphName,
            configuration
        );
        return mutate(computationResult);
    }

    @Procedure(value = "gds.beta.hashgnn.mutate.estimate", mode = READ)
    @Description(DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected NodePropertyValues nodeProperties(ComputationResult<HashGNN, HashGNN.HashGNNResult, HashGNNMutateConfig> computationResult) {
        return HashGNNProcCompanion.getNodeProperties(computationResult);
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(
        ComputationResult<HashGNN, HashGNN.HashGNNResult, HashGNNMutateConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return new MutateResult.Builder();
    }

    @Override
    protected HashGNNMutateConfig newConfig(String username, CypherMapWrapper config) {
        return HashGNNMutateConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<HashGNN, HashGNNMutateConfig> algorithmFactory() {
        return new HashGNNFactory();
    }

    @SuppressWarnings("unused")
    public static final class MutateResult {

        public final long nodePropertiesWritten;
        public final long mutateMillis;
        public final long nodeCount;
        public final long preProcessingMillis;
        public final long computeMillis;
        public final Map<String, Object> configuration;

        MutateResult(
            long nodeCount,
            long nodePropertiesWritten,
            long preProcessingMillis,
            long computeMillis,
            long mutateMillis,
            Map<String, Object> config
        ) {
            this.nodeCount = nodeCount;
            this.nodePropertiesWritten = nodePropertiesWritten;
            this.preProcessingMillis = preProcessingMillis;
            this.computeMillis = computeMillis;
            this.mutateMillis = mutateMillis;
            this.configuration = config;
        }

        static final class Builder extends AbstractResultBuilder<MutateResult> {

            @Override
            public MutateResult build() {
                return new MutateResult(
                    nodeCount,
                    nodePropertiesWritten,
                    preProcessingMillis,
                    computeMillis,
                    mutateMillis,
                    config.toMap()
                );
            }
        }
    }
}
