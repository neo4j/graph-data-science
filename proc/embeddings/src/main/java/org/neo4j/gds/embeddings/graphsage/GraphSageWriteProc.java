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
package org.neo4j.gds.embeddings.graphsage;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.GraphStoreAlgorithmFactory;
import org.neo4j.gds.WriteProc;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageAlgorithmFactory;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageWriteConfig;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.validation.ValidationConfiguration;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.embeddings.graphsage.GraphSageCompanion.GRAPHSAGE_DESCRIPTION;
import static org.neo4j.gds.embeddings.graphsage.GraphSageCompanion.getNodeProperties;
import static org.neo4j.gds.executor.ExecutionMode.WRITE_NODE_PROPERTY;

@GdsCallable(name = "gds.beta.graphSage.write", description = GRAPHSAGE_DESCRIPTION, executionMode = WRITE_NODE_PROPERTY)
public class GraphSageWriteProc extends WriteProc<GraphSage, GraphSage.GraphSageResult, GraphSageWriteProc.GraphSageWriteResult, GraphSageWriteConfig> {

    @Procedure(name = "gds.beta.graphSage.write", mode = Mode.WRITE)
    @Description(GRAPHSAGE_DESCRIPTION)
    public Stream<GraphSageWriteResult> write(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return write(compute(graphName, configuration));
    }

    @Procedure(value = "gds.beta.graphSage.write.estimate", mode = Mode.READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    public ValidationConfiguration<GraphSageWriteConfig> validationConfig(ExecutionContext executionContext) {
        return GraphSageCompanion.getValidationConfig(modelCatalog());
    }

    @Override
    public AlgorithmSpec<GraphSage, GraphSage.GraphSageResult, GraphSageWriteConfig, Stream<GraphSageWriteResult>, AlgorithmFactory<?, GraphSage, GraphSageWriteConfig>> withModelCatalog(
        ModelCatalog modelCatalog
    ) {
        this.setModelCatalog(modelCatalog);
        return this;
    }

    @Override
    protected GraphSageWriteConfig newConfig(String username, CypherMapWrapper config) {
        return GraphSageWriteConfig.of(username, config);
    }

    @Override
    public GraphStoreAlgorithmFactory<GraphSage, GraphSageWriteConfig> algorithmFactory() {
        return new GraphSageAlgorithmFactory<>(modelCatalog());
    }

    @Override
    protected NodePropertyValues nodeProperties(ComputationResult<GraphSage, GraphSage.GraphSageResult, GraphSageWriteConfig> computationResult) {
        return getNodeProperties(computationResult);
    }

    @Override
    protected AbstractResultBuilder<GraphSageWriteResult> resultBuilder(
        ComputationResult<GraphSage, GraphSage.GraphSageResult, GraphSageWriteConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return new GraphSageWriteResult.Builder();
    }

    @SuppressWarnings("unused")
    public static final class GraphSageWriteResult {

        public final long nodeCount;
        public final long nodePropertiesWritten;
        public final long preProcessingMillis;
        public final long computeMillis;
        public final long writeMillis;
        public final Map<String, Object> configuration;

        GraphSageWriteResult(
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

        @SuppressWarnings("unused")
        static class Builder extends AbstractResultBuilder<GraphSageWriteResult> {

            @Override
            public GraphSageWriteResult build() {
                return new GraphSageWriteResult(
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
