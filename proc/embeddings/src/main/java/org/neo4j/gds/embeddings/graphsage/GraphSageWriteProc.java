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
import org.neo4j.gds.GraphStoreValidation;
import org.neo4j.gds.ValidationConfig;
import org.neo4j.gds.WriteProc;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageAlgorithmFactory;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageModelResolver;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageWriteConfig;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.embeddings.graphsage.GraphSageCompanion.GRAPHSAGE_DESCRIPTION;
import static org.neo4j.gds.embeddings.graphsage.GraphSageCompanion.getActualConfig;
import static org.neo4j.gds.embeddings.graphsage.GraphSageCompanion.getNodeProperties;
import static org.neo4j.gds.embeddings.graphsage.GraphSageCompanion.injectRelationshipWeightPropertyFromModel;

public class GraphSageWriteProc extends WriteProc<GraphSage, GraphSage.GraphSageResult, GraphSageWriteProc.GraphSageWriteResult, GraphSageWriteConfig> {

    @Context
    public ModelCatalog modelCatalog;

    @Procedure(name = "gds.beta.graphSage.write", mode = Mode.WRITE)
    @Description(GRAPHSAGE_DESCRIPTION)
    public Stream<GraphSageWriteResult> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        injectRelationshipWeightPropertyFromModel(getActualConfig(graphNameOrConfig, configuration), modelCatalog, username.username());

        return write(compute(graphNameOrConfig, configuration));
    }

    @Procedure(value = "gds.beta.graphSage.write.estimate", mode = Mode.READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        injectRelationshipWeightPropertyFromModel(getActualConfig(graphNameOrConfig, configuration), modelCatalog, username.username());

        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    public ValidationConfig<GraphSageWriteConfig> getValidationConfig() {
        return new ValidationConfig<>() {
            @Override
            public void validateConfigsAfterLoad(
                GraphStore graphStore, GraphCreateConfig graphCreateConfig, GraphSageWriteConfig config
            ) {
                var model = GraphSageModelResolver.resolveModel(modelCatalog, config.username(), config.modelName());
                GraphStoreValidation.validate(graphStore, model.trainConfig());
            }
        };
    }

    @Override
    protected GraphSageWriteConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return GraphSageWriteConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<GraphSage, GraphSageWriteConfig> algorithmFactory() {
        return new GraphSageAlgorithmFactory<>(modelCatalog);
    }

    @Override
    protected NodeProperties nodeProperties(ComputationResult<GraphSage, GraphSage.GraphSageResult, GraphSageWriteConfig> computationResult) {
        return getNodeProperties(computationResult);
    }

    @Override
    protected AbstractResultBuilder<GraphSageWriteResult> resultBuilder(ComputationResult<GraphSage, GraphSage.GraphSageResult, GraphSageWriteConfig> computeResult) {
        return new GraphSageWriteResult.Builder();
    }

    @SuppressWarnings("unused")
    public static final class GraphSageWriteResult {

        public final long nodeCount;
        public final long nodePropertiesWritten;
        public final long createMillis;
        public final long computeMillis;
        public final long writeMillis;
        public final Map<String, Object> configuration;

        GraphSageWriteResult(
            long nodeCount,
            long nodePropertiesWritten,
            long createMillis,
            long computeMillis,
            long writeMillis,
            Map<String, Object> configuration
        ) {
            this.nodeCount = nodeCount;
            this.nodePropertiesWritten = nodePropertiesWritten;
            this.createMillis = createMillis;
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
                    createMillis,
                    computeMillis,
                    writeMillis,
                    config.toMap()
                );
            }
        }
    }
}
