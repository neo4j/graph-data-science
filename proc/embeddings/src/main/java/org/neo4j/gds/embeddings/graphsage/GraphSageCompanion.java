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

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.api.properties.nodes.DoubleArrayNodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageBaseConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageModelResolver;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.GraphStoreValidation;
import org.neo4j.gds.executor.validation.AfterLoadValidation;
import org.neo4j.gds.executor.validation.ValidationConfiguration;
import org.neo4j.gds.utils.StringFormatting;

import java.util.List;
import java.util.Map;

import static org.neo4j.gds.config.RelationshipWeightConfig.RELATIONSHIP_WEIGHT_PROPERTY;
import static org.neo4j.gds.model.ModelConfig.MODEL_NAME_KEY;

public final class GraphSageCompanion {

    public static final String GRAPHSAGE_DESCRIPTION = "The GraphSage algorithm inductively computes embeddings for nodes based on a their features and neighborhoods.";

    private GraphSageCompanion() {}

    @NotNull
    public static <T extends GraphSageBaseConfig> DoubleArrayNodePropertyValues getNodeProperties(ComputationResult<GraphSage, GraphSage.GraphSageResult, T> computationResult) {
        var size = computationResult.graph().nodeCount();
        var embeddings = computationResult.result().embeddings();

        return new DoubleArrayNodePropertyValues() {
            @Override
            public long size() {
                return size;
            }

            @Override
            public double[] doubleArrayValue(long nodeId) {

                return embeddings.get(nodeId);
            }
        };
    }

    static <CONFIG extends GraphSageBaseConfig> ValidationConfiguration<CONFIG> getValidationConfig(ModelCatalog catalog) {
        return new ValidationConfiguration<>() {
            @Override
            public List<AfterLoadValidation<CONFIG>> afterLoadValidations() {
                return List.of(
                    (graphStore, graphProjectConfig, graphSageConfig) -> {
                        Model<ModelData, GraphSageTrainConfig, GraphSageModelTrainer.GraphSageTrainMetrics> model = GraphSageModelResolver.resolveModel(
                            catalog,
                            graphSageConfig.username(),
                            graphSageConfig.modelName()
                        );
                        GraphStoreValidation.validate(graphStore, model.trainConfig());
                    }
                );
            }
        };
    }

    static Map<String, Object> getActualConfig(Object graphNameOrConfig, Map<String, Object> maybeConfig) {
        return graphNameOrConfig instanceof Map
            ? (Map<String, Object>) graphNameOrConfig
            : maybeConfig;
    }

    // FIXME:
    //  For AlgoBaseProc to provide the correct graph, this needs to match with the `trainConfig.relationshipWeightProperty`.
    //  Ideally we would resolve the graph from the GraphStore after the model was resolved. But as GraphSage also supports anonymous loading, this is not possible with the current AlgoBaseProc.
    //  For now we resolve the model at proc level and set the corresponding relationshipWeightProperty (thus no default)
    public static void injectRelationshipWeightPropertyFromModel(Map<String, Object> configuration, ModelCatalog modelCatalog, String username) {
        if (configuration.containsKey(RELATIONSHIP_WEIGHT_PROPERTY)) {
            throw new IllegalArgumentException(StringFormatting.formatWithLocale(
                "The parameter `%s` cannot be overwritten during embedding computation. Instead, specify this parameter in the configuration of the model training.",
                RELATIONSHIP_WEIGHT_PROPERTY
            ));
        }

        String modelName = CypherMapWrapper.create(configuration).requireString(MODEL_NAME_KEY);

        var trainProperty = GraphSageModelResolver
            .resolveModel(modelCatalog, username, modelName).trainConfig().relationshipWeightProperty();
        configuration.put(RELATIONSHIP_WEIGHT_PROPERTY, trainProperty);
    }

}
