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
package org.neo4j.gds.ml.pipeline.linkPipeline;

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.ToMapConvertible;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.model.ModelMetaData;
import org.neo4j.gds.core.model.ModelType;
import org.neo4j.gds.core.model.catalog.ModelMetadata;
import org.neo4j.gds.ml.pipeline.ExecutableNodePropertyStep;
import org.neo4j.gds.ml.pipeline.TrainingPipeline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.neo4j.gds.config.RelationshipWeightConfig.RELATIONSHIP_WEIGHT_PROPERTY;
import static org.neo4j.gds.model.ModelConfig.MODEL_NAME_KEY;

public class LinkPredictionTrainingPipeline extends TrainingPipeline<LinkFeatureStep> {

    public static final String PIPELINE_TYPE = "Link prediction training pipeline";
    public static final ModelType MODEL_TYPE = ModelType.LinkPrediction;

    private LinkPredictionSplitConfig splitConfig;

    public LinkPredictionTrainingPipeline() {
        super(TrainingType.CLASSIFICATION);
        this.splitConfig = LinkPredictionSplitConfig.DEFAULT_CONFIG;
    }


    @Override
    public String type() {
        return PIPELINE_TYPE;
    }

    @Override
    protected Map<String, List<Map<String, Object>>> featurePipelineDescription() {
        return Map.of(
            "nodePropertySteps", ToMapConvertible.toMap(nodePropertySteps),
            "featureSteps", ToMapConvertible.toMap(featureSteps)
        );
    }

    @Override
    protected Map<String, Object> additionalEntries() {
        return Map.of(
            "splitConfig", splitConfig.toMap()
        );
    }

    public LinkPredictionSplitConfig splitConfig() {
        return splitConfig;
    }

    public void setSplitConfig(LinkPredictionSplitConfig splitConfig) {
        this.splitConfig = splitConfig;
    }

    @Override
    public void specificValidateBeforeExecution(GraphStore graphStore) {
        if (featureSteps().isEmpty()) {
            throw new IllegalArgumentException(
                "Training a Link prediction pipeline requires at least one feature. You can add features with the procedure `gds.beta.pipeline.linkPrediction.addFeature`.");
        }
    }

    public Map<String, List<String>> tasksByRelationshipProperty(ModelCatalog modelCatalog, String username) {
        Map<String, List<String>> tasksByRelationshipProperty = new HashMap<>();

        for (ExecutableNodePropertyStep existingStep : nodePropertySteps()) {
            Map<String, Object> config = existingStep.config();
            Optional<String> maybeProperty = extractRelationshipProperty(config, modelCatalog, username);

            maybeProperty.ifPresent(property -> {
                var tasks = tasksByRelationshipProperty.computeIfAbsent(property, key -> new ArrayList<>());
                tasks.add(existingStep.procName());
            });
        }

        return tasksByRelationshipProperty;
    }

    private Optional<String> extractRelationshipProperty(
        Map<String, Object> config,
        ModelCatalog modelCatalog,
        String username
    ) {
        if (config.containsKey(RELATIONSHIP_WEIGHT_PROPERTY)) {
            var existingProperty = (String) config.get(RELATIONSHIP_WEIGHT_PROPERTY);
            return Optional.of(existingProperty);
        } else if (config.containsKey(MODEL_NAME_KEY)) {
            var modelName = (String) config.get(MODEL_NAME_KEY);
            return metadataFor(modelCatalog, username, modelName)
                .map(ModelMetadata::trainConfig)
                .map(trainConfig -> trainConfig.get(RELATIONSHIP_WEIGHT_PROPERTY))
                .filter(String.class::isInstance)
                .map(String.class::cast);
        }

        return Optional.empty();
    }

    private static Optional<ModelMetadata> metadataFor(
        ModelCatalog modelCatalog,
        String username,
        String modelName
    ) {
        return modelCatalog.getAllMetadata(username)
            .filter(modelMetadata -> modelMetadata.name().equals(modelName))
            .findFirst()
            .or(() ->
                modelCatalog.getAllMetadata()
                    .filter(
                        modelMetadata -> modelMetadata.name().equals(modelName)
                                || modelMetadata.name().equals(modelName + ModelMetaData.PUBLIC_MODEL_SUFFIX))
                    .filter(ModelMetadata::published)
                    .findFirst()
            );
    }

    public Optional<String> relationshipWeightProperty(ModelCatalog modelCatalog, String username) {
        var relationshipWeightPropertySet = tasksByRelationshipProperty(
            modelCatalog, username
        ).entrySet();
        return relationshipWeightPropertySet.isEmpty()
            ? Optional.empty()
            : Optional.of(relationshipWeightPropertySet.iterator().next().getKey());
    }
}
