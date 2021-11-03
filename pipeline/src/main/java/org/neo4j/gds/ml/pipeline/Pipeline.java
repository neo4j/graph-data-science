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
package org.neo4j.gds.ml.pipeline;

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.ToMapConvertible;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.neo4j.gds.config.MutatePropertyConfig.MUTATE_PROPERTY_KEY;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public abstract class Pipeline<FEATURE_STEP extends FeatureStep, TRAINING_CONFIG extends ToMapConvertible> implements ToMapConvertible {

    protected final List<NodePropertyStep> nodePropertySteps;
    protected final List<FEATURE_STEP> featureSteps;

    protected List<TRAINING_CONFIG> trainingParameterSpace;

    protected Pipeline(List<TRAINING_CONFIG> initialTrainingParameterSpace) {
        this.nodePropertySteps = new ArrayList<>();
        this.featureSteps = new ArrayList<>();
        this.trainingParameterSpace = initialTrainingParameterSpace;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("featurePipeline", Map.of(
            "nodePropertySteps", ToMapConvertible.toMap(nodePropertySteps),
            "featureSteps", ToMapConvertible.toMap(featureSteps)
        ));
        map.put(
            "trainingParameterSpace",
            trainingParameterSpace.stream().map(Model.Mappable::toMap).collect(Collectors.toList())
        );
        map.putAll(additionalEntries());
        return map;
    }

    protected abstract Map<String, Object> additionalEntries();

    public void validate(GraphStore graphStore, AlgoBaseConfig config) {
        var graphProperties = graphStore.nodePropertyKeys(config.nodeLabelIdentifiers(graphStore));

        var invalidProperties = featureSteps()
            .stream()
            .flatMap(step -> step.inputNodeProperties().stream())
            .filter(property -> !graphProperties.contains(property))
            .collect(Collectors.toList());

        if (!invalidProperties.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "Node properties %s defined in the LinkFeatureSteps do not exist in the graph or part of the pipeline",
                invalidProperties
            ));
        }
    }

    public void addNodePropertyStep(NodePropertyStep step) {
        validateUniqueMutateProperty(step);
        this.nodePropertySteps.add(step);
    }

    public void addFeatureStep(FEATURE_STEP featureStep) {
        this.featureSteps.add(featureStep);
    }

    public List<NodePropertyStep> nodePropertySteps() {
        return this.nodePropertySteps;
    }

    public List<FEATURE_STEP> featureSteps() {
        return this.featureSteps;
    }

    public List<TRAINING_CONFIG> trainingParameterSpace() {
        return trainingParameterSpace;
    }

    public void setTrainingParameterSpace(List<TRAINING_CONFIG> trainingConfigs) {
        this.trainingParameterSpace = trainingConfigs;
    }

    private void validateUniqueMutateProperty(NodePropertyStep step) {
        this.nodePropertySteps.forEach(nodePropertyStep -> {
            var newMutatePropertyName = step.config().get(MUTATE_PROPERTY_KEY);
            var existingMutatePropertyName = nodePropertyStep.config().get(MUTATE_PROPERTY_KEY);
            if (newMutatePropertyName.equals(existingMutatePropertyName)) {
                throw new IllegalArgumentException(formatWithLocale(
                    "The value of `%s` is expected to be unique, but %s was already specified in the %s procedure.",
                    MUTATE_PROPERTY_KEY,
                    newMutatePropertyName,
                    nodePropertyStep.procMethod().getName()
                ));
            }
        });
    }
}
