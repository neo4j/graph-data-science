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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.config.ToMapConvertible;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.neo4j.gds.config.MutatePropertyConfig.MUTATE_PROPERTY_KEY;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public abstract class PipelineBuilder<FEATURE_STEP extends FeatureStep> implements ToMapConvertible {

    protected final List<NodePropertyStep> nodePropertySteps;
    protected final List<FEATURE_STEP> featureSteps;

    protected PipelineBuilder() {
        this.nodePropertySteps = new ArrayList<>();
        this.featureSteps = new ArrayList<>();
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("featurePipeline", Map.of(
            "nodePropertySteps", ToMapConvertible.toMap(nodePropertySteps),
            "featureSteps", ToMapConvertible.toMap(featureSteps)
        ));
        map.putAll(additionalEntries());
        return map;
    }

    public abstract void validate(Graph graph);

    protected abstract Map<String, Object> additionalEntries();

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
