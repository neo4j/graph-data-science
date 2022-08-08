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

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.ToMapConvertible;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.config.MutatePropertyConfig.MUTATE_PROPERTY_KEY;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public interface Pipeline<FEATURE_STEP extends FeatureStep> extends ToMapConvertible {

    List<ExecutableNodePropertyStep> nodePropertySteps();

    List<FEATURE_STEP> featureSteps();

    default void validateBeforeExecution(GraphStore graphStore, Collection<NodeLabel> nodeLabels) {
        Set<String> invalidProperties = featurePropertiesMissingFromGraph(graphStore, nodeLabels);

        nodePropertySteps().stream()
            .flatMap(step -> Stream.ofNullable((String) step.config().get(MUTATE_PROPERTY_KEY)))
            .forEach(invalidProperties::remove);

        if (!invalidProperties.isEmpty()) {
            throw Pipeline.missingNodePropertiesFromFeatureSteps(invalidProperties);
        }

        specificValidateBeforeExecution(graphStore);
    }

    void specificValidateBeforeExecution(GraphStore graphStore);

    default void validateFeatureProperties(GraphStore graphStore, Collection<NodeLabel> nodeLabels) {
        Set<String> invalidProperties = featurePropertiesMissingFromGraph(graphStore, nodeLabels);

        if (!invalidProperties.isEmpty()) {
            throw missingNodePropertiesFromFeatureSteps(invalidProperties);
        }
    }

    default Set<String> featurePropertiesMissingFromGraph(GraphStore graphStore, Collection<NodeLabel> nodeLabels) {
        var graphProperties = graphStore.nodePropertyKeys(nodeLabels);

        return featureSteps()
            .stream()
            .flatMap(step -> step.inputNodeProperties().stream())
            .filter(property -> !graphProperties.contains(property))
            .collect(Collectors.toSet());
    }

    static IllegalArgumentException missingNodePropertiesFromFeatureSteps(Set<String> invalidProperties) {
        return new IllegalArgumentException(formatWithLocale(
            "Node properties %s defined in the feature steps do not exist in the graph or part of the pipeline",
            invalidProperties.stream().sorted().collect(Collectors.toList())
        ));
    }
}
