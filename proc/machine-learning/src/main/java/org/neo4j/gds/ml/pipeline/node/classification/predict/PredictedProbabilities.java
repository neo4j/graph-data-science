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
package org.neo4j.gds.ml.pipeline.node.classification.predict;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.write.NodeProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

final class PredictedProbabilities {
    private PredictedProbabilities() {}

    @NotNull
    static List<NodeProperty> asProperties(
        Optional<NodeClassificationPipelineResult> result,
        String propertyName,
        Optional<String> predictedProbabilityProperty
    ) {
        if (result.isEmpty()) return Collections.emptyList();

        var nodeProperties = new ArrayList<NodeProperty>();

        var classProperties = result.get().predictedClasses().asNodeProperties();
        nodeProperties.add(NodeProperty.of(propertyName, classProperties));

        if (result.get().predictedProbabilities().isEmpty()) return nodeProperties;

        NodePropertyValues nodePropertyValues = result.get().predictedProbabilities().get().asNodeProperties();
        nodeProperties.add(NodeProperty.of(predictedProbabilityProperty.orElseThrow(), nodePropertyValues));

        return nodeProperties;
    }
}
