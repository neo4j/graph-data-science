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
package org.neo4j.gds.ml.nodemodels.logisticregression;

import org.neo4j.gds.core.ml.functions.Weights;
import org.neo4j.gds.core.ml.subgraph.LocalIdMap;
import org.neo4j.gds.core.ml.tensor.Matrix;
import org.neo4j.gds.core.ml.features.FeatureExtraction;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;

import java.util.List;
import java.util.TreeSet;

@ValueClass
public interface NodeLogisticRegressionData {

    static MemoryEstimation memoryEstimation(int numberOfClasses, int numberOfFeatures) {
        return MemoryEstimations.builder(NodeLogisticRegressionData.class)
            .add("classIdMap", LocalIdMap.memoryEstimation(numberOfClasses))
            .fixed("weights", Weights.sizeInBytes(numberOfClasses, numberOfFeatures))
            .build();
    }

    Weights<Matrix> weights();

    LocalIdMap classIdMap();

    static NodeLogisticRegressionData from(
        Graph graph,
        List<String> featureProperties,
        String targetProperty
    ) {
        var classIdMap = makeClassIdMap(graph, targetProperty);
        var featuresPerClass = FeatureExtraction.featureCountWithBias(graph, featureProperties);
        var weights = Weights.ofMatrix(classIdMap.size(), featuresPerClass);

        return builder()
            .classIdMap(classIdMap)
            .weights(weights)
            .build();
    }

    private static LocalIdMap makeClassIdMap(Graph graph, String targetProperty) {
        var classSet = new TreeSet<Long>();
        var classIdMap = new LocalIdMap();
        graph.forEachNode(nodeId -> {
            classSet.add(graph.nodeProperties(targetProperty).longValue(nodeId));
            return true;
        });
        classSet.forEach(classIdMap::toMapped);
        return classIdMap;
    }

    static ImmutableNodeLogisticRegressionData.Builder builder() {
        return ImmutableNodeLogisticRegressionData.builder();
    }
}
