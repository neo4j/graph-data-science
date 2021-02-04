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
package org.neo4j.gds.ml.nodemodels.multiclasslogisticregression;

import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.subgraph.LocalIdMap;
import org.neo4j.gds.ml.features.BiasFeature;
import org.neo4j.gds.ml.features.FeatureExtraction;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;

import java.util.Collection;
import java.util.TreeSet;

@ValueClass
public interface MultiClassNLRData {

    Weights<Matrix> weights();

    LocalIdMap classIdMap();

    static MultiClassNLRData from(
        Collection<String> featureProperties,
        String targetPropertyKey,
        Graph graph
    ) {
        var classIdMap = makeClassIdMap(graph, targetPropertyKey);
        var weights = initWeights(graph, classIdMap.size(), featureProperties);
        return builder()
            .classIdMap(classIdMap)
            .weights(weights)
            .build();
    }

    private static LocalIdMap makeClassIdMap(Graph graph, String targetPropertyKey) {
        var classSet = new TreeSet<Long>();
        var classIdMap = new LocalIdMap();
        graph.forEachNode(nodeId -> {
            classSet.add(graph.nodeProperties(targetPropertyKey).longValue(nodeId));
            return true;
        });
        classSet.forEach(classIdMap::toMapped);
        return classIdMap;
    }

    private static Weights<Matrix> initWeights(Graph graph, int numberOfClasses, Collection<String> featureProperties) {
        var featureExtractors = FeatureExtraction.propertyExtractors(graph, featureProperties);
        featureExtractors.add(new BiasFeature());
        var featuresPerClass = FeatureExtraction.featureCount(featureExtractors);
        return new Weights<>(Matrix.fill(0.0, numberOfClasses, featuresPerClass));
    }

    static ImmutableMultiClassNLRData.Builder builder() {
        return ImmutableMultiClassNLRData.builder();
    }
}
