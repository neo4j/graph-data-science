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
package org.neo4j.gds.ml.linkmodels.logisticregression;

import org.immutables.value.Value;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.ml.LinkFeatureCombiner;
import org.neo4j.gds.ml.features.FeatureExtraction;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;

import java.util.List;

@ValueClass
public interface LinkLogisticRegressionData {

    Weights<Matrix> weights();

    LinkFeatureCombiner linkFeatureCombiner();

    List<String> featureProperties();

    @Value.Derived
    default int linkFeatureDimension() {
        return linkFeatureCombiner().linkFeatureDimension(nodeFeatureDimension());
    }

    int nodeFeatureDimension();

    static LinkLogisticRegressionData from(
        Graph graph,
        List<String> featureProperties,
        LinkFeatureCombiner linkFeatureCombiner
    ) {
        var nodeFeatureDimension = FeatureExtraction.featureCount(
            FeatureExtraction.propertyExtractors(graph, featureProperties)
        );
        var numberOfFeatures = linkFeatureCombiner.linkFeatureDimension(nodeFeatureDimension);
        var weights = Weights.ofMatrix(1, numberOfFeatures);

        return builder()
            .weights(weights)
            .linkFeatureCombiner(linkFeatureCombiner)
            .featureProperties(featureProperties)
            .nodeFeatureDimension(nodeFeatureDimension)
            .build();
    }

    static ImmutableLinkLogisticRegressionData.Builder builder() {
        return ImmutableLinkLogisticRegressionData.builder();
    }
}
