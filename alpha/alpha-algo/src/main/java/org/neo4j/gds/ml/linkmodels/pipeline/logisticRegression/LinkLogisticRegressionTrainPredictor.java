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
package org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression;

import org.neo4j.gds.ml.core.functions.Sigmoid;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

/**
 * Responsible for producing predictions on a specific graph.
 * Instances should not be reused between different graphs.
 */
public class LinkLogisticRegressionTrainPredictor {
    private final Matrix weights;
    private final HugeObjectArray<double[]> linkFeatures;

    public LinkLogisticRegressionTrainPredictor(
        LinkLogisticRegressionData modelData,
        HugeObjectArray<double[]> linkFeatures
    ) {
        this.weights = modelData.weights().data();
        this.linkFeatures = linkFeatures;
    }

    public double predictedProbabilities(long relationshipIdx) {
        var features = linkFeatures.get(relationshipIdx);
        var affinity = 0D;
        for (int i = 0; i < features.length; i++) {
            affinity += weights.dataAt(i) * features[i];
        }
        return Sigmoid.sigmoid(affinity);
    }
}
