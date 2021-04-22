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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.subgraph.LocalIdMap;
import org.neo4j.gds.ml.batch.LazyBatch;
import org.neo4j.gds.ml.features.FeatureExtractionBaseTest;
import org.neo4j.graphalgo.api.Graph;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class NodeLogisticRegressionPredictorFailTest extends FeatureExtractionBaseTest {

    @Override
    public void makeExtractions(Graph graph) {
        var weights = new Weights<>(Matrix.fill(0.0, 5, 4));
        var classIdMap = new LocalIdMap();
        var featureProperties = List.of("a", "b");
        var modelData = ImmutableNodeLogisticRegressionData.of(weights, classIdMap);
        var predictor = new NodeLogisticRegressionPredictor(modelData, featureProperties);
        var batch = new LazyBatch(0, 4, 4);
        predictor.predict(graph, batch);
    }

    @Test
    public void shouldEstimateMemoryUsage() {
        var memoryUsageInBytes = NodeLogisticRegressionPredictor.sizeOfPredictionsVariableInBytes(100, 10, 10);

        int memoryUsageOfFeatureExtractors = 240; // 24 bytes * number of features
        int memoryUsageOfFeatureMatrix = 8032; // 8 bytes * batch size * number of features + 32
        int memoryUsageOfMatrixMultiplication = 8000; // 8 bytes per double * batchSize * numberOfClasses
        int memoryUsageOfSoftMax = memoryUsageOfMatrixMultiplication; // computed over the matrix multiplication, it requires an equally-sized matrix
        assertThat(memoryUsageInBytes).isEqualTo(memoryUsageOfFeatureExtractors + memoryUsageOfFeatureMatrix + memoryUsageOfFeatureMatrix + memoryUsageOfSoftMax);
    }
}
