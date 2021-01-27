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

import org.neo4j.gds.embeddings.graphsage.ddl4j.ComputationContext;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixConstant;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Sigmoid;
import org.neo4j.gds.ml.Predictor;
import org.neo4j.gds.ml.batch.Batch;
import org.neo4j.graphalgo.api.Graph;

public class LinkLogisticRegressionPredictor extends LinkLogisticRegressionBase
    implements Predictor<double[], LinkLogisticRegressionData> {

    public LinkLogisticRegressionPredictor(LinkLogisticRegressionData modelData) {
        super(modelData);
    }

    @Override
    public LinkLogisticRegressionData modelData() {
        return modelData;
    }

    @Override
    public double[] predict(Graph graph, Batch batch) {
        ComputationContext ctx = new ComputationContext();
        MatrixConstant features = features(graph, batch);
        return ctx.forward(predictions(features)).data();
    }

    public double predictedProbability(Graph graph, long sourceId, long targetId) {
        var weightsArray = modelData.weights().data().data();
        var features = features(graph, sourceId, targetId);
        var affinity = 0;
        for (int i = 0; i < weightsArray.length - 1; i++) {
            affinity += weightsArray[i] * features[i];
        }
        var bias = weightsArray[weightsArray.length - 1];
        return Sigmoid.sigmoid(affinity + bias);
    }
}
