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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.ml.Objective;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.batch.Batch;
import org.neo4j.gds.ml.core.functions.Constant;
import org.neo4j.gds.ml.core.functions.ConstantScale;
import org.neo4j.gds.ml.core.functions.CrossEntropyLoss;
import org.neo4j.gds.ml.core.functions.ElementSum;
import org.neo4j.gds.ml.core.functions.L2NormSquaredOld;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.core.tensor.Tensor;
import org.neo4j.gds.ml.core.tensor.Vector;

import java.util.List;

public class NodeLogisticRegressionObjective implements Objective<NodeLogisticRegressionData> {

    private final String targetProperty;
    private final Graph graph;
    private final double penalty;
    private final NodeLogisticRegressionPredictor predictor;

    public NodeLogisticRegressionObjective(
        Graph graph,
        NodeLogisticRegressionPredictor predictor,
        String targetProperty,
        double penalty
    ) {
        this.predictor = predictor;
        this.targetProperty = targetProperty;
        this.graph = graph;
        this.penalty = penalty;
    }

    @Override
    public List<Weights<? extends Tensor<?>>> weights() {
        return List.of(modelData().weights());
    }

    @Override
    public Variable<Scalar> loss(Batch batch, long trainSize) {
        var targets = makeTargets(batch);
        var predictions = predictor.predictionsVariable(graph, batch);
        var unpenalizedLoss = new CrossEntropyLoss(
            predictions,
            targets
        );
        var penaltyVariable = new ConstantScale<>(new L2NormSquaredOld(modelData().weights()), batch.size() * penalty / trainSize);
        return new ElementSum(List.of(unpenalizedLoss, penaltyVariable));
    }

    private Constant<Vector> makeTargets(Batch batch) {
        Iterable<Long> nodeIds = batch.nodeIds();
        int numberOfNodes = batch.size();
        double[] targets = new double[numberOfNodes];
        int nodeOffset = 0;
        var localIdMap = modelData().classIdMap();
        var targetNodeProperty = graph.nodeProperties(targetProperty);
        for (long nodeId : nodeIds) {
            var targetValue = targetNodeProperty.doubleValue(nodeId);
            targets[nodeOffset] = localIdMap.toMapped((long) targetValue);
            nodeOffset++;
        }
        return Constant.vector(targets);
    }

    @Override
    public NodeLogisticRegressionData modelData() {
        return predictor.modelData();
    }
}
