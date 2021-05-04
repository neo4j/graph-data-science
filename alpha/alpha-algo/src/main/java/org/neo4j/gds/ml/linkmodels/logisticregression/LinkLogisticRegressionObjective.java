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

import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.gds.core.ml.Variable;
import org.neo4j.gds.core.ml.functions.ConstantScale;
import org.neo4j.gds.core.ml.functions.ElementSum;
import org.neo4j.gds.core.ml.functions.L2NormSquared;
import org.neo4j.gds.core.ml.functions.LogisticLoss;
import org.neo4j.gds.core.ml.functions.MatrixConstant;
import org.neo4j.gds.core.ml.functions.Weights;
import org.neo4j.gds.core.ml.tensor.Matrix;
import org.neo4j.gds.core.ml.tensor.Scalar;
import org.neo4j.gds.core.ml.tensor.Tensor;
import org.neo4j.gds.ml.Objective;
import org.neo4j.gds.ml.batch.Batch;
import org.neo4j.gds.ml.splitting.EdgeSplitter;
import org.neo4j.graphalgo.api.Graph;

import java.util.List;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class LinkLogisticRegressionObjective extends LinkLogisticRegressionBase implements Objective<LinkLogisticRegressionData> {
    private final Graph graph;
    private final double penalty;

    public LinkLogisticRegressionObjective(LinkLogisticRegressionData llrData, List<String> featureProperties, double penalty, Graph graph) {
        super(llrData, featureProperties);
        this.graph = graph;
        this.penalty = penalty;
    }

    @Override
    public List<Weights<? extends Tensor<?>>> weights() {
        return List.of(modelData.weights());
    }

    @Override
    public Variable<Scalar> loss(Batch batch, long trainSize) {
        MatrixConstant features = features(graph, batch);
        Variable<Matrix> predictions = predictions(features);
        var relationshipCount = new MutableInt();
        batch.nodeIds().forEach(nodeId -> relationshipCount.add(graph.degree(nodeId)));
        var rows = relationshipCount.getValue();
        double[] targets = makeTargetsArray(batch, rows);
        MatrixConstant targetVariable = new MatrixConstant(targets, rows, 1);
        var penaltyVariable = new ConstantScale<>(new L2NormSquared(modelData.weights()), rows * penalty / trainSize);
        var unpenalizedLoss = new LogisticLoss(modelData.weights(), predictions, features, targetVariable);
        return new ElementSum(List.of(unpenalizedLoss, penaltyVariable));
    }

    @Override
    public LinkLogisticRegressionData modelData() {
        return modelData;
    }

    private double[] makeTargetsArray(Batch batch, int rows) {
        var graphCopy = graph.concurrentCopy();
        double[] targets = new double[rows];
        var relationshipOffset = new MutableInt();
        batch.nodeIds().forEach(nodeId -> {
            graphCopy.forEachRelationship(nodeId, -0.66, (src, trg, val) -> {
                if (Double.compare(val, EdgeSplitter.POSITIVE) == 0) {
                    targets[relationshipOffset.getValue()] = 1.0;
                } else if (Double.compare(val, EdgeSplitter.NEGATIVE) == 0) {
                    targets[relationshipOffset.getValue()] = 0.0;
                } else {
                    throw new IllegalArgumentException(formatWithLocale(
                        "The relationship property must have value %s or %s but it has %s",
                        EdgeSplitter.NEGATIVE,
                        EdgeSplitter.POSITIVE,
                        val
                    ));
                }
                relationshipOffset.increment();
                return true;
            });
        });
        return targets;
    }
}
