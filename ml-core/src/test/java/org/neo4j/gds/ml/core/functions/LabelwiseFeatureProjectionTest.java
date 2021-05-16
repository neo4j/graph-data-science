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
package org.neo4j.gds.ml.core.functions;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.FiniteDifferenceTest;
import org.neo4j.gds.ml.core.helper.L2Norm;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Tensor;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class LabelwiseFeatureProjectionTest implements FiniteDifferenceTest {

    private static final int PROJECTED_FEATURE_SIZE = 5;

    @GdlGraph
    private static final String GDL =
        "CREATE" +
        "  (r1:Restaurant {numEmployees: 5.0, rating: 2.0})" +
        ", (d1:Dish {numIngredients: 3.0, rating: 5.0})" +
        ", (c1:Customer {numPurchases: 15.0}) " +
        ", (r1)-[:SERVES]->(d1)" +
        ", (c1)-[:ORDERED {rating: 4.0}]->(d1)";

    @Inject
    TestGraph graph;

    @Test
    void shouldMultiplyWeights() {
        var nodeIds = new long[]{0, 1, 2};
        var labels = new NodeLabel[nodeIds.length];
        for (int i = 0; i < nodeIds.length; i++) {
            labels[i] = graph.nodeLabels(nodeIds[i]).stream().findFirst().get();
        }

        var features = HugeObjectArray.of(
            new double[]{5.0, 2.0},
            new double[]{3.0, 5.0},
            new double[]{15.0}
        );

        Map<NodeLabel, Weights<? extends Tensor<?>>> nodeLabelWeightsMap = makeWeights();
        var projection = new LabelwiseFeatureProjection(
            nodeIds,
            features,
            nodeLabelWeightsMap,
            PROJECTED_FEATURE_SIZE,
            labels
        );

        var actual = projection.apply(new ComputationContext());

        var expected = new Matrix(new double[]{
            5.0, 2.0, 0.0, 0.0, 0.0,
            0.0, 0.0, 3.0, 5.0, 0.0,
            0.0, 0.0, 0.0, 0.0, 15.0
        }, nodeIds.length, PROJECTED_FEATURE_SIZE);

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void shouldComputeGradient() {
        var nodeIds = new long[]{0, 1, 2};
        var labels = new NodeLabel[nodeIds.length];
        for (int i = 0; i < nodeIds.length; i++) {
            labels[i] = graph.nodeLabels(nodeIds[i]).stream().findFirst().get();
        }

        var features = HugeObjectArray.of(
            new double[]{5.0, 2.0},
            new double[]{3.0, 5.0},
            new double[]{15.0}
        );
        Map<NodeLabel, Weights<? extends Tensor<?>>> nodeLabelWeightsMap = makeWeights();
        var projection = new LabelwiseFeatureProjection(
            nodeIds,
            features,
            nodeLabelWeightsMap,
            PROJECTED_FEATURE_SIZE,
            labels
        );
        var loss = new L2Norm(projection);
        List<Weights<?>> arrayList = new ArrayList<>(nodeLabelWeightsMap.values());
        finiteDifferenceShouldApproximateGradient(arrayList, loss);
    }

    @Override
    public double tolerance() {
        return 1E-5;
    }

    @Override
    public double epsilon() {
        return 1E-6;
    }

    private Map<NodeLabel, Weights<? extends Tensor<?>>> makeWeights() {
        // design the new feature space so it has 5 features which are in order: restaurant.numEmployees, restaurant.rating, dish.numIngredients, dish.rating, customer.numPurchases
        // this makes it so that its easy to inspect that projection is correct, because any vector is just inserted at some offset into the projected vector
        Weights<Matrix> restaurantWeights = new Weights<>(new Matrix(
            new double[]{
                1.0, 0.0,
                0.0, 1.0,
                0.0, 0.0,
                0.0, 0.0,
                0.0, 0.0
            },
            5,
            2
        ));
        Weights<Matrix> dishWeights = new Weights<>(new Matrix(
            new double[]{
                0.0, 0.0,
                0.0, 0.0,
                1.0, 0.0,
                0.0, 1.0,
                0.0, 0.0
            },
            5,
            2
        ));
        Weights<Matrix> customerWeights = new Weights<>(new Matrix(
            new double[]{
                0.0,
                0.0,
                0.0,
                0.0,
                1.0,
            },
            5,
            1
        ));
        return Map.of(
            NodeLabel.of("Restaurant"), restaurantWeights,
            NodeLabel.of("Dish"), dishWeights,
            NodeLabel.of("Customer"), customerWeights
        );
    }

}
