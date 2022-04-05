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
package org.neo4j.gds.ml.models.linearregression;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.batch.SingletonBatch;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.core.tensor.Tensor;
import org.neo4j.gds.ml.core.tensor.Vector;
import org.neo4j.gds.ml.models.Features;
import org.neo4j.gds.ml.models.FeaturesFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class LinearRegressionObjectiveTest {

    @Test
    void testLoss() {
        int featureDimension = 1;
        Features features = FeaturesFactory.wrap(
            Stream.of(1.0, 2.5, 3.5).map(i -> new double[]{i}).collect(Collectors.toList())
        );

        var objective = new LinearRegressionObjective(
            features,
            HugeDoubleArray.of(1.0, 2.5, 3.5)
        );

        Variable<Scalar> loss = objective.loss(new SingletonBatch(1), 10);
        double lossValue = loss.apply(new ComputationContext()).value();

        assertThat(lossValue).isCloseTo(0.0, Offset.offset(1e-10));

        List<Weights<? extends Tensor<?>>> weights = objective.weights();

        assertThat(weights).hasSize(2);
        assertThat(weights.get(0).data()).isEqualTo(Vector.create(0D, featureDimension));
        assertThat(weights.get(1).data()).isEqualTo(new Scalar(0D));
    }

}
