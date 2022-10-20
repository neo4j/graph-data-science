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
package org.neo4j.gds.ml.models.mlp;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.batch.RangeBatch;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Vector;
import org.neo4j.gds.ml.models.FeaturesFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MLPClassifierObjectiveTest {

    @Test
    void shouldCalculateCrossEntropyLoss() {

        //Fixed classifierData to avoid random weights&biases generation, in order to unit test objective function
        class MLPObjectiveTestClassifierData implements MLPClassifierData {
            @Override
            public int numberOfClasses() {
                return 3;
            }
            @Override
            public int featureDimension() {
                return 3;
            }
            @Override
            public List<Weights<Matrix>> weights() {
                return List.of(new Weights<>(Matrix.create(0.1,6,3)), new Weights<>(Matrix.create(0.1,3,6)));
            }
            @Override
            public List<Weights<Vector>> biases() {
                return List.of(new Weights<>(Vector.create(0.1,6)), new Weights<>(Vector.create(0.1,3)));
            }
            @Override
            public int depth() {
                return 3;
            }
        }

        var classifier = new MLPClassifier(new MLPObjectiveTestClassifierData());
        var featuresHOA = HugeObjectArray.of(
            new double[]{1,1,1},
            new double[]{1,1,2},
            new double[]{1,1,3}
        );
        var features = FeaturesFactory.wrap(featuresHOA);
        var labels = HugeIntArray.of(1,2,3);


        var objective = new MLPClassifierObjective(classifier, features, labels, 0.1, 0);
        var batch = new RangeBatch(0,2,3);
        var ctx = new ComputationContext();

        var crossEntropy = objective.crossEntropyLoss(batch);
        var penalty = objective.penaltyForBatch(batch, 3);
        var loss = objective.loss(batch, 3);

        var crossEntropyValue = ctx.forward(crossEntropy).value();
        var penaltyValue = ctx.forward(penalty).value();
        var lossValue = ctx.forward(loss).value();

        assertThat(crossEntropyValue).isCloseTo(1.09861228866811, Offset.offset(1e-08));
        assertThat(penaltyValue).isCloseTo(0.024000000000000014, Offset.offset(1e-08));
        assertThat(lossValue).isCloseTo(1.12261228866811, Offset.offset(1e-08));
    }

}
