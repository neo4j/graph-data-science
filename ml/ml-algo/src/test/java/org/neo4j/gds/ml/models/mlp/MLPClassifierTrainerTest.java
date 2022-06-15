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
import org.neo4j.gds.TestFeatures;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.LogLevel;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class MLPClassifierTrainerTest {

    @Test
    void shouldTrainMLPClassifier() {
        var trainer = new MLPClassifierTrainer(
            2,
            MLPClassifierTrainConfig.DEFAULT,
            ProgressTracker.NULL_TRACKER,
            LogLevel.INFO,
            TerminationFlag.RUNNING_TRUE,
            1
        );

//        var features = FeaturesFactory.wrap(HugeObjectArray.of(
//            new double[]{0.5, -0.5, 0.5, -0.5},
//            new double[]{1, -1, 1, -1},
//            new double[]{2, -2, 2, -2}
//        ));
        var random = new Random(42L);
        double[][] features = new double[5][4];
        for (int i = 0; i < features.length; i++) {
            for (int j = 0; j < features[i].length; j++) {
                features[i][j] = random.nextDouble();
            }
        }
        var testFeatures = new TestFeatures(features);

        var classifier = trainer.train(testFeatures,
            HugeIntArray.of(0,1,1,0,1),
            ReadOnlyHugeLongArray.of(HugeLongArray.of(0,1,2,3,4))
        );

        assertThat(classifier.numberOfClasses()).isEqualTo(2);
        assertThat(classifier.data().featureDimension()).isEqualTo(4);
        assertThat(classifier.data().inputWeights().data().data()).containsExactly(
            new double[]{0.1, 0.2, 0.3},
            Offset.offset(1e-08)
        );
        assertThat(classifier.data().inputBias().data().data()).containsExactly(
            new double[]{1,-1},
            Offset.offset(1e-08)
        );

    }

}