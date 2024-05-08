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
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.collections.ha.HugeIntArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.LogLevel;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class MLPClassifierTrainerTest {
    @Test
    void shouldTrainMLPClassifier() {
        var trainer = new MLPClassifierTrainer(
            3,
            MLPClassifierTrainConfig.of(Map.of("hiddenLayerSizes", List.of(6,3))),
            Optional.of(42L),
            ProgressTracker.NULL_TRACKER,
            LogLevel.INFO,
            TerminationFlag.RUNNING_TRUE,
            new Concurrency(1)
        );

        double[][] featuresArr = new double[5][3];
        for (int i = 0; i < featuresArr.length; i++) {
            for (int j = 0; j < featuresArr[i].length; j++) {
                featuresArr[i][j] = ((double) i) / (j + 1);
            }
        }

        var features = new TestFeatures(featuresArr);

        var classifier = trainer.train(features,
            HugeIntArray.of(0, 1, 1, 0, 2),
            ReadOnlyHugeLongArray.of(HugeLongArray.of(0,1,2,3,4))
        );

        assertThat(classifier.numberOfClasses()).isEqualTo(3);
        assertThat(classifier.data().featureDimension()).isEqualTo(3);

        var weightsData = classifier.data().weights().stream().map(weightMatrix -> weightMatrix.data().data()).flatMapToDouble(Arrays::stream).toArray();
        var biasesData = classifier.data().biases().stream().map(biasMatrix -> biasMatrix.data().data()).flatMapToDouble(Arrays::stream).toArray();

        assertThat(weightsData).containsExactly(new double[]{-0.7372657516983959, 0.3079293950195503, 0.305114900060949,
            0.29978329591843544, 0.07127331441958636, 0.3991787102083366,
            0.35565060145005795, -0.46532486303542725, 0.35737432014996895,
            0.027530091387412234, 0.7285202128608667, -0.4381335779126728,
            0.0630714963659577, 0.2970556813696429, -0.27047598353851565,
            -0.0941915887414353, -0.663937973017749, 0.2585321423500475,

            0.39216564975405266, 0.17476701678763587, -0.2413119339614262, 0.19097445079411837, -0.17830200296551527, -0.24087611144737858,
            0.18065292064316218, -0.5006648414767616, 0.05522208535751216, -0.5593999633538338, 0.33040245315084527, -0.24998532555684688,
            -0.17979515787796813, -0.06877010098113616, 0.12188080425874777, -0.18728144449315798, 0.4237931944902021, -0.5626186682164449,

            0.22863210923280752, -0.0691509777350231, 0.3145074032647546,
            0.7541508812238437, 0.20939512884730727, -0.2529111683075559,
            -0.14501801881880644, 0.08878756893643547, -0.714558132318431},

            Offset.offset(1e-08));

        assertThat(biasesData).containsExactly(new double[]{0.42079347613759627, 0.11138002240332029, -0.4783116555061014, 0.5295434580113074, -0.29127368800633635, -0.2801557830624123,
                0.8628671623470577, -0.29676673652637353, 0.2649994661218771,
                0.7412322974329772, -0.5443043943189265, 0.6806274074443027},

        Offset.offset(1e-08));

    }

}
