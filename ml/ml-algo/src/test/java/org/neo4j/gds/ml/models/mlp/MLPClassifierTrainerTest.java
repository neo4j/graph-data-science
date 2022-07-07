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
            1
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
            0.19367864342013838, -0.034831284900741714, 0.2930741640658867,
            0.4092502044744987, -0.41172526053249037, 0.41097392213140144,
            -0.06693202137709806, 0.6340581094448043, -0.5325956719802887,
            0.15426710093852322, 0.38825128008800075, -0.17928039067436477,
            -0.0941915887414353, -0.663937973017749, 0.2585321423500475,

            0.5339256881128489, 0.014639864782473836, -0.5215050486551349, 0.43753688372746846, -0.30396382503820385, -0.2801557830624123,
            -0.1585616083764529, 0.26926764218806043, -0.011697916085198212, -0.12602797515926661, -0.45366783941244426, -0.34009034608632643,
            -0.10991870933083187, 0.29349709834725524, -0.3763237407922344, -0.15815798048941407, 0.08454856510756982, 0.14385636597305973,

            0.7452403459913415, -0.593019400207447, 0.6862011133737366,
            -0.6157597197274745, 0.2964865891194861, 0.6948062650777714,
            -0.7691860232183144, 0.2785204956481744, 0.5787673575481294},

            Offset.offset(1e-08));

        assertThat(biasesData).containsExactly(new double[]{0.43542041292391354, 0.10108107878700089, -0.13516679567710813, 0.10448726441087293, -0.08367332032198387, -0.24087611144737858,
            0.7695049069525925, -0.29676673652637353, 0.24618073072352434,
            0.22505918624236096, -0.11724867314260341, 0.3758868950566663},

            Offset.offset(1e-08));

    }

}
