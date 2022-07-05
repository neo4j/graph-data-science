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

import static org.assertj.core.api.Assertions.assertThat;

class MLPClassifierTrainerTest {
    @Test
    void shouldTrainMLPClassifier() {
        var trainer = new MLPClassifierTrainer(
            3,
            MLPClassifierTrainConfig.of(Map.of("hiddenLayerSizes", List.of(3))),
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

        assertThat(weightsData).containsExactly(new double[]{0.3742928742667889, -0.09267558873141771, -0.03957981691897919, 0.4696514681074816,
            0.07005287594529619, 0.18436928817228612, 0.12112573508213205, 0.023682004356143527,
            0.1313380469025015, 0.36380259358761624, 0.5881314452692645, 0.6723783873849645,
            -0.5526855635002939, -0.5395298871806025, -0.3040335066183237, -0.012537893706217162,

            0.22468184066315902, 0.48547797877052584, -0.27373378268062304, -0.05271766149808099,
            -0.5990212647594454, -0.724680534869662, -0.7664298515960261, -0.6670691413258012,
            0.7543932497629441, 0.11159106680775155, 0.3255273402296144, -0.08927437603451849},

            Offset.offset(1e-08));

        assertThat(biasesData).containsExactly(new double[]{
            0.20606770751781522, -5.657595534070115E-4, -0.002188323708040147, 0.0724123068580541,

            0.20394753938809024, 0.19027149349147854, 3.6469482110609635E-4},

            Offset.offset(1e-08));

    }

}
