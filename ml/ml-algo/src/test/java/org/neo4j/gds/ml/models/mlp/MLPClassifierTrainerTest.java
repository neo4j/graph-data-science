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
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.LogLevel;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.models.FeaturesFactory;

import static org.assertj.core.api.Assertions.assertThat;

class MLPClassifierTrainerTest {
    @Test
    void shouldTrainMLPClassifier() {
        var trainer = new MLPClassifierTrainer(
            3,
            MLPClassifierTrainConfig.DEFAULT,
            ProgressTracker.NULL_TRACKER,
            LogLevel.INFO,
            TerminationFlag.RUNNING_TRUE,
            1
        );

        var features = FeaturesFactory.wrap(HugeObjectArray.of(
            new double[]{0, 0, 0, 0},
            new double[]{1, 1, 1, 1},
            new double[]{2, 2, 2, 2}
        ));

        var classifier = trainer.train(features,
            HugeIntArray.of(0,1,2),
            ReadOnlyHugeLongArray.of(HugeLongArray.of(0,1,2))
        );

        assertThat(classifier.numberOfClasses()).isEqualTo(3);
        assertThat(classifier.data().featureDimension()).isEqualTo(4);
        assertThat(classifier.data().inputWeights().data().data()).containsExactly(
            new double[]{0.29764252720748297, 0.23493600330932676, -0.29469265588519583, -0.33943973808037514,
                0.3357362252422812, 0.6720691797622892, -0.08395433109107091, -0.2155254692061487,
                0.03945668522357953, 0.49094182364238215, 0.6838773288708201, 0.0010429659543524268,
                0.35342071085232274, -0.1604186647033804, -0.45625573205400827, 0.1334309235888843},
            Offset.offset(1e-08)
        );
        assertThat(classifier.data().inputBias().data().data()).containsExactly(
            new double[]{0.20606770751781522, -5.657595534070115E-4, -0.002188323708040147, 0.0724123068580541},
            Offset.offset(1e-08)
        );
        assertThat(classifier.data().outputWeights().data().data()).containsExactly(
            new double[]{0.436148874745391, 0.14971951697007693, -0.36820487404763386, -0.22878729472693624,
                0.26656769819031106, 0.6766669555233438, -0.08567435921149674, -0.4063956816876862,
                -0.16236717778228524, 0.39414339181121, 0.669421629644905, -0.17332715738888166},
            Offset.offset(1e-08)
        );
        assertThat(classifier.data().outputBias().data().data()).containsExactly(
            new double[]{0.20394753938809024, 0.19027149349147854, 3.6469482110609635E-4},
            Offset.offset(1e-08)
        );

    }

}
