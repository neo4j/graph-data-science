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
package org.neo4j.gds.ml.nodemodels;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.TestFeatures;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.models.Classifier;
import org.neo4j.gds.models.Features;
import org.openjdk.jol.util.Multiset;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestLocalIdMap.identityMapOf;
import static org.neo4j.gds.ml.nodemodels.metrics.AllClassMetric.F1_WEIGHTED;

class ClassificationMetricComputerTest {

    private static Stream<Arguments> targetAndScores() {
        return Stream.of(
            Arguments.of(0L, (2 * 4.0 / 5.0 + 1 * 0.0 + 1 * 1.0) / 4.0),
            Arguments.of(1L, (2 * 1.0 + 1 * 1.0 + 1 * 1.0) / 4.0),
            Arguments.of(3L, (2 * 1.0 + 1 * 0.0 + 1 * 2.0 / 3.0) / 4.0)
        );
    }

    @ParameterizedTest
    @MethodSource("targetAndScores")
    void shouldComputeMetrics(long firstTarget, double expectedF1Score) {
        var multiSet = new Multiset<Long>();
        multiSet.add(0L, 2);
        multiSet.add(1L, 1);
        multiSet.add(3L, 1);
        var idMap = identityMapOf(1, 0, 3);
        var targets = HugeLongArray.of(firstTarget, 0, 3, 0);

        var classificationMetricComputer = new ClassificationMetricComputer(
            List.of(F1_WEIGHTED),
            multiSet,
            TestFeatures.singleConstant(1),
            targets,
            1,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        var classifier = new Classifier() {
            @Override
            public LocalIdMap classIdMap() {
                return idMap;
            }

            @Override
            public double[] predictProbabilities(long id, Features features) {
                switch ((int) id) {
                    case 0:
                        return new double[]{0.8, 0.1, 0.1};
                    case 1:
                        return new double[]{0.2, 0.6, 0.2};
                    case 2:
                        return new double[]{0.1, 0.1, 0.8};
                    case 3:
                        return new double[]{0.0, 1.0, 0.0};
                }
                Assertions.fail("we only got 4 nodes");
                return null;
            }

            @Override
            public ClassifierData data() {
                return null;
            }
        };

        var metrics = classificationMetricComputer.computeMetrics(
            HugeLongArray.of(0, 1, 2, 3),
            classifier
        );

        assertThat(metrics.get(F1_WEIGHTED)).isCloseTo(expectedF1Score, Offset.offset(1e-7));
    }
}
