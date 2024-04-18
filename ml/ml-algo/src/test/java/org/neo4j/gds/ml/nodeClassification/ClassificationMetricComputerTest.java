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
package org.neo4j.gds.ml.nodeClassification;

import org.apache.commons.lang3.NotImplementedException;
import org.assertj.core.data.Offset;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.TestClassifier;
import org.neo4j.gds.collections.LongMultiSet;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.collections.ha.HugeIntArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.metrics.classification.F1Weighted;
import org.neo4j.gds.ml.models.Features;
import org.neo4j.gds.ml.models.FeaturesFactory;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

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
        var classCounts = new LongMultiSet();
        classCounts.add(0L, 2);
        classCounts.add(1L, 1);
        classCounts.add(3L, 1);
        var idMap = LocalIdMap.of(1, 0, 3);
        var targets = HugeIntArray.of(idMap.toMapped(firstTarget), idMap.toMapped(0), idMap.toMapped(3), idMap.toMapped(0));

        Features features = FeaturesFactory.wrap(Stream
            .of(0, 1, 2, 3)
            .map(i -> new double[]{i})
            .collect(Collectors.toList()));

        var classifier = new TestClassifier() {

            @Override
            public double[] predictProbabilities(double[] features) {
                switch ((int) features[0]) {
                    case 0:
                        return new double[]{0.8, 0.1, 0.1};
                    case 1:
                        return new double[]{0.2, 0.6, 0.2};
                    case 2:
                        return new double[]{0.1, 0.1, 0.8};
                    case 3:
                        return new double[]{0.0, 1.0, 0.0};
                }
                throw new IllegalStateException("we only got 4 nodes");
            }

            @Override
            public ClassifierData data() {
                throw new NotImplementedException();
            }

            @Override
            public int numberOfClasses() {
                return 3;
            }
        };

        var classificationMetricComputer = ClassificationMetricComputer.forEvaluationSet(
            features,
            targets,
            ReadOnlyHugeLongArray.of(0, 1, 2, 3),
            classifier,
            new Concurrency(1),
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER
        );

        assertThat(classificationMetricComputer.score(new F1Weighted(idMap, classCounts))).isCloseTo(expectedF1Score, Offset.offset(1e-7));
    }
}
