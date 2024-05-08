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
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.gds.TestClassifier;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.models.Features;
import org.neo4j.gds.ml.models.FeaturesFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(SoftAssertionsExtension.class)
class ParallelNodeClassifierTest {

    private ParallelNodeClassifier predictor;
    private ParallelNodeClassifier parallelPredictor;
    private int numberOfPredictions;
    private TestClassifier classifier;
    private Features features;

    @BeforeEach
    void setUp() {
        var featureDimension = 4;
        var random = new Random(42);

        this.numberOfPredictions = 20;
        List<double[]> featuresList = IntStream
            .range(0, numberOfPredictions)
            .mapToObj(i -> random.doubles(featureDimension, -1, 1).toArray())
            .collect(Collectors.toList());

        this.features = FeaturesFactory.wrap(featuresList);

        var classMap = LocalIdMap.of(LongStream.range(0, featureDimension).toArray());

        this.classifier = new TestClassifier() {

            @Override
            public double[] predictProbabilities(double[] features) {
                double sum = Arrays.stream(features).sum();
                return Arrays.stream(features).map(i -> i / sum).toArray();
            }

            @Override
            public ClassifierData data() {
                throw new NotImplementedException();
            }

            @Override
            public int numberOfClasses() {
                return featureDimension;
            }
        };

        int batchSize = 1;
        this.predictor = new ParallelNodeClassifier(
            classifier,
            features,
            batchSize,
            new Concurrency(1),
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER
        );

        this.parallelPredictor = new ParallelNodeClassifier(
            classifier,
            features,
            batchSize,
            new Concurrency(4),
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER
        );
    }

    @Test
    void testOnEvaluationSet(SoftAssertions softly) {
        var evaluationSet = ReadOnlyHugeLongArray.of(4, 7, 13, 17, 1);
        var predictions = predictor.predict(evaluationSet);
        var parallelPredictions = parallelPredictor.predict(evaluationSet);

        var expectedClasses = HugeLongArray.of(2, 0, 1, 3, 1);

        assertThat(predictions.size()).isEqualTo(parallelPredictions.size());

        for (long i = 0; i < predictions.size(); i++) {
            softly.assertThat(predictions.get(i)).isEqualTo(expectedClasses.get(i));
            softly.assertThat(predictions.get(i)).isEqualTo(parallelPredictions.get(i));
        }
    }

    @Test
    void testOnWithProbabilities(SoftAssertions softly) {
        var probabilities = HugeObjectArray.newArray(double[].class, numberOfPredictions);
        var parallelProbabilities = HugeObjectArray.newArray(double[].class, numberOfPredictions);

        var predictions = predictor.predict(probabilities);
        var parallelPredictions = parallelPredictor.predict(parallelProbabilities);

        assertThat(predictions.size()).isEqualTo(parallelPredictions.size());

        for (long i = 0; i < predictions.size(); i++) {
            softly.assertThat(probabilities.get(i)).isEqualTo(classifier.predictProbabilities(features.get(i)));
            softly.assertThat(probabilities.get(i)).isEqualTo(parallelProbabilities.get(i));
        }
    }

}
