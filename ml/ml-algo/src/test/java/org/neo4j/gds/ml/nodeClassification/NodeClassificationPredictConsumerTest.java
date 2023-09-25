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
import org.junit.jupiter.api.Test;
import org.neo4j.gds.TestClassifier;
import org.neo4j.gds.collections.ha.HugeIntArray;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.batch.BatchTransformer;
import org.neo4j.gds.ml.core.batch.ListBatch;
import org.neo4j.gds.ml.core.batch.SingletonBatch;
import org.neo4j.gds.ml.decisiontree.DecisionTreePredictor;
import org.neo4j.gds.ml.decisiontree.TreeNode;
import org.neo4j.gds.ml.models.ClassifierFactory;
import org.neo4j.gds.ml.models.Features;
import org.neo4j.gds.ml.models.FeaturesFactory;
import org.neo4j.gds.ml.models.randomforest.ImmutableRandomForestClassifierData;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class NodeClassificationPredictConsumerTest {

    @Test
    void canProducePredictions() {
        var classifier = new TestClassifier() {
            @Override
            public double[] predictProbabilities(double[] features) {
                if (Double.compare(features[0], 0) == 0) {
                    return new double[]{0.2, 0.8};
                } else {
                    return new double[]{0.7, 0.3};
                }
            }

            @Override
            public ClassifierData data() {
                throw new NotImplementedException();
            }

            @Override
            public int numberOfClasses() {
                return 2;
            }
        };
        var probabilities = HugeObjectArray.newArray(double[].class, 2);
        var predictedClasses = HugeIntArray.newArray(2);
        var predictConsumer = new NodeClassificationPredictConsumer(
            FeaturesFactory.wrap(List.of(new double[] {0.0}, new double[] {1.0})),
            BatchTransformer.IDENTITY,
            classifier,
            probabilities,
            predictedClasses,
            ProgressTracker.NULL_TRACKER
        );

        predictConsumer.accept(new SingletonBatch(0));
        predictConsumer.accept(new SingletonBatch(1));

        assertThat(probabilities.get(0)).containsExactly(0.2, 0.8);
        assertThat(probabilities.get(1)).containsExactly(0.7, 0.3);
        assertThat(predictedClasses.get(0)).isEqualTo(1);
        assertThat(predictedClasses.get(1)).isEqualTo(0);
    }

    @Test
    void predictWithRandomForest() {
        var root = new TreeNode<>(1);
        var modelData = ImmutableRandomForestClassifierData
            .builder()
            .addDecisionTree(new DecisionTreePredictor<>(root))
            .featureDimension(1)
            .numberOfClasses(2)
            .build();

        var classifier = ClassifierFactory.create(modelData);

        Features features = FeaturesFactory.wrap(List.of(new double[]{42.0}, new double[]{1337.0}));

        assertThat(classifier.predictProbabilities(features.get(0))).containsExactly(0, 1);
        assertThat(classifier.predictProbabilities(features.get(1))).containsExactly(0, 1);

        var predictedClasses = HugeIntArray.newArray(features.size());

        new NodeClassificationPredictConsumer(
            features,
            BatchTransformer.IDENTITY,
            classifier,
            null,
            predictedClasses,
            ProgressTracker.NULL_TRACKER
        ).accept(new ListBatch(new long[] {0L, 1L}));

        assertThat(predictedClasses.get(0)).isEqualTo(1);
        assertThat(predictedClasses.get(1)).isEqualTo(1);
    }
}
