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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.TestClassifier;
import org.neo4j.gds.TestFeatures;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.decisiontree.DecisionTreePredict;
import org.neo4j.gds.decisiontree.TreeNode;
import org.neo4j.gds.ml.core.batch.BatchTransformer;
import org.neo4j.gds.ml.core.batch.ListBatch;
import org.neo4j.gds.ml.core.batch.SingletonBatch;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.models.ClassifierFactory;
import org.neo4j.gds.models.Features;
import org.neo4j.gds.models.FeaturesFactory;
import org.neo4j.gds.models.randomforest.ImmutableRandomForestData;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestLocalIdMap.identityMapOf;

class NodeClassificationPredictConsumerTest {

    @Test
    void canProducePredictions() {
        var classifier = new TestClassifier() {
            @Override
            public LocalIdMap classIdMap() {
                return identityMapOf(0, 1);
            }

            @Override
            public double[] predictProbabilities(long id, Features features) {
                if (id == 0) {
                    return new double[]{0.2, 0.8};
                } else {
                    return new double[]{0.7, 0.3};
                }
            }

            @Override
            public ClassifierData data() {
                return null;
            }
        };
        var probabilities = HugeObjectArray.newArray(double[].class, 2);
        var predictedClasses = HugeLongArray.newArray(2);
        var predictConsumer = new NodeClassificationPredictConsumer(
            new TestFeatures(new double[0][0]),
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
        LocalIdMap classMapping = LocalIdMap.of(5, 10);

        var root = new TreeNode<>(classMapping.toMapped(10));
        var modelData = ImmutableRandomForestData
            .builder()
            .addDecisionTree(new DecisionTreePredict<>(root))
            .featureDimension(1)
            .classIdMap(classMapping)
            .build();

        var classifier = ClassifierFactory.create(modelData);

        Features features = FeaturesFactory.wrap(List.of(new double[]{42.0}, new double[]{1337.0}));

        assertThat(classifier.predictProbabilities(0, features)).containsExactly(0, 1);
        assertThat(classifier.predictProbabilities(1, features)).containsExactly(0, 1);

        var predictedClasses = HugeLongArray.newArray(features.size());

        new NodeClassificationPredictConsumer(
            features,
            BatchTransformer.IDENTITY,
            classifier,
            null,
            predictedClasses,
            ProgressTracker.NULL_TRACKER
        ).accept(new ListBatch(List.of(0L, 1L)));

        assertThat(predictedClasses.get(0)).isEqualTo(10);
        assertThat(predictedClasses.get(1)).isEqualTo(10);
    }
}
