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
package org.neo4j.gds.models.randomforest;

import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.paged.HugeByteArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.decisiontree.ClassificationDecisionTreeTrain;
import org.neo4j.gds.decisiontree.DecisionTreeLoss;
import org.neo4j.gds.decisiontree.DecisionTreePredict;
import org.neo4j.gds.decisiontree.DecisionTreeTrainConfigImpl;
import org.neo4j.gds.decisiontree.FeatureBagger;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;

import java.util.Random;

public class ClassificationRandomForestTrain<LOSS extends DecisionTreeLoss> {

    private final LOSS lossFunction;
    private final HugeObjectArray<double[]> allFeatureVectors;
    private final LocalIdMap classIdMap;
    private final RandomForestTrainConfig config;
    private final int concurrency;
    private final HugeLongArray allLabels;

    public ClassificationRandomForestTrain(
        LOSS lossFunction,
        HugeObjectArray<double[]> allFeatureVectors,
        int concurrency,
        LocalIdMap classIdMap,
        HugeLongArray allLabels,
        RandomForestTrainConfig config
    ) {
        this.lossFunction = lossFunction;
        this.allFeatureVectors = allFeatureVectors;
        this.classIdMap = classIdMap;
        this.config = config;
        this.concurrency = concurrency;
        this.allLabels = allLabels;
    }

    public ClassificationRandomForestTrainResult train() {
        int numberOfDecisionTrees = config.numberOfDecisionTrees();
        var decisionTrees = new DecisionTreePredict[numberOfDecisionTrees];
        var bootstrappedDatasets = new HugeByteArray[numberOfDecisionTrees];

        var tasks = ParallelUtil.tasks(numberOfDecisionTrees, index -> () -> {
            var decisionTreeTrainConfig = DecisionTreeTrainConfigImpl.builder()
                .maxDepth(config.maxDepth())
                .minSplitSize(config.minSplitSize())
                .randomSeed(config.randomSeed().map(seed -> seed + index)).build();

            var random = config.randomSeed()
                .map(seed -> new Random(seed + index))
                .orElseGet(Random::new);

            var featureBagger = new FeatureBagger(random, allFeatureVectors.get(0).length, config.featureBaggingRatio());

            var decisionTree = new ClassificationDecisionTreeTrain<>(
                lossFunction,
                allFeatureVectors,
                allLabels,
                classIdMap,
                decisionTreeTrainConfig,
                featureBagger,
                config.numFeatureVectorsRatio()
            );
            decisionTrees[index] = decisionTree.train();
            bootstrappedDatasets[index] = decisionTree.bootstrappedDataset();
        });
        ParallelUtil.runWithConcurrency(this.concurrency, tasks, Pools.DEFAULT);

        return ImmutableClassificationRandomForestTrainResult.of(
            new ClassificationRandomForestPredict(decisionTrees, classIdMap, concurrency),
            bootstrappedDatasets
        );
    }
}
