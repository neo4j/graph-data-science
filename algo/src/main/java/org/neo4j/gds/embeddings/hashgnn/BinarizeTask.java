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
package org.neo4j.gds.embeddings.hashgnn;

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.features.FeatureConsumer;
import org.neo4j.gds.ml.core.features.FeatureExtraction;
import org.neo4j.gds.ml.core.features.FeatureExtractor;
import org.neo4j.gds.ml.util.ShuffleUtil;

import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
import java.util.stream.Collectors;

class BinarizeTask implements Runnable {
    private final Partition partition;
    private final HugeObjectArray<HugeAtomicBitSet> truncatedFeatures;
    private final List<FeatureExtractor> featureExtractors;
    private final int[][] propertyEmbeddings;
    private final FeatureBinarizationConfig binarizationConfig;
    private final ProgressTracker progressTracker;
    private long totalNumFeatures;

    BinarizeTask(
        Partition partition,
        HashGNNConfig config,
        HugeObjectArray<HugeAtomicBitSet> truncatedFeatures,
        List<FeatureExtractor> featureExtractors,
        int[][] propertyEmbeddings,
        ProgressTracker progressTracker
    ) {
        this.partition = partition;
        this.binarizationConfig = config.binarizeFeatures().orElseThrow();
        this.truncatedFeatures = truncatedFeatures;
        this.featureExtractors = featureExtractors;
        this.propertyEmbeddings = propertyEmbeddings;
        this.progressTracker = progressTracker;
    }

    static HugeObjectArray<HugeAtomicBitSet> compute(
        Graph graph,
        List<Partition> partition,
        HashGNNConfig config,
        SplittableRandom rng,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag,
        MutableLong totalNumFeaturesOutput
    ) {
        progressTracker.beginSubTask("Binarize node property features");

        var featureExtractors = FeatureExtraction.propertyExtractors(
            graph,
            config.featureProperties()
        );

        var inputDimension = FeatureExtraction.featureCount(featureExtractors);
        var propertyEmbeddings = embedProperties(config, rng, inputDimension);

        var truncatedFeatures = HugeObjectArray.newArray(HugeAtomicBitSet.class, graph.nodeCount());

        var tasks = partition.stream()
            .map(p -> new BinarizeTask(
                p,
                config,
                truncatedFeatures,
                featureExtractors,
                propertyEmbeddings,
                progressTracker
            ))
            .collect(Collectors.toList());
        RunWithConcurrency.builder()
            .concurrency(config.concurrency())
            .tasks(tasks)
            .terminationFlag(terminationFlag)
            .run();

        totalNumFeaturesOutput.add(tasks.stream().mapToLong(BinarizeTask::totalNumFeatures).sum());

        progressTracker.endSubTask("Binarize node property features");

        return truncatedFeatures;
    }

    // creates a sparse projection array with one row per input feature
    // (input features vector for each node is the concatenation of the node's properties)
    // the first half of each row contains indices of positive output features in the projected space
    // the second half of each row contains indices of negative output features in the projected space
    // this array is used embed the properties themselves from inputDimension to embeddingDimension dimensions.
    public static int[][] embedProperties(HashGNNConfig config, SplittableRandom rng, int inputDimension) {
        var binarizationConfig = config.binarizeFeatures().orElseThrow();
        var permutation = new int[binarizationConfig.dimension()];
        Arrays.setAll(permutation, i -> i);

        var propertyEmbeddings = new int[inputDimension][];

        for (int inputFeature = 0; inputFeature < inputDimension; inputFeature++) {
            ShuffleUtil.shuffleArray(permutation, rng);
            propertyEmbeddings[inputFeature] = new int[2 * binarizationConfig.densityLevel()];
            for (int feature = 0; feature < 2 * binarizationConfig.densityLevel(); feature++) {
                propertyEmbeddings[inputFeature][feature] = permutation[feature];
            }
        }
        return propertyEmbeddings;
    }

    @Override
    public void run() {
        partition.consume(nodeId -> {
            var featureVector = new float[binarizationConfig.dimension()];
            FeatureExtraction.extract(nodeId, -1, featureExtractors, new FeatureConsumer() {
                @Override
                public void acceptScalar(long nodeOffset, int offset, double value) {
                    for (int feature = 0; feature < binarizationConfig.densityLevel(); feature++) {
                        int positiveFeature = propertyEmbeddings[offset][feature];
                        featureVector[positiveFeature] += value;
                    }

                    for (int feature = binarizationConfig.densityLevel(); feature < 2 * binarizationConfig.densityLevel(); feature++) {
                        int negativeFeature = propertyEmbeddings[offset][feature];
                        featureVector[negativeFeature] -= value;

                    }
                }

                @Override
                public void acceptArray(long nodeOffset, int offset, double[] values) {
                    for (int inputFeatureOffset = 0; inputFeatureOffset < values.length; inputFeatureOffset++) {
                        for (int feature = 0; feature < binarizationConfig.densityLevel(); feature++) {
                            int positiveFeature = propertyEmbeddings[offset + inputFeatureOffset][feature];
                            featureVector[positiveFeature] += values[inputFeatureOffset];
                        }
                        for (int feature = binarizationConfig.densityLevel(); feature < 2 * binarizationConfig.densityLevel(); feature++) {
                            int negativeFeature = propertyEmbeddings[offset + inputFeatureOffset][feature];
                            featureVector[negativeFeature] -= values[inputFeatureOffset];
                        }
                    }
                }
            });

            var bitSet = HugeAtomicBitSet.create(binarizationConfig.dimension());
            for (int feature = 0; feature < featureVector.length; feature++) {
                if (featureVector[feature] > 0) {
                    bitSet.set(feature);
                }
            }
            totalNumFeatures += bitSet.cardinality();
            truncatedFeatures.set(nodeId, bitSet);
        });

        progressTracker.logProgress(partition.nodeCount());
    }

    public long totalNumFeatures() {
        return totalNumFeatures;
    }

}
