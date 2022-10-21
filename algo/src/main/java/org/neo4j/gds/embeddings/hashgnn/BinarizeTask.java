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

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.features.FeatureConsumer;
import org.neo4j.gds.ml.core.features.FeatureExtraction;
import org.neo4j.gds.ml.core.features.FeatureExtractor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.stream.Collectors;

import static org.neo4j.gds.embeddings.hashgnn.HashGNN.hashArgMin;

class BinarizeTask implements Runnable {
    private final Partition partition;
    private final HashGNNConfig config;
    private final HugeObjectArray<BitSet> truncatedFeatures;
    private final List<FeatureExtractor> featureExtractors;
    private final int[][] propertyEmbeddings;
    private final List<int[]> hashesList;
    private final HashGNN.MinAndArgmin minAndArgMin;
    private final FeatureBinarizationConfig binarizationConfig;

    BinarizeTask(
        Partition partition,
        HashGNNConfig config,
        HugeObjectArray<BitSet> truncatedFeatures,
        List<FeatureExtractor> featureExtractors,
        int[][] propertyEmbeddings,
        List<int[]> hashesList
    ) {
        this.partition = partition;
        this.config = config;
        this.binarizationConfig = config.binarizeFeatures().get();
        this.truncatedFeatures = truncatedFeatures;
        this.featureExtractors = featureExtractors;
        this.propertyEmbeddings = propertyEmbeddings;
        this.hashesList = hashesList;
        this.minAndArgMin = new HashGNN.MinAndArgmin(Integer.MAX_VALUE, -1);
    }

    static HugeObjectArray<BitSet> compute(
        Graph graph,
        List<Partition> partition,
        HashGNNConfig config,
        SplittableRandom rng,
        long randomSeed,
        ProgressTracker progressTracker
    ) {
        progressTracker.logInfo("Starting binarization");

        var hashesList = new ArrayList<int[]>(config.embeddingDensity());
        for (int i = 0; i < config.embeddingDensity(); i++) {
            hashesList.add(HashGNN.computeHashesFromTriple(
                config.binarizeFeatures().get().dimension(),
                HashGNN.HashTriple.generate(rng)
            ));
        }

        var featureExtractors = FeatureExtraction.propertyExtractors(
            graph,
            config.featureProperties()
        );

        var inputDimension = FeatureExtraction.featureCount(featureExtractors);
        var propertyEmbeddings = embedProperties(config, randomSeed, inputDimension);

        var truncatedFeatures = HugeObjectArray.newArray(BitSet.class, graph.nodeCount());

        var tasks = partition.stream()
            .map(p -> new BinarizeTask(
                p,
                config,
                truncatedFeatures,
                FeatureExtraction.propertyExtractors(graph, config.featureProperties()),
                propertyEmbeddings,
                hashesList
            ))
            .collect(Collectors.toList());
        RunWithConcurrency.builder()
            .concurrency(config.concurrency())
            .tasks(tasks)
            .run();

        progressTracker.logInfo("Finished binarization");

        return truncatedFeatures;
    }

    private static int[][] embedProperties(HashGNNConfig config, long randomSeed, int inputDimension) {
        var binarizationConfig = config.binarizeFeatures().get();
        var rng = new Random(randomSeed);
        var permutation = new ArrayList<Integer>(binarizationConfig.dimension());
        for (int offset = 0; offset < binarizationConfig.dimension(); offset++) {
            permutation.add(offset);
        }

        var propertyEmbeddings = new int[inputDimension][];

        for (int inputFeature = 0; inputFeature < inputDimension; inputFeature++) {
            Collections.shuffle(permutation, rng);
            propertyEmbeddings[inputFeature] = new int[2 * binarizationConfig.densityLevel()];
            for (int ambientOffset = 0; ambientOffset < 2 * binarizationConfig.densityLevel(); ambientOffset++) {
                propertyEmbeddings[inputFeature][ambientOffset] = permutation.get(ambientOffset);
            }
        }
        return propertyEmbeddings;
    }

    @Override
    public void run() {
        partition.consume(nodeId -> {
            var featureVector = new float[binarizationConfig.dimension()];
            // should the second nodeId be replaced by 0 ? nodeOffset doesn't matter I guess.
            FeatureExtraction.extract(nodeId, nodeId, featureExtractors, new FeatureConsumer() {
                @Override
                public void acceptScalar(long nodeOffset, int offset, double value) {
                    for (int ambientOffset = 0; ambientOffset < 2 * binarizationConfig.densityLevel(); ambientOffset++) {
                        if (ambientOffset < binarizationConfig.densityLevel()) {
                            int positiveAmbientFeature = propertyEmbeddings[offset][ambientOffset];
                            featureVector[positiveAmbientFeature] += value;
                        } else {
                            int negativeAmbientFeature = propertyEmbeddings[offset][ambientOffset];
                            featureVector[negativeAmbientFeature] -= value;
                        }
                    }
                }

                @Override
                public void acceptArray(long nodeOffset, int offset, double[] values) {
                    for (int inputFeatureOffset = 0; inputFeatureOffset < values.length; inputFeatureOffset++) {
                        for (int ambientOffset = 0; ambientOffset < 2 * binarizationConfig.densityLevel(); ambientOffset++) {
                            if (ambientOffset < binarizationConfig.densityLevel()) {
                                int positiveAmbientFeature = propertyEmbeddings[offset + inputFeatureOffset][ambientOffset];
                                featureVector[positiveAmbientFeature] += values[inputFeatureOffset];
                            } else {
                                int negativeAmbientFeature = propertyEmbeddings[offset + inputFeatureOffset][ambientOffset];
                                featureVector[negativeAmbientFeature] -= values[inputFeatureOffset];
                            }
                        }
                    }
                }
            });

            truncatedFeatures.set(nodeId, truncate(featureVector));
        });
    }

    private BitSet truncate(float[] floatVector) {
        var bitset = new BitSet(floatVector.length);
        for (int ambientOffset = 0; ambientOffset < floatVector.length; ambientOffset++) {
            if (floatVector[ambientOffset] > 0) {
                bitset.set(ambientOffset);
            }
        }
        var sampledBitset = new BitSet(binarizationConfig.dimension());
        for (int i = 0; i < config.embeddingDensity(); i++) {
            hashArgMin(bitset, hashesList.get(i), minAndArgMin);
            if (minAndArgMin.argMin != -1) {
                sampledBitset.set(minAndArgMin.argMin);
            }
        }
        return sampledBitset;
    }

}
