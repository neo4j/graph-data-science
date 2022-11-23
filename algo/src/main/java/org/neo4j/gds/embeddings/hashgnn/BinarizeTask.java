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

import java.util.List;
import java.util.SplittableRandom;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class BinarizeTask implements Runnable {
    private final Partition partition;
    private final HugeObjectArray<HugeAtomicBitSet> truncatedFeatures;
    private final List<FeatureExtractor> featureExtractors;
    private final double[][] propertyEmbeddings;

    private final double threshold;
    private final BinarizeFeaturesConfig binarizationConfig;
    private final ProgressTracker progressTracker;
    private long totalNumFeatures;

    private double scalarProductSum;

    private double scalarProductSumOfSquares;

    BinarizeTask(
        Partition partition,
        HashGNNConfig config,
        HugeObjectArray<HugeAtomicBitSet> truncatedFeatures,
        List<FeatureExtractor> featureExtractors,
        double[][] propertyEmbeddings,
        ProgressTracker progressTracker
    ) {
        this.partition = partition;
        this.binarizationConfig = config.binarizeFeatures().orElseThrow();
        this.threshold = binarizationConfig.threshold();
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

        var binarizationConfig = config.binarizeFeatures().orElseThrow();

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

        var squaredSum = tasks.stream().mapToDouble(BinarizeTask::scalarProductSumOfSquares).sum();
        var sum = tasks.stream().mapToDouble(BinarizeTask::scalarProductSum).sum();
        long exampleCount = graph.nodeCount() * binarizationConfig.dimension();
        var avg = sum / exampleCount;

        var variance = (squaredSum - exampleCount * avg * avg) / exampleCount;
        var std = Math.sqrt(variance);

        progressTracker.logInfo(formatWithLocale("Hyperplane scalar products have mean %.4f and standard deviation %.4f. A threshold for binarization may be set to the average plus a few standard deviations.", avg, std));

        progressTracker.endSubTask("Binarize node property features");

        return truncatedFeatures;
    }
    // creates a random projection vector for each feature
    // (input features vector for each node is the concatenation of the node's properties)
    // this array is used embed the properties themselves from inputDimension to embeddingDimension dimensions.
    public static double[][] embedProperties(HashGNNConfig config, SplittableRandom rng, int inputDimension) {
        var binarizationConfig = config.binarizeFeatures().orElseThrow();
        var propertyEmbeddings = new double[inputDimension][];

        for (int inputFeature = 0; inputFeature < inputDimension; inputFeature++) {
            propertyEmbeddings[inputFeature] = new double[binarizationConfig.dimension()];
            for (int feature = 0; feature < binarizationConfig.dimension(); feature++) {
                // Box-muller transformation to generate gaussian
                double matrixValue = Math.sqrt(-2*Math.log(rng.nextDouble(0.0, 1.0))) * Math.cos(2*Math.PI * rng.nextDouble(0.0, 1.0));
                propertyEmbeddings[inputFeature][feature] = matrixValue;
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
                    for (int feature = 0; feature < binarizationConfig.dimension(); feature++) {
                        double featureValue = propertyEmbeddings[offset][feature];
                        featureVector[feature] += value * featureValue;
                    }
                }

                @Override
                public void acceptArray(long nodeOffset, int offset, double[] values) {
                    for (int inputFeatureOffset = 0; inputFeatureOffset < values.length; inputFeatureOffset++) {
                        double value = values[inputFeatureOffset];
                        for (int feature = 0; feature < binarizationConfig.dimension(); feature++) {
                            double featureValue = propertyEmbeddings[offset + inputFeatureOffset][feature];
                            featureVector[feature] += value * featureValue;
                        }
                    }
                }
            });

            var featureSet = round(featureVector);
            totalNumFeatures += featureSet.cardinality();
            truncatedFeatures.set(nodeId, featureSet);
        });

        progressTracker.logProgress(partition.nodeCount());
    }

    private HugeAtomicBitSet round(float[] floatVector) {
        var bitset = HugeAtomicBitSet.create(floatVector.length);
        for (int feature = 0; feature < floatVector.length; feature++) {
            var scalarProduct = floatVector[feature];
            scalarProductSum += scalarProduct;
            scalarProductSumOfSquares += scalarProduct * scalarProduct;
            if (scalarProduct > threshold) {
                bitset.set(feature);
            }
        }
        return bitset;
    }

    public long totalNumFeatures() {
        return totalNumFeatures;
    }

    public double scalarProductSum() {
        return scalarProductSum;
    }
    public double scalarProductSumOfSquares() {
        return scalarProductSumOfSquares;
    }

}
