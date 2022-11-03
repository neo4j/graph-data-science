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
import com.carrotsearch.hppc.BitSetIterator;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.features.FeatureExtraction;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Based on the paper "Hashing-Accelerated Graph Neural Networks for Link Prediction"
 */
public class HashGNN extends Algorithm<HashGNN.HashGNNResult> {
    private final long randomSeed;
    private final Graph graph;
    private final SplittableRandom rng;
    private final HashGNNConfig config;

    public HashGNN(Graph graph, HashGNNConfig config, ProgressTracker progressTracker) {
        super(progressTracker);
        this.graph = graph;
        this.config = config;
        this.randomSeed = config.randomSeed().orElse((new SplittableRandom().nextLong()));
        this.rng = new SplittableRandom(randomSeed);
    }

    @Override
    public HashGNNResult compute() {
        progressTracker.beginSubTask();
        var degreePartition = PartitionUtils.degreePartition(
            graph,
            // Since degree only very approximately reflect the min hash task workload per node we decrease the partition sizes.
            Math.toIntExact(Math.min(config.concurrency() * 8L, graph.nodeCount())),
            Function.identity(),
            Optional.of(1)
        );
        var rangePartition = PartitionUtils.rangePartition(
            config.concurrency(),
            graph.nodeCount(),
            Function.identity(),
            Optional.of(1)
        );

        Graph graphCopy = graph.concurrentCopy();
        GraphSchema schema = graph.schema();
        List<Graph> graphs = config.heterogeneous()
            ? schema.relationshipSchema().availableTypes()
            .stream()
            .map(rt -> graph.relationshipTypeFilteredGraph(Set.of(rt)))
            .collect(Collectors.toList())
            : List.of(graphCopy);

        var embeddingsB = config.binarizeFeatures().isPresent()
            ? BinarizeTask.compute(graph, rangePartition, config, rng, progressTracker, terminationFlag)
            : RawFeaturesTask.compute(config, rng, progressTracker, graph, rangePartition, terminationFlag);
        int embeddingDimension = config.binarizeFeatures().map(FeatureBinarizationConfig::dimension).orElseGet(() -> {
            var featureExtractors = FeatureExtraction.propertyExtractors(
                graph,
                config.featureProperties()
            );
            return FeatureExtraction.featureCount(featureExtractors);
        });

        var embeddingsA = HugeObjectArray.newArray(BitSet.class, graph.nodeCount());
        embeddingsA.setAll(unused -> new BitSet(embeddingDimension));

        double avgDegree = (graph.relationshipCount() / (double) graph.nodeCount());
        int upperBoundBits = Math.min(embeddingDimension, config.embeddingDensity());
        int upperBoundExpectedBits = (int) Math.round(upperBoundBits * (1 - Math.pow(1 - (1.0 / upperBoundBits), config.embeddingDensity())));
        double scaledNeighborInfluence = graph.relationshipCount() == 0 ? 1.0 : upperBoundExpectedBits * config.neighborInfluence() / avgDegree;

        var hashes = HashTask.compute(
            embeddingDimension,
            scaledNeighborInfluence,
            graphs.size(),
            config,
            randomSeed,
            terminationFlag
        );

        for (int iteration = 0; iteration < config.iterations(); iteration++) {
            terminationFlag.assertRunning();

            progressTracker.logInfo("Starting iteration " + iteration);

            var currentEmbeddings = iteration % 2 == 0 ? embeddingsA : embeddingsB;
            var previousEmbeddings = iteration % 2 == 0 ? embeddingsB : embeddingsA;
            for (long i = 0; i < currentEmbeddings.size(); i++) {
                currentEmbeddings.get(i).clear();
            }

            MinHashTask.compute(
                degreePartition,
                graphs,
                config,
                embeddingDimension,
                currentEmbeddings,
                previousEmbeddings,
                iteration,
                hashes,
                progressTracker,
                terminationFlag
            );

            progressTracker.logInfo("Finished iteration " + iteration);
        }

        var binaryOutputVectors = (config.iterations() - 1) % 2 == 0 ? embeddingsA : embeddingsB;

        HugeObjectArray<double[]> outputVectors;
        if (config.outputDimension().isPresent()) {
            outputVectors = DensifyTask.densify(
                graph,
                rangePartition,
                config,
                rng,
                binaryOutputVectors,
                progressTracker,
                terminationFlag
            );
        } else {
            outputVectors = HugeObjectArray.newArray(double[].class, graph.nodeCount());
            outputVectors.setAll(nodeId -> bitSetToArray(binaryOutputVectors.get(nodeId), embeddingDimension));
        }

        progressTracker.endSubTask();
        return new HashGNNResult(outputVectors);
    }

    private double[] bitSetToArray(BitSet bitSet, int dimension) {
        var array = new double[dimension];
        var iterator = bitSet.iterator();
        var bit = iterator.nextSetBit();
        while (bit != BitSetIterator.NO_MORE) {
            array[bit] = 1.0;
            bit = iterator.nextSetBit();
        }
        return array;
    }

    static final class MinAndArgmin {
        public int min;
        public int argMin;

        MinAndArgmin() {
            this.min = -1;
            this.argMin = Integer.MAX_VALUE;
        }
    }

    @Override
    public void release() {

    }

    public static class HashGNNResult {
        private final HugeObjectArray<double[]> embeddings;

        public HashGNNResult(HugeObjectArray<double[]> embeddings) {
            this.embeddings = embeddings;
        }

        public HugeObjectArray<double[]> embeddings() {
            return embeddings;
        }
    }

}
