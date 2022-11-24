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

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.features.FeatureExtraction;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

/**
 * Based on the paper "Hashing-Accelerated Graph Neural Networks for Link Prediction"
 */
public class HashGNN extends Algorithm<HashGNN.HashGNNResult> {
    private static final long DEGREE_PARTITIONS_PER_THREAD = 4;
    private final long randomSeed;
    private final Graph graph;
    private final SplittableRandom rng;
    private final HashGNNConfig config;
    private final MutableLong totalSetBits = new MutableLong();

    public HashGNN(Graph graph, HashGNNConfig config, ProgressTracker progressTracker) {
        super(progressTracker);
        this.graph = graph;
        this.config = config;

        long tempRandomSeed = config.randomSeed().orElse((new SplittableRandom().nextLong()));
        this.randomSeed = new SplittableRandom(tempRandomSeed).nextLong();
        this.rng = new SplittableRandom(randomSeed);
    }

    @Override
    public HashGNNResult compute() {
        progressTracker.beginSubTask("HashGNN");

        var degreePartition = PartitionUtils.degreePartition(
            graph,
            // Since degree only very approximately reflect the min hash task workload per node we decrease the partition sizes.
            Math.toIntExact(Math.min(config.concurrency() * DEGREE_PARTITIONS_PER_THREAD, graph.nodeCount())),
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

        var embeddingsB = constructInputEmbeddings(rangePartition);
        int embeddingDimension = (int) embeddingsB.get(0).size();

        double avgInputActiveFeatures = totalSetBits.doubleValue() / graph.nodeCount();
        progressTracker.logInfo(formatWithLocale("Density (number of active features) of binary input features is %.4f.", avgInputActiveFeatures));

        var embeddingsA = HugeObjectArray.newArray(HugeAtomicBitSet.class, graph.nodeCount());
        embeddingsA.setAll(unused -> HugeAtomicBitSet.create(embeddingDimension));

        double avgDegree = (graph.relationshipCount() / (double) graph.nodeCount());
        double upperBoundNeighborExpectedBits = embeddingDimension == 0
            ? 1
            : embeddingDimension * (1 - Math.pow(
                1 - (1.0 / embeddingDimension),
                avgDegree)
            );

        progressTracker.beginSubTask("Propagate embeddings");

        for (int iteration = 0; iteration < config.iterations(); iteration++) {
            terminationFlag.assertRunning();

            var currentEmbeddings = iteration % 2 == 0 ? embeddingsA : embeddingsB;
            var previousEmbeddings = iteration % 2 == 0 ? embeddingsB : embeddingsA;
            for (long i = 0; i < currentEmbeddings.size(); i++) {
                currentEmbeddings.get(i).clear();
            }

            double scaledNeighborInfluence = graph.relationshipCount() == 0 ? 1.0 : (totalSetBits.doubleValue() / graph.nodeCount()) * config.neighborInfluence() / upperBoundNeighborExpectedBits;
            totalSetBits.setValue(0);

            var hashes = HashTask.compute(
                embeddingDimension,
                scaledNeighborInfluence,
                graphs.size(),
                config,
                randomSeed,
                terminationFlag,
                progressTracker
            );

            MinHashTask.compute(
                degreePartition,
                graphs,
                config,
                embeddingDimension,
                currentEmbeddings,
                previousEmbeddings,
                hashes,
                progressTracker,
                terminationFlag,
                totalSetBits
            );

            double avgActiveFeatures = totalSetBits.doubleValue() / graph.nodeCount();
            progressTracker.logInfo(formatWithLocale("After iteration %d average node embedding density (number of active features) is %.4f.", iteration, avgActiveFeatures));
        }

        progressTracker.endSubTask("Propagate embeddings");

        var binaryOutputVectors = (config.iterations() - 1) % 2 == 0 ? embeddingsA : embeddingsB;

        HugeObjectArray<double[]> outputVectors;
        if (config.outputDimension().isPresent()) {
            outputVectors = DensifyTask.compute(
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

        progressTracker.endSubTask("HashGNN");

        return new HashGNNResult(outputVectors);
    }

    private double[] bitSetToArray(HugeAtomicBitSet bitSet, int dimension) {
        var array = new double[dimension];
        bitSet.forEachSetBit(bit -> {
            array[(int) bit] = 1.0;
        });
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

    private HugeObjectArray<HugeAtomicBitSet> constructInputEmbeddings(List<Partition> partition) {
        List<HugeObjectArray<HugeAtomicBitSet>> inputEmbeddingsList = new ArrayList<>();
        var embeddingDimension = new MutableInt();
        var bitOffsets = new ArrayList<Integer>();

        if (!config.featureProperties().isEmpty()) {
            if (config.binarizeFeatures().isPresent()) {
                inputEmbeddingsList.add(BinarizeTask.compute(
                    graph,
                    partition,
                    config,
                    rng,
                    progressTracker,
                    terminationFlag,
                    totalSetBits
                ));
                bitOffsets.add(embeddingDimension.getValue());
                embeddingDimension.add(config.binarizeFeatures().get().dimension());
            } else {
                inputEmbeddingsList.add(RawFeaturesTask.compute(
                    config,
                    progressTracker,
                    graph,
                    partition,
                    terminationFlag,
                    totalSetBits
                ));
                var featureExtractors = FeatureExtraction.propertyExtractors(
                    graph,
                    config.featureProperties()
                );
                bitOffsets.add(embeddingDimension.getValue());
                embeddingDimension.add(FeatureExtraction.featureCount(featureExtractors));
            }
        }

        if (!config.generateFeatures().isPresent()) {
            return inputEmbeddingsList.get(0);
        }

        inputEmbeddingsList.add(GenerateFeaturesTask.compute(
            graph,
            partition,
            config,
            randomSeed,
            progressTracker,
            terminationFlag,
            totalSetBits
        ));
        bitOffsets.add(embeddingDimension.getValue());
        embeddingDimension.add(config.generateFeatures().get().dimension());

        var concatInputEmbeddings = HugeObjectArray.newArray(HugeAtomicBitSet.class, graph.nodeCount());

        var concatTasks = partition.stream().map(p -> (Runnable) () -> p.consume(nodeId -> {
            var concatFeatures = HugeAtomicBitSet.create(embeddingDimension.getValue());
            for (int i = 0; i < inputEmbeddingsList.size(); i++) {
                var embedding = inputEmbeddingsList.get(i).get(nodeId);
                int bitOffset = bitOffsets.get(i);
                embedding.forEachSetBit(bit -> concatFeatures.set(bitOffset + bit));
            }
            concatInputEmbeddings.set(nodeId, concatFeatures);
        })).collect(Collectors.toList());

        RunWithConcurrency.builder()
            .concurrency(config.concurrency())
            .tasks(concatTasks)
            .run();

        return concatInputEmbeddings;
    }
}
