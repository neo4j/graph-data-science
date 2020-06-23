/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.gds.embeddings.graphsage;

import org.neo4j.gds.embeddings.graphsage.ddl4j.ComputationContext;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Constant;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.DummyVariable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.NormaliseRows;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageBaseConfig;
import org.neo4j.gds.embeddings.graphsage.batch.BatchProvider;
import org.neo4j.gds.embeddings.graphsage.batch.MiniBatchProvider;
import org.neo4j.gds.embeddings.graphsage.subgraph.SubGraph;
import org.neo4j.gds.embeddings.graphsage.subgraph.SubGraphBuilder;
import org.neo4j.gds.embeddings.graphsage.subgraph.SubGraphBuilderImpl;
import org.neo4j.logging.Log;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.neo4j.graphalgo.core.concurrency.ParallelUtil.parallelStreamConsume;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class GraphSageModel {
    private final Layer[] layers;
    protected final int concurrency;
    private double degreeProbabilityNormalizer;

    private final double tolerance;
    private final double learningRate;
    // number of negative samples
    private final int Q;
    private final int searchDepth;
    private final int epochs;
    private final int maxOptimizationIterations;
    private final MiniBatchProvider batchProvider;

    private final Log log;

    public GraphSageModel(int concurrency, int batchSize, List<Layer> layers,
                          Log log) {
        this(concurrency, batchSize, 1e-4, 0.1, 1, 100, layers, log);
    }

    public GraphSageModel(
        int concurrency,
        int batchSize,
        double tolerance,
        double learningRate,
        int epochs,
        int maxOptimizationIterations,
        Collection<Layer> layers,
        Log log
    ) {
        this(concurrency, batchSize, tolerance, learningRate, epochs, maxOptimizationIterations, 5, 20, layers, log);

    }

    public GraphSageModel(
        int concurrency,
        int batchSize,
        double tolerance,
        double learningRate,
        int epochs,
        int maxOptimizationIterations,
        int searchDepth,
        int negativeSamples,
        Collection<Layer> layers,
        Log log
    ) {
        this.concurrency = concurrency;
        this.layers = layers.toArray(Layer[]::new);

        this.tolerance = tolerance;
        this.learningRate = learningRate;
        this.epochs = epochs;
        this.maxOptimizationIterations = maxOptimizationIterations;
        this.searchDepth = searchDepth;
        this.Q = negativeSamples;
        this.batchProvider = new MiniBatchProvider(batchSize);

        this.log = log;
    }

    public GraphSageModel(GraphSageBaseConfig config, Log log) {
        this(
            config.concurrency(),
            config.batchSize(),
            config.tolerance(),
            config.learningRate(),
            config.epochs(),
            config.maxOptimizationIterations(),
            config.searchDepth(),
            config.negativeSamples(),
            config.layerConfigs().stream()
                .map(LayerInitialisationFactory::createLayer)
                .collect(Collectors.toList()),
            log
        );
    }

    public HugeObjectArray<double[]> makeEmbeddings(
        Graph graph,
        HugeObjectArray<double[]> features
    ) {
        HugeObjectArray<double[]> result = HugeObjectArray.newArray(
            double[].class,
            graph.nodeCount(),
            AllocationTracker.EMPTY
        );

        parallelStreamConsume(this.batchProvider.stream(graph), concurrency, batches -> {
            batches.forEach(batch -> {
                ComputationContext ctx = ComputationContext.instance();
                Variable embeddingVariable = embeddingVariable(graph, batch, features);
                int dimension = embeddingVariable.dimension(1);
                double[] embeddings = ctx.forward(embeddingVariable).data;

                for (int internalId = 0; internalId < batch.size(); internalId++) {
                    double[] nodeEmbedding = Arrays.copyOfRange(
                        embeddings,
                        internalId * dimension,
                        (internalId + 1) * dimension
                    );
                    result.set(batch.get(internalId), nodeEmbedding);
                }
            });
        });

        return result;
    }

    public TrainResult train(Graph graph, HugeObjectArray<double[]> features) {
        Map<String, Double> epochLosses = new TreeMap<>();
        double startLoss;
        this.degreeProbabilityNormalizer = LongStream
            .range(0, graph.nodeCount())
            .mapToDouble(nodeId -> Math.pow(graph.degree(nodeId), 0.75))
            .sum();

        double oldLoss = evaluateLoss(graph, features, this.batchProvider, -1);
        startLoss = oldLoss;
        for (int epoch = 0; epoch < this.epochs; epoch++) {
            trainEpoch(graph, features, epoch);
            double newLoss = evaluateLoss(graph, features, this.batchProvider, epoch);
            epochLosses.put(
                formatWithLocale("Epoch: %d", epoch),
                newLoss
            );
            if (Math.abs((newLoss - oldLoss)/oldLoss) < this.tolerance) {
                break;
            }
        }

        return new TrainResult(startLoss, epochLosses);
    }

    private double evaluateLoss(Graph graph, HugeObjectArray<double[]> features, BatchProvider batchProvider, int epoch) {
        DoubleAdder doubleAdder = new DoubleAdder();
        parallelStreamConsume(batchProvider.stream(graph), concurrency, batches -> {
            batches.forEach(batch -> {
                ComputationContext ctx = ComputationContext.instance();
                Variable loss = lossFunction(batch, graph, features);
                doubleAdder.add(ctx.forward(loss).data[0]);
            });
        });
        double lossValue = doubleAdder.doubleValue();
        log.debug(formatWithLocale("Loss after epoch %s: %s", epoch, lossValue));
        return lossValue;
    }

    private void trainEpoch(Graph graph, HugeObjectArray<double[]> features, int epoch) {
        List<Weights> weights = getWeights();

        Updater updater = new AdamOptimizer(weights, learningRate);

        AtomicInteger batchCounter = new AtomicInteger(0);
        parallelStreamConsume(((BatchProvider) this.batchProvider).stream(graph), concurrency, batches -> {
            batches.forEach(batch -> trainOnBatch(
                batch,
                graph,
                features,
                updater,
                epoch,
                batchCounter.incrementAndGet()
            ));
        });
    }

    private void trainOnBatch(
        List<Long> batch,
        Graph graph,
        HugeObjectArray<double[]> features,
        Updater updater,
        int epoch,
        int batchIndex
    ) {
        for (Layer layer : layers) {
            layer.generateNewRandomState();
        }

        Variable lossFunction = lossFunction(batch, graph, features);

        int iteration = 0;

        double newLoss = Double.MAX_VALUE;
        double oldLoss;

        while(iteration < this.maxOptimizationIterations) {
            oldLoss = newLoss;

            ComputationContext localCtx = ComputationContext.instance();

            newLoss = localCtx.forward(lossFunction).data[0];
            if (iteration == 0) {
                log.debug(formatWithLocale("Epoch %d\tBatch %d, Initial loss: %.10f", epoch, batchIndex, newLoss));
            }
            if (Math.abs((oldLoss - newLoss)/oldLoss) < this.tolerance) {
                break;
            }
            localCtx.backward(lossFunction);

            updater.update(localCtx);

            iteration++;
        }

        log.debug(formatWithLocale("Epoch %d\tBatch %d LOSS: %.10f at iteration %d", epoch, batchIndex, newLoss, iteration));
    }

    protected Variable lossFunction(List<Long> batch, Graph graph, HugeObjectArray<double[]> features) {
        List<Long> neighborBatch = neighborBatch(graph, batch);
        List<Long> negativeBatch = negativeBatch(graph, batch);
        List<Long> totalBatch = new LinkedList<>();
        totalBatch.addAll(batch);
        totalBatch.addAll(neighborBatch);
        totalBatch.addAll(negativeBatch);
        Variable embeddingVariable = embeddingVariable(graph, totalBatch, features);

        Variable lossFunction = new GraphSageLoss(embeddingVariable, Q);

        lossFunction = new DummyVariable(lossFunction);
        return lossFunction;
    }

    private List<Long> neighborBatch(Graph graph, List<Long> batch) {
        return batch.stream().mapToLong(nodeId -> sampleNeighbor(nodeId, graph))
            .boxed()
            .collect(Collectors.toList());
    }

    private List<Long> negativeBatch(Graph graph, List<Long> batch) {
        return IntStream.range(0, batch.size())
            .mapToLong(ignore -> sampleNegativeNode(graph))
            .boxed()
            .collect(Collectors.toList());
    }

    private long sampleNegativeNode(Graph graph) {
        Random rand = new SecureRandom();
        rand.setSeed(layers[0].randomState());
        double randomValue = rand.nextDouble();
        double cumulativeProbability = 0;

        for(long nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
            cumulativeProbability += Math.pow(graph.degree(nodeId), 0.75) / degreeProbabilityNormalizer;
            if (randomValue < cumulativeProbability) {
                return nodeId;
            }
        }
        throw new RuntimeException("This should never happen");
    }

    private long sampleNeighbor(long nodeId, Graph graph) {
        UniformNeighborhoodSampler sampler = new UniformNeighborhoodSampler();
        int searchDistance = new Random().nextInt(this.searchDepth) + 1;
        AtomicLong currentNode = new AtomicLong(nodeId);
        while (searchDistance > 0) {
            List<Long> samples = sampler.sample(graph, currentNode.get(), 1, 0);
            if (samples.size() == 1) {
                currentNode.set(samples.get(0));
            } else {
                // terminate
                searchDistance = 0;
            }
            searchDistance--;
        }
        return currentNode.get();
    }

    private Variable featureVariables(Collection<Long> nodeIds, HugeObjectArray<double[]> features) {
        ArrayList<Long> nodeList = new ArrayList<>(nodeIds);
        int dimension = features.get(0).length;
        double[] data = new double[nodeIds.size() * dimension];
        IntStream.range(0, nodeIds.size()).forEach(nodeOffset -> {
            System.arraycopy(features.get(nodeList.get(nodeOffset)), 0, data, nodeOffset * dimension, dimension);
        });
        return Constant.matrix(data, nodeIds.size(), dimension);
    }

    private Variable embeddingVariable(Graph graph, Collection<Long> nodeIds, HugeObjectArray<double[]> features) {
        SubGraphBuilder subGraphBuilder = new SubGraphBuilderImpl();
        List<NeighborhoodFunction> neighborhoodFunctions = Arrays
            .stream(layers)
            .map(layer -> (NeighborhoodFunction) layer::neighborhoodFunction)
            .collect(Collectors.toList());
        List<SubGraph> subGraphs = subGraphBuilder.buildSubGraphs(nodeIds, neighborhoodFunctions, graph);

        Variable previousLayerRepresentations = featureVariables(subGraphs.get(subGraphs.size() - 1).nextNodes, features);

        for (int layerNr = layers.length - 1; layerNr >= 0; layerNr--) {
            Layer layer = layers[layers.length - layerNr - 1];
            previousLayerRepresentations = layer
                .aggregator()
                .aggregate(previousLayerRepresentations,
                    subGraphs.get(layerNr).adjacency,
                    subGraphs.get(layerNr).selfAdjacency
                );
        }
        return new NormaliseRows(previousLayerRepresentations);
    }

    private List<Weights> getWeights() {
        return Arrays.stream(layers)
            .flatMap(layer -> layer.weights().stream())
            .collect(Collectors.toList());
    }

    public static class TrainResult {
        private final double startLoss;
        private final Map<String, Double> epochLosses;

        public TrainResult(double startLoss, Map<String, Double> epochLosses) {
            this.startLoss = startLoss;
            this.epochLosses = epochLosses;
        }

        public double startLoss() {
            return startLoss;
        }

        public Map<String, Double> epochLosses() {
            return epochLosses;
        }
    }
}
