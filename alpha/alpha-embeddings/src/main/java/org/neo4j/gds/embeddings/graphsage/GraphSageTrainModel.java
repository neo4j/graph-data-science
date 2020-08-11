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

import org.neo4j.gds.embeddings.graphsage.algo.GraphSageBaseConfig;
import org.neo4j.gds.embeddings.graphsage.ddl4j.ComputationContext;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixConstant;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.NormalizeRows;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.PassthroughVariable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Scalar;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Tensor;
import org.neo4j.gds.embeddings.graphsage.subgraph.SubGraph;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.logging.Log;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.neo4j.graphalgo.core.concurrency.ParallelUtil.parallelStreamConsume;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class GraphSageTrainModel {
    private final Layer[] layers;
    private final Log log;
    private final BatchProvider batchProvider;
    private final double learningRate;
    private final double tolerance;
    private final int negativeSampleWeight;
    private final int concurrency;
    private final int epochs;
    private final int maxIterations;
    private final int maxSearchDepth;
    private double degreeProbabilityNormalizer;


    public GraphSageTrainModel(GraphSageBaseConfig config, Log log) {
        this.layers = config.layerConfigs().stream()
            .map(LayerFactory::createLayer)
            .toArray(Layer[]::new);
        this.log = log;
        this.batchProvider = new BatchProvider(config.batchSize());
        this.learningRate = config.learningRate();
        this.tolerance = config.tolerance();
        this.negativeSampleWeight = config.negativeSampleWeight();
        this.concurrency = config.concurrency();
        this.epochs = config.epochs();
        this.maxIterations = config.maxIterations();
        this.maxSearchDepth = config.searchDepth();
    }

    public ModelTrainResult train(Graph graph, HugeObjectArray<double[]> features) {
        Map<String, Double> epochLosses = new TreeMap<>();
        degreeProbabilityNormalizer = LongStream
            .range(0, graph.nodeCount())
            .mapToDouble(nodeId -> Math.pow(graph.degree(nodeId), 0.75))
            .sum();

        double initialLoss = evaluateLoss(graph, features, batchProvider, -1);
        double previousLoss = initialLoss;
        for (int epoch = 0; epoch < epochs; epoch++) {
            trainEpoch(graph, features, epoch);
            double newLoss = evaluateLoss(graph, features, batchProvider, epoch);
            epochLosses.put(
                formatWithLocale("Epoch: %d", epoch),
                newLoss
            );
            if (Math.abs((newLoss - previousLoss) / previousLoss) < tolerance) {
                break;
            }
            previousLoss = newLoss;
        }

        return ModelTrainResult.of(initialLoss, epochLosses, this.layers);
    }

    private void trainEpoch(Graph graph, HugeObjectArray<double[]> features, int epoch) {
        List<Weights<? extends Tensor<?>>> weights = getWeights();

        AdamOptimizer updater = new AdamOptimizer(weights, learningRate);

        AtomicInteger batchCounter = new AtomicInteger(0);
        parallelStreamConsume(
            batchProvider.stream(graph),
            concurrency,
            batches -> batches.forEach(batch -> trainOnBatch(
                batch,
                graph,
                features,
                updater,
                epoch,
                batchCounter.incrementAndGet()
            ))
        );
    }

    private void trainOnBatch(
        long[] batch,
        Graph graph,
        HugeObjectArray<double[]> features,
        AdamOptimizer updater,
        int epoch,
        int batchIndex
    ) {
        for (Layer layer : layers) {
            layer.generateNewRandomState();
        }

        Variable<Scalar> lossFunction = lossFunction(batch, graph, features);

        double newLoss = Double.MAX_VALUE;
        double oldLoss;

        log.debug(formatWithLocale("Epoch %d\tBatch %d, Initial loss: %.10f", epoch, batchIndex, newLoss));

        int iteration = 0;
        while (iteration < maxIterations) {
            oldLoss = newLoss;

            ComputationContext localCtx = new ComputationContext();

            newLoss = localCtx.forward(lossFunction).dataAt(0);
            double lossDiff = Math.abs((oldLoss - newLoss) / oldLoss);
            if (lossDiff < tolerance) {
                break;
            }
            localCtx.backward(lossFunction);

            updater.update(localCtx);

            iteration++;
        }

        log.debug(formatWithLocale(
            "Epoch %d\tBatch %d LOSS: %.10f at iteration %d",
            epoch,
            batchIndex,
            newLoss,
            iteration
        ));
    }

    private double evaluateLoss(
        Graph graph,
        HugeObjectArray<double[]> features,
        BatchProvider batchProvider,
        int epoch
    ) {
        DoubleAdder doubleAdder = new DoubleAdder();
        parallelStreamConsume(
            batchProvider.stream(graph),
            concurrency,
            batches -> batches.forEach(batch -> {
                ComputationContext ctx = new ComputationContext();
                Variable<Scalar> loss = lossFunction(batch, graph, features);
                doubleAdder.add(ctx.forward(loss).dataAt(0));
            })
        );
        double lossValue = doubleAdder.doubleValue();
        log.debug(formatWithLocale("Loss after epoch %s: %s", epoch, lossValue));
        return lossValue;
    }

    Variable<Scalar> lossFunction(long[] batch, Graph graph, HugeObjectArray<double[]> features) {
        long[] totalBatch = LongStream
            .concat(Arrays.stream(batch), LongStream.concat(
                neighborBatch(graph, batch),
                negativeBatch(graph, batch.length)
            )).toArray();
        Variable<Matrix> embeddingVariable = embeddingVariable(graph, totalBatch, features);

        Variable<Scalar> lossFunction = new GraphSageLoss(embeddingVariable, negativeSampleWeight);

        return new PassthroughVariable<>(lossFunction);
    }

    private LongStream neighborBatch(Graph graph, long[] batch) {
        return Arrays.stream(batch).map(nodeId -> {
            int searchDepth = ThreadLocalRandom.current().nextInt(maxSearchDepth) + 1;
            AtomicLong currentNode = new AtomicLong(nodeId);
            while (searchDepth > 0) {
                List<Long> samples = new UniformNeighborhoodSampler().sample(graph, currentNode.get(), 1, 0);
                if (samples.size() == 1) {
                    currentNode.set(samples.get(0));
                } else {
                    // terminate
                    searchDepth = 0;
                }
                searchDepth--;
            }
            return currentNode.get();
        });
    }

    private LongStream negativeBatch(Graph graph, int batchSize) {
        Random rand = new Random(layers[0].randomState());
        return IntStream.range(0, batchSize)
            .mapToLong(ignore -> {
                double randomValue = rand.nextDouble();
                double cumulativeProbability = 0;

                for (long nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
                    cumulativeProbability += Math.pow(graph.degree(nodeId), 0.75) / degreeProbabilityNormalizer;
                    if (randomValue < cumulativeProbability) {
                        return nodeId;
                    }
                }
                throw new RuntimeException("This should never happen");
            });
    }

    private Variable<Matrix> featureVariables(long[] nodeIds, HugeObjectArray<double[]> features) {
        int dimension = features.get(0).length;
        double[] data = new double[nodeIds.length * dimension];
        IntStream
            .range(0, nodeIds.length)
            .forEach(nodeOffset -> System.arraycopy(
                features.get(nodeIds[nodeOffset]),
                0,
                data,
                nodeOffset * dimension,
                dimension
            ));
        return new MatrixConstant(data, nodeIds.length, dimension);
    }

    private Variable<Matrix> embeddingVariable(Graph graph, long[] nodeIds, HugeObjectArray<double[]> features) {
        List<NeighborhoodFunction> neighborhoodFunctions = Arrays
            .stream(layers)
            .map(layer -> (NeighborhoodFunction) layer::neighborhoodFunction)
            .collect(Collectors.toList());
        Collections.reverse(neighborhoodFunctions);
        List<SubGraph> subGraphs = SubGraph.buildSubGraphs(nodeIds, neighborhoodFunctions, graph);

        Variable<Matrix> previousLayerRepresentations = featureVariables(
            subGraphs.get(subGraphs.size() - 1).nextNodes,
            features
        );

        for (int layerNr = layers.length - 1; layerNr >= 0; layerNr--) {
            Layer layer = layers[layers.length - layerNr - 1];
            previousLayerRepresentations = layer
                .aggregator()
                .aggregate(
                    previousLayerRepresentations,
                    subGraphs.get(layerNr).adjacency,
                    subGraphs.get(layerNr).selfAdjacency
                );
        }
        return new NormalizeRows(previousLayerRepresentations);
    }

    private List<Weights<? extends Tensor<?>>> getWeights() {
        return Arrays.stream(layers)
            .flatMap(layer -> layer.weights().stream())
            .collect(Collectors.toList());
    }

    @ValueClass
    public interface ModelTrainResult {

        double startLoss();

        Map<String, Double> epochLosses();

        Layer[] layers();

        static ModelTrainResult of(double startLoss, Map<String, Double> epochLosses, Layer[] layers) {
            return ImmutableModelTrainResult.builder()
                .startLoss(startLoss)
                .epochLosses(epochLosses)
                .layers(layers)
                .build();
        }
    }
}
