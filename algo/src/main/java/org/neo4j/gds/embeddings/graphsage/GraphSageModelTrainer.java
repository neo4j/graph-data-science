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
package org.neo4j.gds.embeddings.graphsage;

import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.ddl4j.ComputationContext;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.PassthroughVariable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Scalar;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Tensor;
import org.neo4j.gds.ml.features.FeatureExtraction;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.neo4j.gds.embeddings.graphsage.GraphSageHelper.embeddings;
import static org.neo4j.gds.embeddings.graphsage.RelationshipWeights.UNWEIGHTED;
import static org.neo4j.graphalgo.core.concurrency.ParallelUtil.parallelStreamConsume;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class GraphSageModelTrainer {
    private Layer[] layers;
    private final boolean useWeights;
    private final BatchProvider batchProvider;
    private final double learningRate;
    private final double tolerance;
    private final int negativeSampleWeight;
    private final int concurrency;
    private final int epochs;
    private final int maxIterations;
    private final int maxSearchDepth;
    private final Function<Graph, List<LayerConfig>> layerConfigsFunction;
    private final FeatureFunction featureFunction;
    private final Collection<Weights<? extends Tensor<?>>> labelProjectionWeights;
    private final ProgressLogger progressLogger;
    private double degreeProbabilityNormalizer;

    public GraphSageModelTrainer(GraphSageTrainConfig config, ProgressLogger progressLogger) {
        this(config, progressLogger, new SingleLabelFeatureFunction(), Collections.emptyList());
    }

    public GraphSageModelTrainer(
        GraphSageTrainConfig config,
        ProgressLogger progressLogger,
        FeatureFunction featureFunction,
        Collection<Weights<? extends Tensor<?>>> labelProjectionWeights
    ) {
        this.layerConfigsFunction = graph -> config.layerConfigs(firstLayerColumns(config, graph));
        this.batchProvider = new BatchProvider(config.batchSize());
        this.learningRate = config.learningRate();
        this.tolerance = config.tolerance();
        this.negativeSampleWeight = config.negativeSampleWeight();
        this.concurrency = config.concurrency();
        this.epochs = config.epochs();
        this.maxIterations = config.maxIterations();
        this.maxSearchDepth = config.searchDepth();
        this.featureFunction = featureFunction;
        this.labelProjectionWeights = labelProjectionWeights;
        this.progressLogger = progressLogger;

        this.useWeights = config.relationshipWeightProperty() != null;
    }

    public ModelTrainResult train(Graph graph, HugeObjectArray<double[]> features) {
        progressLogger.logStart();
        Map<String, Double> epochLosses = new TreeMap<>();

        this.layers = layerConfigsFunction.apply(graph).stream()
            .map(LayerFactory::createLayer)
            .toArray(Layer[]::new);

        degreeProbabilityNormalizer = LongStream
            .range(0, graph.nodeCount())
            .mapToDouble(nodeId -> Math.pow(graph.degree(nodeId), 0.75))
            .sum();

        double initialLoss = evaluateLoss(graph, features, batchProvider, -1);
        double previousLoss = initialLoss;
        for (int epoch = 0; epoch < epochs; epoch++) {
            var epochMessage = ":: Epoch " + (epoch + 1);
            progressLogger.logStart(epochMessage);

            trainEpoch(graph, features, epoch);
            double newLoss = evaluateLoss(graph, features, batchProvider, epoch);
            epochLosses.put(
                formatWithLocale("Epoch: %d", epoch),
                newLoss
            );
            progressLogger.logFinish(epochMessage);
            if (Math.abs((newLoss - previousLoss) / previousLoss) < tolerance) {
                break;
            }
            previousLoss = newLoss;
        }
        progressLogger.logFinish();

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

        progressLogger
            .getLog()
            .debug("Epoch %d\tBatch %d, Initial loss: %.10f", epoch, batchIndex, newLoss);

        int iteration = 0;
        while (iteration < maxIterations) {
            progressLogger.logStart(":: Iteration " + (iteration + 1));
            oldLoss = newLoss;

            ComputationContext localCtx = new ComputationContext();

            newLoss = localCtx.forward(lossFunction).dataAt(0);
            double lossDiff = Math.abs((oldLoss - newLoss) / oldLoss);

            if (lossDiff < tolerance) {
                progressLogger.logFinish(":: Iteration " + (iteration + 1));
                break;
            }
            localCtx.backward(lossFunction);

            updater.update(localCtx);

            progressLogger.logFinish(":: Iteration " + (iteration + 1));
            iteration++;
        }

        progressLogger.getLog().debug(
            "Epoch %d\tBatch %d LOSS: %.10f at iteration %d",
            epoch,
            batchIndex,
            newLoss,
            iteration
        );
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
        progressLogger.getLog().debug("Loss after epoch %s: %s", epoch, lossValue);
        return lossValue;
    }

    private Variable<Scalar> lossFunction(long[] batch, Graph graph, HugeObjectArray<double[]> features) {
        long[] totalBatch = LongStream
            .concat(Arrays.stream(batch), LongStream.concat(
                neighborBatch(graph, batch),
                negativeBatch(graph, batch.length)
            )).toArray();
        Variable<Matrix> embeddingVariable = embeddings(graph, useWeights, totalBatch, features, this.layers, featureFunction);

        Variable<Scalar> lossFunction = new GraphSageLoss(
            useWeights ? graph::relationshipProperty : UNWEIGHTED,
            embeddingVariable,
            totalBatch,
            negativeSampleWeight
        );

        return new PassthroughVariable<>(lossFunction);
    }

    private LongStream neighborBatch(Graph graph, long[] batch) {
        return Arrays.stream(batch).map(nodeId -> {
            int searchDepth = ThreadLocalRandom.current().nextInt(maxSearchDepth) + 1;
            AtomicLong currentNode = new AtomicLong(nodeId);
            while (searchDepth > 0) {
                NeighborhoodSampler neighborhoodSampler = useWeights ?
                    new WeightedNeighborhoodSampler(0L) :
                    new UniformNeighborhoodSampler(0L);
                OptionalLong maybeSample = neighborhoodSampler.sampleOne(graph, nodeId);
                if (maybeSample.isPresent()) {
                    currentNode.set(maybeSample.getAsLong());
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
                throw new RuntimeException(
                    "This happens when there are no relationships in the Graph. " +
                    "This condition is checked by the calling procedure."
                );
            });
    }

    private List<Weights<? extends Tensor<?>>> getWeights() {
        List<Weights<? extends Tensor<?>>> weights = new ArrayList<>(labelProjectionWeights);
        weights.addAll(Arrays.stream(layers)
            .flatMap(layer -> layer.weights().stream())
            .collect(Collectors.toList()));
        return weights;
    }

    private int firstLayerColumns(GraphSageTrainConfig config, Graph graph) {
        return config.projectedFeatureDimension().orElseGet(() -> {
            var featureExtractors = GraphSageHelper.featureExtractors(graph, config);
            return FeatureExtraction.featureCount(featureExtractors);
        });
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
