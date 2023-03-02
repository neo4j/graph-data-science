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

import org.immutables.value.Value;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.config.ToMapConvertible;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.model.Model.CustomInfo;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.features.FeatureExtraction;
import org.neo4j.gds.ml.core.functions.ConstantScale;
import org.neo4j.gds.ml.core.functions.ElementSum;
import org.neo4j.gds.ml.core.functions.L2NormSquared;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.optimizer.AdamOptimizer;
import org.neo4j.gds.ml.core.subgraph.SubGraph;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.core.tensor.Tensor;
import org.neo4j.gds.ml.models.TrainingMethod;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.neo4j.gds.embeddings.graphsage.GraphSageHelper.embeddingsComputationGraph;
import static org.neo4j.gds.ml.core.tensor.TensorFunctions.averageTensors;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class GraphSageModelTrainer {
    private final long randomSeed;
    private final FeatureFunction featureFunction;
    private final Collection<Weights<Matrix>> labelProjectionWeights;
    private final ExecutorService executor;
    private final ProgressTracker progressTracker;
    private final GraphSageTrainConfig config;

    public GraphSageModelTrainer(GraphSageTrainConfig config, ExecutorService executor, ProgressTracker progressTracker) {
        this(config, executor, progressTracker, new SingleLabelFeatureFunction(), Collections.emptyList());
    }

    public GraphSageModelTrainer(
        GraphSageTrainConfig config,
        ExecutorService executor,
        ProgressTracker progressTracker,
        FeatureFunction featureFunction,
        Collection<Weights<Matrix>> labelProjectionWeights
    ) {
        this.config = config;
        this.featureFunction = featureFunction;
        this.labelProjectionWeights = labelProjectionWeights;
        this.executor = executor;
        this.progressTracker = progressTracker;
        this.randomSeed = config.randomSeed().orElseGet(() -> ThreadLocalRandom.current().nextLong());
    }

    public static List<Task> progressTasks(GraphSageTrainConfig config, long nodeCount) {
        return List.of(
            Tasks.leaf("Prepare batches", config.batchesPerIteration(nodeCount)),
            Tasks.iterativeDynamic(
                "Train model",
                () -> List.of(Tasks.iterativeDynamic(
                    "Epoch",
                    () -> List.of(Tasks.leaf("Iteration", config.batchesPerIteration(nodeCount))),
                    config.maxIterations()
                )),
                config.epochs()
            )
        );
    }

    public ModelTrainResult train(Graph graph, HugeObjectArray<double[]> features) {
        var layers = config.layerConfigs(firstLayerColumns(config, graph)).stream()
            .map(LayerFactory::createLayer)
            .toArray(Layer[]::new);

        assert graph.hasRelationshipProperty() == config.hasRelationshipWeightProperty() : "Weight property of graph and config needs to match.";

        var weights = new ArrayList<Weights<? extends Tensor<?>>>(labelProjectionWeights);
        for (Layer layer : layers) {
            weights.addAll(layer.weights());
        }

        progressTracker.beginSubTask("Prepare batches");

        var batchSampler = new BatchSampler(graph, progressTracker);

        List<long[]> extendedBatches = batchSampler
            .extendedBatches(config.batchSize(), config.searchDepth(), randomSeed);

        var random = new SplittableRandom(randomSeed);

        progressTracker.endSubTask("Prepare batches");

        progressTracker.beginSubTask("Train model");

        boolean converged = false;
        var iterationLossesPerEpoch = new ArrayList<List<Double>>();
        var prevEpochLoss = Double.NaN;
        int epochs = config.epochs();

        // if each batch is used more than once, we cache the tasks, otherwise we compute them lazily
        boolean createBatchTasksEagerly = config.batchesPerIteration(graph.nodeCount()) * config.maxIterations() > extendedBatches.size();

        for (int epoch = 1; epoch <= epochs && !converged; epoch++) {
            progressTracker.beginSubTask("Epoch");
            // also tried using random.nextLong() but this somehow had a worse quality
            long epochLocalSeed = epoch + randomSeed;

            Supplier<List<BatchTask>> batchTaskSampler;
            if (createBatchTasksEagerly) {
                List<BatchTask> tasksForEpoch = extendedBatches
                    .stream()
                    .map(extendedBatch -> createBatchTask(
                        extendedBatch,
                        graph,
                        features,
                        layers,
                        weights,
                        epochLocalSeed
                    ))
                    .collect(Collectors.toList());

                batchTaskSampler = () -> IntStream
                    .range(0, config.batchesPerIteration(graph.nodeCount()))
                    .mapToObj(__ -> tasksForEpoch.get(random.nextInt(tasksForEpoch.size())))
                    .collect(Collectors.toList());
            } else {
                batchTaskSampler = () -> IntStream
                    .range(0, config.batchesPerIteration(graph.nodeCount()))
                    .mapToObj(__ -> createBatchTask(
                        extendedBatches.get(random.nextInt(extendedBatches.size())),
                        graph,
                        features,
                        layers,
                        weights,
                        epochLocalSeed
                    ))
                    .collect(Collectors.toList());
            }

            var epochResult = trainEpoch(batchTaskSampler, weights, prevEpochLoss);
            List<Double> epochLosses = epochResult.losses();
            iterationLossesPerEpoch.add(epochLosses);
            prevEpochLoss = epochLosses.get(epochLosses.size() - 1);
            converged = epochResult.converged();
            progressTracker.endSubTask("Epoch");
        }

        progressTracker.endSubTask("Train model");

        return ModelTrainResult.of(iterationLossesPerEpoch, converged, layers);
    }

    /**
     * sampling the neighbor subgraph for each layer + constructing the loss function
     */
    private BatchTask createBatchTask(
        long[] extendedBatch,
        Graph graph,
        HugeObjectArray<double[]> features,
        Layer[] layers,
        ArrayList<Weights<? extends Tensor<?>>> weights,
        long localSeed
    ) {
        // as we pass a reference for the relationshipWeights, we need a local copy
        var localGraph = graph.concurrentCopy();

        List<SubGraph> subGraphs = GraphSageHelper.subGraphsPerLayer(localGraph, extendedBatch, layers, localSeed);

        Variable<Matrix> batchedFeaturesExtractor = featureFunction.apply(
            localGraph,
            subGraphs.get(subGraphs.size() - 1).originalNodeIds(),
            features
        );

        Variable<Matrix> embeddingVariable = embeddingsComputationGraph(subGraphs, layers, batchedFeaturesExtractor);

        Variable<Scalar> lossWithoutPenalty = new GraphSageLoss(
            SubGraph.relationshipWeightFunction(localGraph),
            embeddingVariable,
            extendedBatch,
            config.negativeSampleWeight()
        );

        long originalBatchSize = extendedBatch.length / 3;

        Variable<Scalar> loss;
        if (config.penaltyL2() > 0) {
            List<Variable<?>> l2penalty = Arrays
                .stream(layers)
                .map(layer -> layer.aggregator().weightsWithoutBias())
                .flatMap(layerWeights -> layerWeights.stream().map(L2NormSquared::new))
                .collect(Collectors.toList());

            loss = new ElementSum(List.of(
                lossWithoutPenalty,
                new ConstantScale<>(
                    new ElementSum(l2penalty),
                    // we scale the penalty to achieve the same impact on the last (smaller) batch as on every other batch
                    config.penaltyL2() * originalBatchSize / graph.nodeCount()
                )
            ));
        } else {
            loss = lossWithoutPenalty;
        }

        return new BatchTask(loss, weights, progressTracker);
    }

    private EpochResult trainEpoch(
        Supplier<List<BatchTask>> sampledBatchTaskSupplier,
        List<Weights<? extends Tensor<?>>> weights,
        double prevEpochLoss
    ) {
        var updater = new AdamOptimizer(weights, config.learningRate());

        int iteration = 1;
        var iterationLosses = new ArrayList<Double>();
        double prevLoss = prevEpochLoss;
        var converged = false;

        int maxIterations = config.maxIterations();
        for (; iteration <= maxIterations; iteration++) {
            progressTracker.beginSubTask("Iteration");

            var sampledBatchTasks = sampledBatchTaskSupplier.get();

            // run forward + maybe backward for each Batch
            RunWithConcurrency.builder()
                .concurrency(config.concurrency())
                .tasks(sampledBatchTasks)
                .executor(executor)
                .run();
            var avgLossPerNode = sampledBatchTasks.stream().mapToDouble(BatchTask::loss).sum() / sampledBatchTasks.size();
            iterationLosses.add(avgLossPerNode);
            progressTracker.logInfo(formatWithLocale("Average loss per node: %.10f", avgLossPerNode));

            if (Math.abs(prevLoss - avgLossPerNode) < config.tolerance()) {
                converged = true;
                progressTracker.endSubTask("Iteration");
                break;
            }

            prevLoss = avgLossPerNode;

            var batchedGradients = sampledBatchTasks
                .stream()
                .map(BatchTask::weightGradients)
                .collect(Collectors.toList());

            var meanGradients = averageTensors(batchedGradients);

            updater.update(meanGradients);
            progressTracker.endSubTask("Iteration");
        }

        return ImmutableEpochResult.of(converged, iterationLosses);
    }

    @ValueClass
    interface EpochResult {
        boolean converged();

        List<Double> losses();
    }

    static class BatchTask implements Runnable {

        private final Variable<Scalar> lossFunction;
        private final List<Weights<? extends Tensor<?>>> weightVariables;
        private List<? extends Tensor<?>> weightGradients;
        private final ProgressTracker progressTracker;
        private double loss;

        BatchTask(
            Variable<Scalar> lossFunction,
            List<Weights<? extends Tensor<?>>> weightVariables,
            ProgressTracker progressTracker
        ) {
            this.lossFunction = lossFunction;
            this.weightVariables = weightVariables;
            this.progressTracker = progressTracker;
        }

        @Override
        public void run() {
            var localCtx = new ComputationContext();
            loss = localCtx.forward(lossFunction).value();

            localCtx.backward(lossFunction);
            weightGradients = weightVariables.stream().map(localCtx::gradient).collect(Collectors.toList());

            progressTracker.logProgress();
        }

        public double loss() {
            return loss;
        }

        List<? extends Tensor<?>> weightGradients() {
            return weightGradients;
        }
    }

    private static int firstLayerColumns(GraphSageTrainConfig config, Graph graph) {
        return config.projectedFeatureDimension().orElseGet(() -> {
            var featureExtractors = GraphSageHelper.featureExtractors(graph, config);
            return FeatureExtraction.featureCount(featureExtractors);
        });
    }

    @ValueClass
    public interface GraphSageTrainMetrics extends ToMapConvertible, CustomInfo {
        static GraphSageTrainMetrics empty() {
            return ImmutableGraphSageTrainMetrics.of(List.of(), false);
        }

        @Value.Derived
        default List<Double> epochLosses() {
            return iterationLossPerEpoch().stream()
                .map(iterationLosses -> iterationLosses.get(iterationLosses.size() - 1))
                .collect(Collectors.toList());
        }

        List<List<Double>> iterationLossPerEpoch();

        boolean didConverge();

        @Value.Derived
        default int ranEpochs() {
            return iterationLossPerEpoch().isEmpty()
                ? 0
                : iterationLossPerEpoch().size();
        }

        @Value.Derived
        default List<Integer> ranIterationsPerEpoch() {
            return iterationLossPerEpoch().stream().map(List::size).collect(Collectors.toList());
        }

        @Override
        @Value.Auxiliary
        @Value.Derived
        default Map<String, Object> toMap() {
            return Map.of(
                "metrics", Map.of(
                    "epochLosses", epochLosses(),
                    "iterationLossesPerEpoch", iterationLossPerEpoch(),
                    "didConverge", didConverge(),
                    "ranEpochs", ranEpochs(),
                    "ranIterationsPerEpoch", ranIterationsPerEpoch()
            ));
        }

        @Override
        default Optional<TrainingMethod> optionalTrainerMethod() { return Optional.empty(); }
    }

    @ValueClass
    public interface ModelTrainResult {

        GraphSageTrainMetrics metrics();

        Layer[] layers();

        static ModelTrainResult of(
            List<List<Double>> iterationLossesPerEpoch,
            boolean converged,
            Layer[] layers
        ) {
            return ImmutableModelTrainResult.builder()
                .layers(layers)
                .metrics(ImmutableGraphSageTrainMetrics.of(iterationLossesPerEpoch, converged))
                .build();
        }
    }
}
