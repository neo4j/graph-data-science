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
package org.neo4j.gds.ml.gradientdescent;

import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.tasks.LogLevel;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.batch.Batch;
import org.neo4j.gds.ml.core.batch.BatchQueue;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.optimizer.AdamOptimizer;
import org.neo4j.gds.ml.core.optimizer.Updater;
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.core.tensor.Tensor;
import org.neo4j.gds.utils.StringFormatting;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.neo4j.gds.ml.core.tensor.TensorFunctions.averageTensors;

public class Training {
    private final GradientDescentConfig config;
    private final ProgressTracker progressTracker;
    private final LogLevel messageLogLevel;
    private final long trainSize;
    private final TerminationFlag terminationFlag;

    public Training(
        GradientDescentConfig config,
        ProgressTracker progressTracker,
        LogLevel messageLogLevel,
        long trainSize,
        TerminationFlag terminationFlag
    ) {
        this.config = config;
        this.progressTracker = progressTracker;
        this.messageLogLevel = messageLogLevel;
        this.trainSize = trainSize;
        this.terminationFlag = terminationFlag;
    }

    public static MemoryEstimation memoryEstimation(
        int numberOfFeatures,
        int numberOfClasses
    ) {
        return memoryEstimation(MemoryRange.of(numberOfFeatures), numberOfClasses);
    }

    public static MemoryEstimation memoryEstimation(
        MemoryRange numberOfFeaturesRange,
        int numberOfClasses
    ) {
        return MemoryEstimations.builder(Training.class.getSimpleName())
            .add(MemoryEstimations.of(
                    "updater",
                    numberOfFeaturesRange.apply(features -> AdamOptimizer.sizeInBytes(
                            numberOfClasses,
                            Math.toIntExact(features)
                        )
                    )
                )
            )
            .perThread(
                "weight gradients",
                numberOfFeaturesRange
                    .apply(features -> Weights.sizeInBytes(numberOfClasses, Math.toIntExact(features)))
            )
            .build();
    }

    public void train(Objective<?> objective, Supplier<BatchQueue> queueSupplier, int concurrency) {
        Updater updater = new AdamOptimizer(objective.weights(), config.learningRate());
        var stopper = TrainingStopper.defaultStopper(config);

        var losses = new ArrayList<Double>();

        var consumers = executeBatches(concurrency, objective, queueSupplier.get());
        var prevWeightGradients = avgWeightGradients(consumers);
        var initialLoss = avgLoss(consumers);
        progressTracker.logMessage(messageLogLevel, StringFormatting.formatWithLocale("Initial loss %s", initialLoss));
        while (!stopper.terminated()) {
            // each loop represents one epoch
            terminationFlag.assertRunning();
            updater.update(prevWeightGradients);
            consumers = executeBatches(concurrency, objective, queueSupplier.get());
            prevWeightGradients = avgWeightGradients(consumers);

            double loss = avgLoss(consumers);
            losses.add(loss);
            stopper.registerLoss(loss);
            progressTracker.logMessage(messageLogLevel, StringFormatting.formatWithLocale(
                "Epoch %d with loss %s",
                losses.size(),
                loss
            ));
        }

        progressTracker.logMessage(messageLogLevel, StringFormatting.formatWithLocale(
            "%s after %d out of %d epochs. Initial loss: %s, Last loss: %s.%s",
            stopper.converged() ? "converged" : "terminated",
            losses.size(),
            config.maxEpochs(),
            initialLoss,
            losses.get(losses.size() - 1),
            stopper.converged() ? "" : " Did not converge"

        ));
    }

    private List<ObjectiveUpdateConsumer> executeBatches(int concurrency, Objective<?> objective, BatchQueue batches) {
        var consumers = new ArrayList<ObjectiveUpdateConsumer>(concurrency);
        for (int i = 0; i < concurrency; i++) {
            consumers.add(new ObjectiveUpdateConsumer(objective, trainSize));
        }
        batches.parallelConsume(concurrency, consumers, terminationFlag);
        return consumers;
    }

    private List<? extends Tensor<? extends Tensor<?>>> avgWeightGradients(List<ObjectiveUpdateConsumer> consumers) {
        List<? extends List<? extends Tensor<?>>> localGradientSums = consumers
            .stream()
            .map(ObjectiveUpdateConsumer::summedWeightGradients)
            .collect(Collectors.toList());

        var numberOfBatches = consumers
            .stream()
            .mapToInt(ObjectiveUpdateConsumer::consumedBatches)
            .sum();

        return averageTensors(localGradientSums, numberOfBatches);
    }

    private double avgLoss(List<ObjectiveUpdateConsumer> consumers) {
        return consumers.stream().mapToDouble(ObjectiveUpdateConsumer::lossSum).sum() / consumers.stream().mapToInt(ObjectiveUpdateConsumer::consumedBatches).sum();
    }

    static class ObjectiveUpdateConsumer implements Consumer<Batch> {
        private final Objective<?> objective;
        private final long trainSize;
        private final List<? extends Tensor<?>> summedWeightGradients;
        private double lossSum;
        private int consumedBatches;

        ObjectiveUpdateConsumer(
            Objective<?> objective,
            long trainSize
        ) {
            this.objective = objective;
            this.trainSize = trainSize;
            this.summedWeightGradients = objective
                .weights()
                .stream()
                .map(weight -> weight.data().createWithSameDimensions())
                .collect(Collectors.toList());
            this.consumedBatches = 0;
            this.lossSum = 0;
        }

        @Override
        public void accept(Batch batch) {
            Variable<Scalar> loss = objective.loss(batch, trainSize);
            var ctx = new ComputationContext();
            lossSum += ctx.forward(loss).value();
            ctx.backward(loss);

            List<? extends Tensor<?>> localWeightGradient = objective
                .weights()
                .stream()
                .map(ctx::gradient)
                .collect(Collectors.toList());

            for (int i = 0; i < summedWeightGradients.size(); i++) {
                summedWeightGradients.get(i).addInPlace(localWeightGradient.get(i));
            }

            consumedBatches++;
        }

        List<? extends Tensor<?>> summedWeightGradients() {
            return summedWeightGradients;
        }

        int consumedBatches() {
            return consumedBatches;
        }

        double lossSum() {
            return lossSum;
        }
    }

}
