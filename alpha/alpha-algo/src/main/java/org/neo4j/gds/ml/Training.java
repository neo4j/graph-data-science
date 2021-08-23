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
package org.neo4j.gds.ml;

import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.neo4j.gds.ml.core.tensor.TensorFunctions.averageTensors;

public class Training {
    private final TrainingConfig config;
    private final ProgressTracker progressTracker;
    private final long trainSize;
    private final TerminationFlag terminationFlag;

    public Training(
        TrainingConfig config,
        ProgressTracker progressTracker,
        long trainSize,
        TerminationFlag terminationFlag
    ) {
        this.config = config;
        this.progressTracker = progressTracker;
        this.trainSize = trainSize;
        this.terminationFlag = terminationFlag;
    }

    public static MemoryEstimation memoryEstimation(
        int numberOfFeatures,
        int numberOfClasses,
        int numberOfWeights
    ) {
        return MemoryEstimations.builder(Training.class)
            .add(MemoryEstimations.of(
                "updater",
                MemoryRange.of(AdamOptimizer.sizeInBytes(numberOfClasses, numberOfFeatures, numberOfWeights))))
            .perThread("weight gradients", Weights.sizeInBytes(numberOfClasses, numberOfFeatures) * numberOfWeights)
            .build();
    }

    public void train(Objective<?> objective, Supplier<BatchQueue> queueSupplier, int concurrency) {
        Updater updater = new AdamOptimizer(objective.weights());
        int epoch = 0;
        var stopper = TrainingStopper.defaultStopper(config);
        double initialLoss = evaluateLoss(objective, queueSupplier.get(), concurrency);
        double lastLoss = initialLoss;

        while (!stopper.terminated()) {
            terminationFlag.assertRunning();

            trainEpoch(objective, queueSupplier.get(), concurrency, updater);
            lastLoss = evaluateLoss(objective, queueSupplier.get(), concurrency);
            stopper.registerLoss(lastLoss);
            epoch++;
            progressTracker.logProgress();
            progressTracker.progressLogger().getLog().debug("Loss: %s, After Epoch: %d", lastLoss, epoch);
        }
        progressTracker.progressLogger().getLog().debug(
            "Training %s after %d epochs. Initial loss: %s, Last loss: %s.%s",
            stopper.converged() ? "converged" : "terminated",
            epoch,
            initialLoss,
            lastLoss,
            stopper.converged() ? "" : " Did not converge"
        );
    }

    private double evaluateLoss(Objective<?> objective, BatchQueue batches, int concurrency) {
        DoubleAdder totalLoss = new DoubleAdder();

        batches.parallelConsume(
            new LossEvalConsumer(
                objective,
                totalLoss,
                trainSize
            ),
            concurrency,
            terminationFlag
        );

        return totalLoss.doubleValue();
    }

    private void trainEpoch(
        Objective<?> objective,
        BatchQueue batches,
        int concurrency,
        Updater updater
    ) {
        var consumers = new ArrayList<ObjectiveUpdateConsumer>(concurrency);
        for (int i = 0; i < concurrency; i++) {
            consumers.add(new ObjectiveUpdateConsumer(objective, trainSize));
        }

        batches.parallelConsume(concurrency, consumers, terminationFlag);

        List<? extends List<? extends Tensor<?>>> localGradientSums = consumers
            .stream()
            .map(ObjectiveUpdateConsumer::summedWeightGradients)
            .collect(Collectors.toList());

        var numberOfBatches = consumers
            .stream()
            .mapToInt(ObjectiveUpdateConsumer::consumedBatches)
            .sum();


        var avgWeightGradients = averageTensors(localGradientSums, numberOfBatches);
        updater.update(avgWeightGradients);
    }

    static class ObjectiveUpdateConsumer implements Consumer<Batch> {
        private final Objective<?> objective;
        private final long trainSize;
        private List<? extends Tensor<?>> summedWeightGradients;
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
        }

        @Override
        public void accept(Batch batch) {
            Variable<Scalar> loss = objective.loss(batch, trainSize);
            var ctx = new ComputationContext();
            ctx.forward(loss);
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
    }

    static class LossEvalConsumer implements Consumer<Batch> {
        private final Objective<?> objective;
        private final DoubleAdder totalLoss;
        private final long trainSize;

        LossEvalConsumer(Objective<?> objective, DoubleAdder lossAdder, long trainSize) {
            this.objective = objective;
            this.totalLoss = lossAdder;
            this.trainSize = trainSize;
        }

        @Override
        public void accept(Batch batch) {
            Variable<Scalar> loss = objective.loss(batch, trainSize);
            ComputationContext ctx = new ComputationContext();
            totalLoss.add(ctx.forward(loss).value());
        }

    }
}
