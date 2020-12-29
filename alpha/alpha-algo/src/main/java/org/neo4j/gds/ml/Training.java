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
package org.neo4j.gds.ml;

import org.neo4j.gds.embeddings.graphsage.ddl4j.ComputationContext;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Scalar;
import org.neo4j.logging.Log;

import java.util.concurrent.atomic.DoubleAdder;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class Training {
    private final TrainingSettings settings;
    private final Log log;

    public Training(TrainingSettings settings, Log log) {
        this.settings = settings;
        this.log = log;
    }

    public void train(Objective objective, Supplier<BatchQueue> queueSupplier, int concurrency) {
        Updater singleUpdater = settings.sharedUpdater() ? settings.updater(objective.weights()) : null;
        Updater[] updaters = null;
        if (!settings.sharedUpdater()) {
            updaters = new Updater[concurrency];
            for (int i = 0; i < concurrency; i++) {
                updaters[i] = settings.updater(objective.weights());
            }
        }
        int epoch = 0;
        TrainingStopper stopper = settings.stopper();
        double initialLoss = evaluateLoss(objective, queueSupplier.get(), concurrency);
        double lastLoss = initialLoss;
        while (!stopper.terminated()) {
            trainEpoch(settings, objective, queueSupplier.get(), concurrency, singleUpdater, updaters);
            lastLoss = evaluateLoss(objective, queueSupplier.get(), concurrency);
            stopper.registerLoss(lastLoss);
            epoch++;
            log.debug(formatWithLocale("Loss: %s, After Epoch: %d", lastLoss, epoch));
        }
        log.debug(formatWithLocale(
            "Training %s after %d epochs. Initial loss: %s, Last loss: %s.%s",
            stopper.converged() ? "converged" : "terminated",
            epoch,
            initialLoss,
            lastLoss,
            stopper.converged() ? "" : "Did not converge"
        ));
    }

    private double evaluateLoss(Objective objective, BatchQueue batches, int concurrency) {
        DoubleAdder totalLoss = new DoubleAdder();

        batches.parallelConsume(
            new LossEvalConsumer(
                objective,
                totalLoss
            ),
            concurrency
        );

        return totalLoss.doubleValue();
    }

    private void trainEpoch(
        TrainingSettings settings,
        Objective objective,
        BatchQueue batches,
        int concurrency,
        Updater singleUpdater,
        Updater[] updaters
    ) {
        batches.parallelConsume(
            concurrency,
            jobId ->
                new ObjectiveUpdateConsumer(
                    objective,
                    settings.sharedUpdater() ? singleUpdater : updaters[jobId]
                )
        );
    }

    static class ObjectiveUpdateConsumer implements Consumer<Batch> {
        private final Objective objective;
        private final Updater updater;

        ObjectiveUpdateConsumer(Objective objective, Updater updater) {
            this.objective = objective;
            this.updater = updater;
        }

        @Override
        public void accept(Batch batch) {
            Variable<Scalar> loss = objective.loss(batch);
            ComputationContext ctx = new ComputationContext();
            ctx.forward(loss);
            ctx.backward(loss);
            updater.update(ctx);
        }
    }

    static class LossEvalConsumer implements Consumer<Batch> {
        private final Objective objective;
        private final DoubleAdder totalLoss;

        LossEvalConsumer(Objective objective, DoubleAdder lossAdder) {
            this.objective = objective;
            this.totalLoss = lossAdder;
        }

        @Override
        public void accept(Batch batch) {
            Variable<Scalar> loss = objective.loss(batch);
            ComputationContext ctx = new ComputationContext();
            totalLoss.add(ctx.forward(loss).value());
        }

    }
}
