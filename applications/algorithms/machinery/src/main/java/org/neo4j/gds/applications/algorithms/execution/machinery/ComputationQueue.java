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
package org.neo4j.gds.applications.algorithms.execution.machinery;

import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimingsBuilder;
import org.neo4j.gds.applications.algorithms.machinery.DimensionTransformer;
import org.neo4j.gds.applications.algorithms.machinery.Label;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedLog;
import org.neo4j.gds.applications.algorithms.machinery.ResultRenderer;
import org.neo4j.gds.applications.algorithms.machinery.SideEffect;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.loading.GraphResources;
import org.neo4j.gds.mem.MemoryEstimation;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/**
 * This is where we take your work and line it up for execution asynchronously,
 * in an {@link java.util.concurrent.ExecutorService}.
 * Choosing that executor service is where we distinguish running modes,
 * be it free for all, or something bounded.
 */
class ComputationQueue {
    private final ExecutorService executorService;
    private final ComputationFacade computationFacade;

    ComputationQueue(ExecutorService executorService, ComputationFacade computationFacade) {
        this.executorService = executorService;
        this.computationFacade = computationFacade;
    }

    /**
     * We return {@link java.util.concurrent.CompletableFuture} because those are nicer to work with for callers,
     * compared to working with basic old {@link java.util.concurrent.Future}s directly.
     * Especially the {@link java.util.concurrent.CompletableFuture#whenComplete(java.util.function.BiConsumer)} hook is handy.
     *
     * @return a {@link java.util.concurrent.CompletableFuture} representing your work, dispatched onto (an)other thread(s)
     */
    <CONFIGURATION extends AlgoBaseConfig, RESULT, METADATA, TRANSFORMED_RESULT> CompletableFuture<TRANSFORMED_RESULT> enqueueComputation(
        RequestScopedLog requestScopedLog,
        AlgorithmProcessingTimingsBuilder timingsBuilder,
        GraphResources graphResources,
        User user,
        ConstructAndRun<RESULT> constructAndRun,
        CONFIGURATION configuration,
        DimensionTransformer dimensionTransformer,
        Supplier<MemoryEstimation> estimationSupplier,
        Label label,
        Optional<SideEffect<RESULT, METADATA>> sideEffect,
        ResultRenderer<RESULT, TRANSFORMED_RESULT, METADATA> resultRenderer
    ) {
        requestScopedLog.onEnqueueingWork();
        return CompletableFuture.supplyAsync(
            () -> {
                requestScopedLog.onExecutingWork();

                return computationFacade.runAlgorithmApplySideEffectTransformResult(
                    requestScopedLog,
                    timingsBuilder,
                    graphResources,
                    user,
                    constructAndRun,
                    configuration,
                    dimensionTransformer,
                    estimationSupplier,
                    label,
                    sideEffect,
                    resultRenderer
                );
            }, executorService
        );
    }
}
