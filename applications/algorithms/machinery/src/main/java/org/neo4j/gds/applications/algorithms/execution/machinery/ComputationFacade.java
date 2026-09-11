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
import java.util.function.Supplier;

/**
 * Here is <a href="https://en.wikipedia.org/wiki/Facade_pattern">a front-facing interface masking more complex underlying or structural code</a>,
 * namely the synchronous execution of an algorithm, possibly applying a side effect, and rendering a result.
 * On top is sprinkled validation, checks, instrumentation etc. It is a right Swiss Army knife.
 * Think of this as a marker of a layer; you should not be too concerned about the innards.
 */
final class ComputationFacade {
    private final MemoryChecker memoryChecker;
    private final SideEffectApplicator sideEffectApplicator;

    private ComputationFacade(MemoryChecker memoryChecker, SideEffectApplicator sideEffectApplicator) {
        this.memoryChecker = memoryChecker;
        this.sideEffectApplicator = sideEffectApplicator;
    }

    static ComputationFacade create(MemoryChecker memoryChecker) {
        var sideEffectApplicator = new SideEffectApplicator();

        return new ComputationFacade(memoryChecker, sideEffectApplicator);
    }

    /**
     * A computation has three main stages:
     *
     * <ol>
     *     <li>We run the algorithma</li>
     *     <li>We apply a side effect</li>
     *     <li>We render a result for output</li>
     * </ol>
     */
    <CONFIGURATION extends AlgoBaseConfig, RESULT, METADATA, TRANSFORMED_RESULT> TRANSFORMED_RESULT runAlgorithmApplySideEffectTransformResult(
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
        var result = checkGraphAndContinue(
            requestScopedLog,
            timingsBuilder,
            graphResources,
            user,
            constructAndRun,
            configuration,
            dimensionTransformer,
            estimationSupplier,
            label
        );

        requestScopedLog.onProcessingResult();
        var metadata = sideEffectApplicator.applySideEffect(timingsBuilder, graphResources, result, sideEffect);

        // work is done, we can finalise timings
        var timings = timingsBuilder.build();

        requestScopedLog.onRenderingOutput();
        return resultRenderer.render(graphResources, result, timings, metadata);
    }

    private <CONFIGURATION extends AlgoBaseConfig, RESULT> Optional<RESULT> checkGraphAndContinue(
        RequestScopedLog requestScopedLog,
        AlgorithmProcessingTimingsBuilder timingsBuilder,
        GraphResources graphResources,
        User user,
        ConstructAndRun<RESULT> constructAndRun,
        CONFIGURATION configuration,
        DimensionTransformer dimensionTransformer,
        Supplier<MemoryEstimation> estimationSupplier,
        Label label
    ) {
        if (graphResources.graph().isEmpty()) return Optional.empty();

        requestScopedLog.onComputing();
        var result = memoryChecker.checkMemoryAndRunAlgorithm(
            timingsBuilder,
            graphResources,
            estimationSupplier,
            dimensionTransformer,
            user,
            label,
            configuration,
            constructAndRun
        );

        return Optional.of(result);
    }
}
