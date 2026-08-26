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
package org.neo4j.gds.applications.algorithms.execution;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.execution.machinery.AlgorithmProcessingFacade;
import org.neo4j.gds.applications.algorithms.execution.machinery.ConstructAndRun;
import org.neo4j.gds.applications.algorithms.machinery.DimensionTransformer;
import org.neo4j.gds.applications.algorithms.machinery.Label;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultRenderer;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.loading.validation.GraphStoreValidation;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryEstimation;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Some parameter embellishment, to reduce parameter lists sizes.
 * This object is request scoped, so that it can carry all the implicit parameters relating to the request.
 */
public class AlgorithmProcessingFacadeConvenience {
    private final Log log;
    private final AlgorithmProcessingFacade algorithmProcessingFacade;
    private final RequestScopedDependencies requestScopedDependencies;

    public AlgorithmProcessingFacadeConvenience(
        Log log,
        AlgorithmProcessingFacade algorithmProcessingFacade,
        RequestScopedDependencies requestScopedDependencies
    ) {
        this.log = log;
        this.algorithmProcessingFacade = algorithmProcessingFacade;
        this.requestScopedDependencies = requestScopedDependencies;
    }

    /**
     * This is currently specific convenience for a synchronous streaming mode algorithm, that is _regular_.
     * That means no relationship override, no graph store validation, no graph validation, no dimension transformer,
     * no side effect. We have enough of those to warrant this shortcut.
     * There will be other convenience methods.
     *
     * @throws java.lang.RuntimeException if your work was interrupted, or if something went wrong. that's work as in, could be any stage that got interrupted, there might even have been side effects completed
     */
    public <CONFIGURATION extends AlgoBaseConfig, RESULT, RENDERING> Stream<RENDERING> runAlgorithm(
        GraphName graphName,
        CONFIGURATION configuration,
        ConstructAndRun<RESULT> constructAndRun,
        Supplier<MemoryEstimation> memoryEstimationSupplier,
        Label label,
        StreamResultBuilder<RESULT, RENDERING> streamResultBuilder
    ) {
        var future = algorithmProcessingFacade.loadGraphThenRunAlgorithm(
            requestScopedDependencies.databaseId(),
            graphName,
            requestScopedDependencies.correlationId(),
            requestScopedDependencies.user(),
            configuration.toGraphParameters(),
            Optional.empty(), // simple basic convenience here
            GraphStoreValidation.DISABLED, // make this an optional...
            true, // simple basic convenience here
            Optional.empty(), // or make this a DISABLED
            constructAndRun,
            configuration,
            requestScopedDependencies.terminationFlag(),
            DimensionTransformer.DISABLED, // simple basic convenience here
            memoryEstimationSupplier,
            label,
            Optional.empty(), // no side effects in stream mode
            new StreamResultRenderer<>(streamResultBuilder)
        );

        try {
            return future.get();
        } catch (InterruptedException e) {
            log.error("interruption error, your work could not be completed", e);
            throw new RuntimeException("interruption error", e);
        } catch (ExecutionException e) {
            log.error("execution error, something went wrong while executing your work", e);
            throw new RuntimeException("execution error", e);
        }
    }
}
