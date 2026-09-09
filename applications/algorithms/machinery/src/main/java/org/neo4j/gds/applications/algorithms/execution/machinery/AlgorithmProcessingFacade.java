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

import org.neo4j.gds.GraphParameters;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.algorithms.machinery.DimensionTransformer;
import org.neo4j.gds.applications.algorithms.machinery.Label;
import org.neo4j.gds.applications.algorithms.machinery.ResultRenderer;
import org.neo4j.gds.applications.algorithms.machinery.SideEffect;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.RequestCorrelationId;
import org.neo4j.gds.core.loading.validation.GraphStoreValidation;
import org.neo4j.gds.core.loading.validation.GraphValidation;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Here is <a href="https://en.wikipedia.org/wiki/Facade_pattern">a front-facing interface masking more complex underlying or structural code</a>,
 * in this case, the full stack of asynchronous algorithm processing.
 * This is what we should be using to process any algorithm in any context.
 * Algorithms running from Cypher in the plugin immediately block and wait, turning the asynchronous job synchronous.
 * Other calling contexts can let the computation run in the background and come back for results later.
 */
public interface AlgorithmProcessingFacade {
    /**
     * This is a very long parameter list.
     * It is _on purpose_ a very long parameter list.
     * This object ought to live as a singleton, across requests.
     * Thus, request scoped things are just parameters.
     * And of course, the parameter list is the superset of parameters needed for downstream services,
     * to perform all the tasks: loading, running, {streaming, mutating, writing}, rendering.
     * It is a lot. Should we apply <a href="https://refactoring.com/catalog/introduceParameterObject.html">Parameter Object</a>?
     * The important bit here is: keep this lifetime scoped, not request scoped;
     * use parameterisation, not constructor injection.
     *
     * And in the interest of practicality: since this is an interface for the purpose of decorating, let's have only this one method - less work decorating innit.
     * Convenience and overrides can live above.
     */
    <CONFIGURATION extends AlgoBaseConfig, RESULT, METADATA, TRANSFORMED_RESULT> CompletableFuture<TRANSFORMED_RESULT> loadGraphThenRunAlgorithm(
        DatabaseId databaseId,
        GraphName graphName,
        RequestCorrelationId requestCorrelationId,
        User user,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        GraphStoreValidation graphStoreValidation,
        boolean includeGraph,
        Optional<GraphValidation> graphValidation,
        ConstructAndRun<RESULT> constructAndRun,
        CONFIGURATION configuration,
        TerminationFlag terminationFlag,
        DimensionTransformer dimensionTransformer,
        Supplier<MemoryEstimation> estimationSupplier,
        Label label,
        Optional<SideEffect<RESULT, METADATA>> sideEffect,
        ResultRenderer<RESULT, TRANSFORMED_RESULT, METADATA> resultRenderer
    );
}
