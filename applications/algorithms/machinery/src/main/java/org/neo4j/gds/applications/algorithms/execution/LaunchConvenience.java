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
import org.neo4j.gds.applications.algorithms.machinery.ResultRenderer;
import org.neo4j.gds.applications.algorithms.machinery.SideEffect;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.loading.PostLoadETLHook;
import org.neo4j.gds.core.loading.validation.ValidationRule;
import org.neo4j.gds.mem.MemoryEstimation;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Some parameter embellishment, to reduce parameter lists sizes.
 * This object is request scoped, so that it can carry all the implicit parameters relating to the request.
 */
public class LaunchConvenience {
    private final ValidationRuleFromConfigurationParser validationRuleFromConfigurationParser = ValidationRuleFromConfigurationParser.create();

    private final AlgorithmProcessingFacade algorithmProcessingFacade;
    private final RequestScopedDependencies requestScopedDependencies;

    public LaunchConvenience(
        AlgorithmProcessingFacade algorithmProcessingFacade,
        RequestScopedDependencies requestScopedDependencies
    ) {
        this.algorithmProcessingFacade = algorithmProcessingFacade;
        this.requestScopedDependencies = requestScopedDependencies;
    }

    /**
     * This is currently the most specific convenience needed thus far.
     * It does no graph validation, no dimension transformer.
     * It does do relationship override, graph store validation and side effects, however.
     * There will be other convenience methods, and we can manage them over time to be overloads of this one,
     * to avoid duplication.
     * <p>
     * _Work_ is launched asynchronously, caller deals with completing work and handling errors.
     */
    public <CONFIGURATION extends AlgoBaseConfig, RESULT, METADATA, RENDERING> CompletableFuture<RENDERING> launchAlgorithm(
        GraphName graphName,
        CONFIGURATION configuration,
        Optional<String> relationshipProperty, // only used by conductance thus far
        Iterable<ValidationRule> algorithmSpecificValidationRules,
        Optional<Iterable<PostLoadETLHook>> postLoadETLHooks, // only used by hits thus far
        ConstructAndRun<RESULT> constructAndRun,
        Supplier<MemoryEstimation> memoryEstimationSupplier,
        Label label,
        Optional<SideEffect<RESULT, METADATA>> sideEffect,
        ResultRenderer<RESULT, RENDERING, METADATA> resultRenderer
    ) {
        var builder = validationRuleFromConfigurationParser.parse(configuration);
        algorithmSpecificValidationRules.forEach(builder::withValidationRule);
        var graphStoreValidation = builder.build();

        return algorithmProcessingFacade.loadGraphThenRunAlgorithm(
            requestScopedDependencies.databaseId(),
            graphName,
            requestScopedDependencies.correlationId(),
            requestScopedDependencies.user(),
            configuration.toGraphParameters(),
            relationshipProperty,
            graphStoreValidation,
            postLoadETLHooks,
            Optional.empty(), // or make this a DISABLED
            constructAndRun,
            configuration,
            requestScopedDependencies.terminationFlag(),
            DimensionTransformer.DISABLED, // simple basic convenience here
            memoryEstimationSupplier,
            label,
            sideEffect,
            resultRenderer
        );
    }
}
