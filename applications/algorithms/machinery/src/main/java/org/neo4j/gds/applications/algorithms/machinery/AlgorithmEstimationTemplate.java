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
package org.neo4j.gds.applications.algorithms.machinery;

import org.neo4j.gds.GraphParameters;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.memest.MemoryEstimationGraphConfigParser;

import java.util.Map;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

/**
 * All estimations look the same modulo some small variation. So this calls for something like Template Method.
 * But we do not like inheritance, so the hooks are injected
 */
public class AlgorithmEstimationTemplate {
    private final FictitiousAlgorithmEstimationService fictitiousAlgorithmEstimationService;
    private final DatabaseAlgorithmEstimationService databaseAlgorithmEstimationService;
    private final ProjectedAlgorithmEstimationService projectedAlgorithmEstimationService;

    // request scoped parameters and services
    private final RequestScopedDependencies requestScopedDependencies;

    public AlgorithmEstimationTemplate(
        FictitiousAlgorithmEstimationService fictitiousAlgorithmEstimationService,
        DatabaseAlgorithmEstimationService DatabaseAlgorithmEstimationService,
        ProjectedAlgorithmEstimationService projectedAlgorithmEstimationService,
        RequestScopedDependencies requestScopedDependencies
    ) {
        this.fictitiousAlgorithmEstimationService = fictitiousAlgorithmEstimationService;
        this.databaseAlgorithmEstimationService = DatabaseAlgorithmEstimationService;
        this.projectedAlgorithmEstimationService = projectedAlgorithmEstimationService;
        this.requestScopedDependencies = requestScopedDependencies;
    }

    public MemoryEstimateResult estimate(
        GraphParameters graphParameters,
        Object graphNameOrConfiguration,
        MemoryEstimation memoryEstimation,
        Concurrency concurrency
    ) {
        return estimate(
            graphParameters,
            graphNameOrConfiguration,
            memoryEstimation,
            concurrency,
            DimensionTransformer.DISABLED
        );
    }

    public MemoryEstimateResult estimate(
        GraphParameters graphParameters,
        Object graphNameOrConfiguration,
        MemoryEstimation memoryEstimation,
        Concurrency concurrency,
        DimensionTransformer dimensionTransformer
    ) {
        if (graphNameOrConfiguration instanceof Map<?, ?> graphConfig) {
            var memoryEstimationGraphConfigParser = new MemoryEstimationGraphConfigParser(requestScopedDependencies.user().getUsername());
            @SuppressWarnings("unchecked")
            var projectionConfiguration = memoryEstimationGraphConfigParser.parse((Map<String, Object>) graphConfig);

            return projectionConfiguration.isFictitiousLoading()
                ? fictitiousAlgorithmEstimationService.estimate(projectionConfiguration, memoryEstimation, concurrency, dimensionTransformer)
                : databaseAlgorithmEstimationService.estimate(projectionConfiguration, memoryEstimation, concurrency, dimensionTransformer);
        }

        if (graphNameOrConfiguration instanceof String rawGraphName) {
            GraphName graphName = GraphName.parse(rawGraphName);

            return projectedAlgorithmEstimationService.estimate(
                graphName,
                graphParameters,
                memoryEstimation,
                concurrency,
                dimensionTransformer
            );
        }

        throw new IllegalArgumentException(formatWithLocale(
            "Expected `graphNameOrConfiguration` to be of type String or Map, but got `%s`",
            graphNameOrConfiguration.getClass().getSimpleName()
        ));
    }


}
