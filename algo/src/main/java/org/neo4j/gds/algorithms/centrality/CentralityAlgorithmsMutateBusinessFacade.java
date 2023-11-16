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
package org.neo4j.gds.algorithms.centrality;

import org.neo4j.gds.algorithms.AlgorithmComputationResult;
import org.neo4j.gds.algorithms.NodePropertyMutateResult;
import org.neo4j.gds.algorithms.centrality.specificfields.StandardCentralityStatisticsSpecificFields;
import org.neo4j.gds.algorithms.mutateservices.MutateNodePropertyService;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.betweenness.BetweennessCentralityMutateConfig;
import org.neo4j.gds.config.MutateNodePropertyConfig;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.result.CentralityStatistics;

import java.util.function.Supplier;

import static org.neo4j.gds.algorithms.runner.AlgorithmRunner.runWithTiming;

public class CentralityAlgorithmsMutateBusinessFacade {

    private final CentralityAlgorithmsFacade centralityAlgorithmsFacade;
    private final MutateNodePropertyService mutateNodePropertyService;

    public CentralityAlgorithmsMutateBusinessFacade(
        CentralityAlgorithmsFacade centralityAlgorithmsFacade,
        MutateNodePropertyService mutateNodePropertyService
    ) {
        this.centralityAlgorithmsFacade = centralityAlgorithmsFacade;
        this.mutateNodePropertyService = mutateNodePropertyService;
    }

    public NodePropertyMutateResult<StandardCentralityStatisticsSpecificFields> betweennessCentrality(
        String graphName,
        BetweennessCentralityMutateConfig configuration,
        boolean shouldComputeCentralityDistribution
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> centralityAlgorithmsFacade.betweennessCentrality(graphName, configuration)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return mutateNodeProperty(
            algorithmResult,
            configuration,
            (bcResult) -> bcResult::get,
            (bcResult) -> NodePropertyValuesAdapter.adapt(bcResult),
            (result, centralityDistribution) -> {
                return new StandardCentralityStatisticsSpecificFields(
                    centralityDistribution
                );
            },
            shouldComputeCentralityDistribution,
            intermediateResult.computeMilliseconds,
            () -> StandardCentralityStatisticsSpecificFields.EMPTY
        );
    }

    <RESULT, CONFIG extends MutateNodePropertyConfig, ASF> NodePropertyMutateResult<ASF> mutateNodeProperty(
        AlgorithmComputationResult<RESULT> algorithmResult,
        CONFIG configuration,
        CentralityFunctionSupplier<RESULT> centralityFunctionSupplier,
        NodePropertyValuesMapper<RESULT> nodePropertyValuesMapper,
        SpecificFieldsWithCentralityDistributionSupplier<RESULT, ASF> specificFieldsSupplier,
        boolean shouldComputeCentralityDistribution,
        long computeMilliseconds,
        Supplier<ASF> emptyASFSupplier
    ) {
        return algorithmResult.result().map(result -> {
            // 2. Construct NodePropertyValues from the algorithm result

            var nodePropertyValues = nodePropertyValuesMapper.map(result);


            // Compute result statistics
            var centralityStatistics = CentralityStatistics.centralityStatistics(
                algorithmResult.graph().nodeCount(),
                centralityFunctionSupplier.centralityFunction(result),
                DefaultPool.INSTANCE,
                configuration.concurrency(),
                shouldComputeCentralityDistribution
            );

            var centralitySummary = CentralityStatistics.centralitySummary(centralityStatistics.histogram());


            // 3. Go and mutate the graph store
            var addNodePropertyResult = mutateNodePropertyService.mutate(
                configuration.mutateProperty(),
                nodePropertyValues,
                configuration.nodeLabelIdentifiers(algorithmResult.graphStore()),
                algorithmResult.graph(), algorithmResult.graphStore()
            );

            var specificFields = specificFieldsSupplier.specificFields(result, centralitySummary);

            return NodePropertyMutateResult.<ASF>builder()
                .computeMillis(computeMilliseconds)
                .postProcessingMillis(0)
                .nodePropertiesWritten(addNodePropertyResult.nodePropertiesAdded())
                .mutateMillis(addNodePropertyResult.mutateMilliseconds())
                .configuration(configuration)
                .algorithmSpecificFields(specificFields)
                .build();
        }).orElseGet(() -> NodePropertyMutateResult.empty(emptyASFSupplier.get(), configuration));

    }

}
