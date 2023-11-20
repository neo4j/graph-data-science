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
import org.neo4j.gds.algorithms.NodePropertyWriteResult;
import org.neo4j.gds.algorithms.centrality.specificfields.CentralityStatisticsSpecificFields;
import org.neo4j.gds.algorithms.centrality.specificfields.DefaultCentralitySpecificFields;
import org.neo4j.gds.algorithms.writeservices.WriteNodePropertyService;
import org.neo4j.gds.betweenness.BetweennessCentralityWriteConfig;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.ArrowConnectionInfo;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.degree.DegreeCentralityWriteConfig;
import org.neo4j.gds.result.CentralityStatistics;

import java.util.Optional;
import java.util.function.Supplier;

import static org.neo4j.gds.algorithms.runner.AlgorithmRunner.runWithTiming;

public class CentralityAlgorithmsWriteBusinessFacade {

    private final CentralityAlgorithmsFacade centralityAlgorithmsFacade;
    private final WriteNodePropertyService writeNodePropertyService;

    public CentralityAlgorithmsWriteBusinessFacade(
        CentralityAlgorithmsFacade centralityAlgorithmsFacade,
        WriteNodePropertyService writeNodePropertyService
    ) {
        this.centralityAlgorithmsFacade = centralityAlgorithmsFacade;
        this.writeNodePropertyService = writeNodePropertyService;
    }

    public NodePropertyWriteResult<DefaultCentralitySpecificFields> betweennessCentrality(
        String graphName,
        BetweennessCentralityWriteConfig configuration,
        boolean shouldComputeCentralityDistribution
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> centralityAlgorithmsFacade.betweennessCentrality(graphName, configuration)
        );

        return writeToDatabase(
            intermediateResult.algorithmResult,
            configuration,
            shouldComputeCentralityDistribution,
            intermediateResult.computeMilliseconds,
            "BetweennessCentralityWrite",
            configuration.writeConcurrency(),
            configuration.writeProperty(),
            configuration.arrowConnectionInfo()
        );
    }

    public NodePropertyWriteResult<DefaultCentralitySpecificFields> degreeCentrality(
        String graphName,
        DegreeCentralityWriteConfig configuration,
        boolean shouldComputeCentralityDistribution
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> centralityAlgorithmsFacade.degreeCentrality(graphName, configuration)
        );

        return writeToDatabase(
            intermediateResult.algorithmResult,
            configuration,
            shouldComputeCentralityDistribution,
            intermediateResult.computeMilliseconds,
            "DegreeCentralityWrite",
            configuration.writeConcurrency(),
            configuration.writeProperty(),
            configuration.arrowConnectionInfo()
        );
    }


    <RESULT extends CentralityAlgorithmResult, CONFIG extends AlgoBaseConfig> NodePropertyWriteResult<DefaultCentralitySpecificFields> writeToDatabase(
        AlgorithmComputationResult<RESULT> algorithmResult,
        CONFIG configuration,
        boolean shouldComputeCentralityDistribution,
        long computeMilliseconds,
        String procedureName,
        int writeConcurrency,
        String writeProperty,
        Optional<ArrowConnectionInfo> arrowConnectionInfo
    ) {

        CentralityFunctionSupplier<RESULT> centralityFunctionSupplier = (r) -> r.centralityScoreProvider();
        SpecificFieldsWithCentralityDistributionSupplier<RESULT, DefaultCentralitySpecificFields> specificFieldsSupplier = (r, c) -> new DefaultCentralitySpecificFields(
            c);
        Supplier<DefaultCentralitySpecificFields> emptyASFSupplier = () -> DefaultCentralitySpecificFields.EMPTY;

        NodePropertyValuesMapper<RESULT> nodePropertyValuesMapper = (r) -> r.nodePropertyValues();

        return writeToDatabase(
            algorithmResult,
            configuration,
            centralityFunctionSupplier,
            nodePropertyValuesMapper,
            specificFieldsSupplier,
            shouldComputeCentralityDistribution,
            computeMilliseconds,
            emptyASFSupplier,
            procedureName,
            writeConcurrency,
            writeProperty,
            arrowConnectionInfo
        );
    }

    <RESULT, CONFIG extends AlgoBaseConfig, ASF extends CentralityStatisticsSpecificFields> NodePropertyWriteResult<ASF> writeToDatabase(
        AlgorithmComputationResult<RESULT> algorithmResult,
        CONFIG configuration,
        CentralityFunctionSupplier<RESULT> centralityFunctionSupplier,
        NodePropertyValuesMapper<RESULT> nodePropertyValuesMapper,
        SpecificFieldsWithCentralityDistributionSupplier<RESULT, ASF> specificFieldsSupplier,
        boolean shouldComputeCentralityDistribution,
        long computeMilliseconds,
        Supplier<ASF> emptyASFSupplier,
        String procedureName,
        int writeConcurrency,
        String writeProperty,
        Optional<ArrowConnectionInfo> arrowConnectionInfo
    ) {

        return algorithmResult.result().map(result -> {
            // 2. Construct NodePropertyValues from the algorithm result
            // 2.1 Should we measure some post-processing here?
            var nodePropertyValues = nodePropertyValuesMapper.map(
                result
            );

            // 3. Write to database
            var writeNodePropertyResult = writeNodePropertyService.write(
                algorithmResult.graph(),
                algorithmResult.graphStore(),
                nodePropertyValues,
                writeConcurrency,
                writeProperty,
                procedureName,
                arrowConnectionInfo
            );

            // Compute result statistics
            var centralityStatistics = CentralityStatistics.centralityStatistics(
                algorithmResult.graph().nodeCount(),
                centralityFunctionSupplier.centralityFunction(result),
                DefaultPool.INSTANCE,
                configuration.concurrency(),
                shouldComputeCentralityDistribution
            );

            var centralitySummary = CentralityStatistics.centralitySummary(centralityStatistics.histogram());

            var specificFields = specificFieldsSupplier.specificFields(result, centralitySummary);

            return NodePropertyWriteResult.<ASF>builder()
                .computeMillis(computeMilliseconds)
                .postProcessingMillis(centralityStatistics.computeMilliseconds())
                .nodePropertiesWritten(writeNodePropertyResult.nodePropertiesWritten())
                .writeMillis(writeNodePropertyResult.writeMilliseconds())
                .configuration(configuration)
                .algorithmSpecificFields(specificFields)
                .build();
        }).orElseGet(() -> NodePropertyWriteResult.empty(emptyASFSupplier.get(), configuration));

    }
}
