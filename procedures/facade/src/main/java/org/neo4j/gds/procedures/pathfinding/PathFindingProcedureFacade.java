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
package org.neo4j.gds.procedures.pathfinding;

import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsBusinessFacade;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsWriteModeBusinessFacade;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarMutateConfig;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarStreamConfig;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarWriteConfig;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraMutateConfig;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraStreamConfig;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraMutateConfig;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraStreamConfig;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraWriteConfig;
import org.neo4j.gds.paths.yens.config.ShortestPathYensMutateConfig;
import org.neo4j.gds.paths.yens.config.ShortestPathYensStreamConfig;
import org.neo4j.gds.paths.yens.config.ShortestPathYensWriteConfig;
import org.neo4j.gds.procedures.algorithms.ConfigurationCreator;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.results.StandardWriteRelationshipsResult;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * This is the top facade on the Neo4j Procedures integration for path finding algorithms.
 * The role it plays is, to be newed up with request scoped dependencies,
 * and to capture the procedure-specific bits of path finding algorithms calls.
 * For example, translating a return column specification into a parameter, a business level concept.
 * This is also where we put result rendering.
 */
public class PathFindingProcedureFacade {
    // request scoped services
    private final CloseableResourceRegistry closeableResourceRegistry;
    private final ConfigurationCreator configurationCreator;
    private final NodeLookup nodeLookup;
    private final ProcedureReturnColumns procedureReturnColumns;

    // delegate
    private final PathFindingAlgorithmsBusinessFacade facade;
    private final PathFindingAlgorithmsWriteModeBusinessFacade writeModeFacade;

    public PathFindingProcedureFacade(
        CloseableResourceRegistry closeableResourceRegistry,
        ConfigurationCreator configurationCreator,
        NodeLookup nodeLookup,
        ProcedureReturnColumns procedureReturnColumns,
        PathFindingAlgorithmsBusinessFacade facade,
        PathFindingAlgorithmsWriteModeBusinessFacade writeModeFacade
    ) {
        this.closeableResourceRegistry = closeableResourceRegistry;
        this.configurationCreator = configurationCreator;
        this.nodeLookup = nodeLookup;
        this.procedureReturnColumns = procedureReturnColumns;

        this.facade = facade;
        this.writeModeFacade = writeModeFacade;
    }

    public Stream<PathFindingMutateResult> singlePairShortestPathAStarMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.of(
            runMutateAlgorithm(
                graphName,
                configuration,
                ShortestPathAStarMutateConfig::of,
                facade::singlePairShortestPathAStarMutate
            )
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathAStarMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            ShortestPathAStarMutateConfig::of,
            configuration -> facade.singlePairShortestPathAStarEstimate(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<PathFindingStreamResult> singlePairShortestPathAStarStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return runStreamAlgorithm(
            graphName,
            configuration,
            ShortestPathAStarStreamConfig::of,
            facade::singlePairShortestPathAStarStream
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathAStarStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            ShortestPathAStarStreamConfig::of,
            configuration -> facade.singlePairShortestPathAStarEstimate(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<StandardWriteRelationshipsResult> singlePairShortestPathAStarWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.of(
            runWriteAlgorithm(
                graphName,
                configuration,
                ShortestPathAStarWriteConfig::of,
                writeModeFacade::singlePairShortestPathAStarWrite
            )
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathAStarWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            ShortestPathAStarWriteConfig::of,
            configuration -> facade.singlePairShortestPathAStarEstimate(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<PathFindingMutateResult> singlePairShortestPathDijkstraMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.of(
            runMutateAlgorithm(
                graphName,
                configuration,
                ShortestPathDijkstraMutateConfig::of,
                facade::singlePairShortestPathDijkstraMutate
            )
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathDijkstraMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            ShortestPathDijkstraMutateConfig::of,
            configuration -> facade.singlePairShortestPathDijkstraEstimate(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<PathFindingStreamResult> singlePairShortestPathDijkstraStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return runStreamAlgorithm(
            graphName,
            configuration,
            ShortestPathDijkstraStreamConfig::of,
            facade::singlePairShortestPathDijkstraStream
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathDijkstraStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            ShortestPathDijkstraStreamConfig::of,
            configuration -> facade.singlePairShortestPathDijkstraEstimate(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<StandardWriteRelationshipsResult> singlePairShortestPathDijkstraWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.of(
            runWriteAlgorithm(
                graphName,
                configuration,
                ShortestPathDijkstraWriteConfig::of,
                writeModeFacade::singlePairShortestPathDijkstraWrite
            )
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathDijkstraWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            ShortestPathDijkstraWriteConfig::of,
            configuration -> facade.singlePairShortestPathDijkstraEstimate(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<PathFindingMutateResult> singlePairShortestPathYensMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.of(
            runMutateAlgorithm(
                graphName,
                configuration,
                ShortestPathYensMutateConfig::of,
                facade::singlePairShortestPathYensMutate
            )
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathYensMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            ShortestPathYensMutateConfig::of,
            configuration -> facade.singlePairShortestPathYensEstimate(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<PathFindingStreamResult> singlePairShortestPathYensStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return runStreamAlgorithm(
            graphName,
            configuration,
            ShortestPathYensStreamConfig::of,
            facade::singlePairShortestPathYensStream
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathYensStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            ShortestPathYensStreamConfig::of,
            configuration -> facade.singlePairShortestPathYensEstimate(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<StandardWriteRelationshipsResult> singlePairShortestPathYensWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.of(
            runWriteAlgorithm(
                graphName,
                configuration,
                ShortestPathYensWriteConfig::of,
                writeModeFacade::singlePairShortestPathYensWrite
            )
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathYensWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            ShortestPathYensWriteConfig::of,
            configuration -> facade.singlePairShortestPathYensEstimate(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<PathFindingMutateResult> singleSourceShortestPathDijkstraMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.of(
            runMutateAlgorithm(
                graphName,
                configuration,
                AllShortestPathsDijkstraMutateConfig::of,
                facade::singleSourceShortestPathDijkstraMutate
            )
        );
    }

    public Stream<PathFindingStreamResult> singleSourceShortestPathDijkstraStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return runStreamAlgorithm(
            graphName,
            configuration,
            AllShortestPathsDijkstraStreamConfig::of,
            facade::singleSourceShortestPathDijkstraStream
        );
    }

    /**
     * Just a bit of reuse
     */
    private <CONFIGURATION extends AlgoBaseConfig> MemoryEstimateResult runEstimation(
        Map<String, Object> algorithmConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> configurationParser,
        Function<CONFIGURATION, MemoryEstimateResult> supplier
    ) {
        var configuration = configurationCreator.createConfiguration(
            algorithmConfiguration,
            configurationParser
        );

        return supplier.apply(configuration);
    }

    /**
     * This is kind of a template method: Dijkstra and A* both use the same code structure.
     * It is quite banal:
     * <ol>
     *     <li> configuration parsing
     *     <li> parameter marshalling
     *     <li> delegating to down stream layer to call the thing we are actually interested in
     *     <li> handle resource closure
     * </ol>
     */
    private <CONFIGURATION extends AlgoBaseConfig> Stream<PathFindingStreamResult> runStreamAlgorithm(
        String graphNameAsString,
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> configurationSupplier,
        AlgorithmHandle<CONFIGURATION, PathFindingResult, Stream<PathFindingStreamResult>> algorithm
    ) {
        var graphName = GraphName.parse(graphNameAsString);
        var configuration = configurationCreator.createConfigurationForStream(rawConfiguration, configurationSupplier);
        var resultBuilder = new PathFindingResultBuilderForStreamMode(
            nodeLookup,
            procedureReturnColumns.contains("path")
        );

        var resultStream = algorithm.compute(graphName, configuration, resultBuilder);

        // we need to do this for stream mode
        closeableResourceRegistry.register(resultStream);

        return resultStream;
    }

    /**
     * Contract this with {@link #runStreamAlgorithm}:
     * <ul>
     *     <li>Same input handling
     *     <li>Pick a different result marshaller - this is the big responsibility in this layer
     *     <li>Delegate to compute
     *     <li>No stream closing hook because we end up returning just a single element wrapped in a stream
     * </ul>
     *
     * So very much the same, but I didn't fancy trying to extract reuse today, for readability's sake
     */
    private <CONFIGURATION extends AlgoBaseConfig> PathFindingMutateResult runMutateAlgorithm(
        String graphNameAsString,
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> configurationSupplier,
        AlgorithmHandle<CONFIGURATION, PathFindingResult, PathFindingMutateResult> algorithm
    ) {
        var graphName = GraphName.parse(graphNameAsString);
        var configuration = configurationCreator.createConfiguration(rawConfiguration, configurationSupplier);

        // mutation
        var resultBuilder = new PathFindingResultBuilderForMutateMode(configuration);

        return algorithm.compute(graphName, configuration, resultBuilder);
    }

    /**
     * Writes are like mutate: the usual pre-processing, then the actual algorithm call,
     * and finally post-processing: the side effect (the writes) and result rendering.
     */
    private <CONFIGURATION extends AlgoBaseConfig> StandardWriteRelationshipsResult runWriteAlgorithm(
        String graphNameAsString,
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> configurationSupplier,
        AlgorithmHandle<CONFIGURATION, PathFindingResult, StandardWriteRelationshipsResult> algorithm
    ) {
        var graphName = GraphName.parse(graphNameAsString);
        var configuration = configurationCreator.createConfiguration(rawConfiguration, configurationSupplier);

        // write
        var resultBuilder = new PathFindingResultBuilderForWriteMode(configuration);

        return algorithm.compute(graphName, configuration, resultBuilder);
    }
}
