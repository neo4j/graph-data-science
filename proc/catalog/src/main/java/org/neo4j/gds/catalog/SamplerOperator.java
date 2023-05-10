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
package org.neo4j.gds.catalog;

import org.neo4j.gds.config.RandomWalkWithRestartsProcConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.loading.GraphStoreWithConfig;
import org.neo4j.gds.core.loading.ImmutableCatalogRequest;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.graphsampling.GraphSampleConstructor;
import org.neo4j.gds.graphsampling.RandomWalkBasedNodesSampler;
import org.neo4j.gds.graphsampling.config.RandomWalkWithRestartsConfig;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

public final class SamplerOperator {

   public static Stream<GraphSampleProc.RandomWalkSamplingResult> performSampling(
        String fromGraphName,
        String graphName,
        Map<String, Object> configuration,
        Function<CypherMapWrapper, RandomWalkWithRestartsConfig> samplerConfigProvider,
        Function<RandomWalkWithRestartsConfig, RandomWalkBasedNodesSampler> samplerAlgorithmProvider,
        ExecutionContext executionContext
    ) {
        try (var progressTimer = ProgressTimer.start()) {

            var fromGraphStore = graphStoreFromCatalog(fromGraphName, executionContext);

            var cypherMap = CypherMapWrapper.create(configuration);
            var samplerConfig = samplerConfigProvider.apply(cypherMap);

            var samplerAlgorithm = samplerAlgorithmProvider.apply(samplerConfig);
            var progressTracker = new TaskProgressTracker(
                GraphSampleConstructor.progressTask(fromGraphStore.graphStore(), samplerAlgorithm),
                executionContext.log(),
                samplerConfig.concurrency(),
                samplerConfig.jobId(),
                executionContext.taskRegistryFactory(),
                EmptyUserLogRegistryFactory.INSTANCE
            );
            var graphSampleConstructor = new GraphSampleConstructor(
                samplerConfig,
                fromGraphStore.graphStore(),
                samplerAlgorithm,
                progressTracker
            );
            var sampledGraphStore = graphSampleConstructor.compute();

            var rwrProcConfig = RandomWalkWithRestartsProcConfig.of(
                executionContext.username(),
                graphName,
                fromGraphName,
                fromGraphStore.config(),
                cypherMap
            );
            GraphStoreCatalog.set(rwrProcConfig, sampledGraphStore);

            var projectMillis = progressTimer.stop().getDuration();
            return Stream.of(new GraphSampleProc.RandomWalkSamplingResult(
                graphName,
                fromGraphName,
                sampledGraphStore.nodeCount(),
                sampledGraphStore.relationshipCount(),
                samplerAlgorithm.numberOfStartNodes(),
                projectMillis
            ));
        }
   }

    static GraphStoreWithConfig graphStoreFromCatalog(String graphName, ExecutionContext executionContext) {
        var catalogRequest = ImmutableCatalogRequest.of(
            executionContext.databaseId().databaseName(),
            executionContext.username(),
            Optional.empty(),
            executionContext.isGdsAdmin()
        );
        return GraphStoreCatalog.get(catalogRequest, graphName);
    }

}
