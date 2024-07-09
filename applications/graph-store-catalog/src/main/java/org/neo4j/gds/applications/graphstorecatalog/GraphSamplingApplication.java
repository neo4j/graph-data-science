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
package org.neo4j.gds.applications.graphstorecatalog;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.User;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.graphsampling.GraphSampleConstructor;
import org.neo4j.gds.graphsampling.RandomWalkSamplerType;
import org.neo4j.gds.logging.Log;

import java.util.Map;

public final class GraphSamplingApplication {
    private final Log log;
    private final GraphStoreCatalogService graphStoreCatalogService;

    public GraphSamplingApplication(Log log, GraphStoreCatalogService graphStoreCatalogService) {
        this.log = log;
        this.graphStoreCatalogService = graphStoreCatalogService;
    }

    RandomWalkSamplingResult sample(
        User user,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        GraphStore graphStore,
        GraphProjectConfig graphProjectConfig,
        GraphName originGraphName,
        GraphName graphName,
        Map<String, Object> configuration,
        RandomWalkSamplerType samplerType
    ) {
        try (var progressTimer = ProgressTimer.start()) {
            var cypherMap = CypherMapWrapper.create(configuration);
            var samplerProvider = SamplerProvider.of(samplerType, cypherMap);
            var samplerConfig = samplerProvider.config();

            var samplerAlgorithm = samplerProvider.algorithm();
            var progressTracker = new TaskProgressTracker(
                GraphSampleConstructor.progressTask(graphStore, samplerAlgorithm),
                log,
                samplerConfig.concurrency(),
                samplerConfig.jobId(),
                taskRegistryFactory,
                userLogRegistryFactory
            );
            var graphSampleConstructor = new GraphSampleConstructor(
                samplerConfig,
                graphStore,
                samplerAlgorithm,
                progressTracker
            );
            var sampledGraphStore = graphSampleConstructor.compute();

            var rwrProcConfig = RandomWalkWithRestartsConfiguration.of(
                user.getUsername(),
                graphName.getValue(),
                originGraphName.getValue(),
                graphProjectConfig,
                cypherMap
            );

            graphStoreCatalogService.set(rwrProcConfig, sampledGraphStore);

            var projectMillis = progressTimer.stop().getDuration();

            return new RandomWalkSamplingResult(
                graphName.getValue(),
                originGraphName.getValue(),
                sampledGraphStore.nodeCount(),
                sampledGraphStore.relationshipCount(),
                samplerAlgorithm.startNodesCount(),
                projectMillis
            );
        }
    }
}
