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
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.executor.ProcPreconditions;
import org.neo4j.gds.graphsampling.GraphSampleConstructor;
import org.neo4j.gds.graphsampling.config.RandomWalkWithRestartsConfig;
import org.neo4j.gds.graphsampling.samplers.rw.rwr.RandomWalkWithRestarts;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class GraphSampleProc extends CatalogProc {

    private static final String RWR_DESCRIPTION = "Constructs a random subgraph based on random walks with restarts";

    @Procedure(name = "gds.alpha.graph.sample.rwr", mode = READ)
    @Description(RWR_DESCRIPTION)
    public Stream<RandomWalkWithRestartsSampleResult> sampleRandomWalkWithRestarts(
        @Name(value = "graphName") String graphName,
        @Name(value = "fromGraphName") String fromGraphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ProcPreconditions.check();
        validateGraphName(username(), graphName);

        try (var progressTimer = ProgressTimer.start()) {
            var fromGraphStore = graphStoreFromCatalog(fromGraphName);

            var cypherMap = CypherMapWrapper.create(configuration);
            var rwrConfig = RandomWalkWithRestartsConfig.of(cypherMap);

            var randomWalkWithRestarts = new RandomWalkWithRestarts(rwrConfig);
            var progressTracker = new TaskProgressTracker(
                GraphSampleConstructor.progressTask(fromGraphStore.graphStore(), randomWalkWithRestarts),
                executionContext().log(),
                rwrConfig.concurrency(),
                rwrConfig.jobId(),
                executionContext().taskRegistryFactory(),
                EmptyUserLogRegistryFactory.INSTANCE
            );
            var graphSampleConstructor = new GraphSampleConstructor(
                rwrConfig,
                fromGraphStore.graphStore(),
                randomWalkWithRestarts,
                progressTracker
            );
            var sampledGraphStore = graphSampleConstructor.compute();

            var rwrProcConfig = RandomWalkWithRestartsProcConfig.of(
                username(),
                graphName,
                fromGraphName,
                fromGraphStore.config(),
                cypherMap
            );

            GraphStoreCatalog.set(rwrProcConfig, sampledGraphStore);

            var projectMillis = progressTimer.stop().getDuration();
            return Stream.of(new RandomWalkWithRestartsSampleResult(
                graphName,
                fromGraphName,
                sampledGraphStore.nodeCount(),
                sampledGraphStore.relationshipCount(),
                randomWalkWithRestarts.startNodesUsed().size(),
                projectMillis
            ));
        }
    }


    public static class RandomWalkWithRestartsSampleResult extends GraphProjectProc.GraphProjectResult {
        public final String fromGraphName;
        public final long startNodeCount;

        RandomWalkWithRestartsSampleResult(
            String graphName,
            String fromGraphName,
            long nodeCount,
            long relationshipCount,
            long startNodeCount,
            long projectMillis
        ) {
            super(graphName, nodeCount, relationshipCount, projectMillis);
            this.fromGraphName = fromGraphName;
            this.startNodeCount = startNodeCount;
        }
    }

}
