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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.BaseConfig;
import org.neo4j.gds.config.GraphWriteNodePropertiesConfig;
import org.neo4j.gds.core.CypherMapAccess;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.loading.GraphStoreWithConfig;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.core.write.NodePropertyExporter;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GraphStoreFromCatalogLoader;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;

public final class NodePropertiesWriter {

    private NodePropertiesWriter() {}

    static Stream<NodePropertiesWriteResult> write(
        String graphName,
        Object nodeProperties,
        Object nodeLabels,
        Map<String, Object> configuration,
        ExecutionContext executionContext,
        Optional<String> deprecationWarning
    ) {
        validateGraphName(graphName);

        // input
        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        GraphWriteNodePropertiesConfig config = GraphWriteNodePropertiesConfig.of(
            graphName,
            nodeProperties,
            nodeLabels,
            cypherConfig
        );
        // validation
        validateConfig(cypherConfig, config);
        GraphStore graphStore = graphStoreFromCatalog(
            graphName,
            config,
            executionContext.username(),
            executionContext.databaseId(),
            executionContext.isGdsAdmin()
        ).graphStore();
        config.validate(graphStore);

        var validNodeLabels = config.validNodeLabels(graphStore);
        var task = Tasks.iterativeFixed(
            "Graph :: NodeProperties :: Write",
            () -> List.of(
                NodePropertyExporter.innerTask("Label", Task.UNKNOWN_VOLUME)
            ),
            validNodeLabels.size()
        );
        var progressTracker = new TaskProgressTracker(
            task,
            executionContext.log(),
            config.writeConcurrency(),
            new JobId(),
            executionContext.taskRegistryFactory(),
            executionContext.userLogRegistryFactory()
        );

        deprecationWarning.ifPresent(progressTracker::logWarning);

        var allNodeProperties = config
            .nodeProperties()
            .stream()
            .map(UserInputWriteProperties.PropertySpec::writeProperty)
            .collect(Collectors.toList());

        // writing
        NodePropertiesWriteResult.Builder builder = new NodePropertiesWriteResult.Builder(graphName, allNodeProperties);
        try (ProgressTimer ignored = ProgressTimer.start(builder::withWriteMillis)) {
            long propertiesWritten = runWithExceptionLogging(
                "Node property writing failed",
                executionContext.log(),
                () -> writeNodeProperties(graphStore, config, validNodeLabels, executionContext, progressTracker)
            );
            builder.withPropertiesWritten(propertiesWritten);
        }
        // result
        return Stream.of(builder.build());
    }


    private static long writeNodeProperties(
        GraphStore graphStore,
        GraphWriteNodePropertiesConfig config,
        Iterable<NodeLabel> validNodeLabels,
        ExecutionContext executionContext, ProgressTracker progressTracker
    ) {
        var propertiesWritten = 0L;

        progressTracker.beginSubTask();
        try {
            for (var label : validNodeLabels) {
                var subGraph = graphStore.getGraph(
                    Collections.singletonList(label),
                    graphStore.relationshipTypes(),
                    Optional.empty()
                );

                var nodePropertyExporterBuilder = executionContext.nodePropertyExporterBuilder();

                assert nodePropertyExporterBuilder != null : "NodePropertyExporterBuilder must be set to the ExecutionContext!";

                var exporter = nodePropertyExporterBuilder
                    .withIdMap(subGraph)
                    .withTerminationFlag(TerminationFlag.wrap(executionContext.terminationMonitor()))
                    .parallel(Pools.DEFAULT, config.writeConcurrency())
                    .withProgressTracker(progressTracker)
                    .withArrowConnectionInfo(config.arrowConnectionInfo())
                    .build();

                var writeNodeProperties =
                    config.nodeProperties().stream()
                        .map(nodePropertyKey ->
                            ImmutableNodeProperty.of(
                                nodePropertyKey.writeProperty(),
                                subGraph.nodeProperties(nodePropertyKey.nodeProperty())
                            )
                        )
                        .collect(Collectors.toList());

                exporter.write(writeNodeProperties);

                propertiesWritten += exporter.propertiesWritten();
            }
        } finally {
            progressTracker.endSubTask();
        }

        return propertiesWritten;
    }

    private static @NotNull String validateGraphName(@Nullable String graphName) {
        return CypherMapAccess.failOnBlank("graphName", graphName);
    }

    private static void validateConfig(CypherMapAccess cypherConfig, BaseConfig config) {
        cypherConfig.requireOnlyKeysFrom(config.configKeys());
    }

    private static GraphStoreWithConfig graphStoreFromCatalog(
        String graphName, BaseConfig config, String username,
        DatabaseId databaseId, boolean gdsAdmin
    ) {
        return GraphStoreFromCatalogLoader.graphStoreFromCatalog(
            graphName,
            config,
            username,
            databaseId,
            gdsAdmin
        );
    }
}
