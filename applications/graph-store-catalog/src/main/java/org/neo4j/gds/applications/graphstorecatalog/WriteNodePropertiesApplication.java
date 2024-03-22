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

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.core.write.NodePropertyExporter;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class WriteNodePropertiesApplication {
    private final Log log;

    public WriteNodePropertiesApplication(Log log) {
        this.log = log;
    }

    NodePropertiesWriteResult write(
        GraphStore graphStore,
        NodePropertyExporterBuilder nodePropertyExporterBuilder,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        UserLogRegistryFactory userLogRegistryFactory,
        GraphName graphName,
        GraphWriteNodePropertiesConfig configuration
    ) {
        var validNodeLabels = configuration.validNodeLabels(graphStore);
        var task = Tasks.iterativeFixed(
            "Graph :: NodeProperties :: Write",
            () -> List.of(
                NodePropertyExporter.innerTask("Label", Task.UNKNOWN_VOLUME)
            ),
            validNodeLabels.size()
        );
        var progressTracker = new TaskProgressTracker(
            task,
            (org.neo4j.logging.Log) log.getNeo4jLog(),
            configuration.writeConcurrency(),
            new JobId(),
            taskRegistryFactory,
            userLogRegistryFactory
        );

        var allNodeProperties = configuration
            .nodeProperties()
            .stream()
            .map(UserInputWriteProperties.PropertySpec::writeProperty)
            .collect(Collectors.toList());

        // writing
        NodePropertiesWriteResult.Builder builder = new NodePropertiesWriteResult.Builder(
            graphName.getValue(),
            allNodeProperties
        );
        try (ProgressTimer ignored = ProgressTimer.start(builder::withWriteMillis)) {
            try {
                var propertiesWritten = writeNodeProperties(
                    graphStore,
                    configuration,
                    validNodeLabels,
                    nodePropertyExporterBuilder,
                    terminationFlag,
                    progressTracker
                );

                builder.withPropertiesWritten(propertiesWritten);
            } catch (RuntimeException e) {
                log.warn("Node property writing failed", e);
                throw e;
            }
        }

        return builder.build();
    }

    private static long writeNodeProperties(
        GraphStore graphStore,
        GraphWriteNodePropertiesConfig config,
        Iterable<NodeLabel> validNodeLabels,
        NodePropertyExporterBuilder nodePropertyExporterBuilder,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker
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

                var resultStore = config.resolveResultStore(graphStore.resultStore());
                var exporter = nodePropertyExporterBuilder
                    .withIdMap(subGraph)
                    .withTerminationFlag(terminationFlag)
                    .parallel(DefaultPool.INSTANCE, config.writeConcurrency())
                    .withProgressTracker(progressTracker)
                    .withArrowConnectionInfo(
                        config.arrowConnectionInfo(),
                        graphStore.databaseInfo().remoteDatabaseId().map(DatabaseId::databaseName)
                    )
                    .withResultStore(resultStore)
                    .build();

                var writeNodeProperties = config.nodeProperties()
                    .stream()
                    .map(
                        nodePropertyKey -> ImmutableNodeProperty.of(
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
}
