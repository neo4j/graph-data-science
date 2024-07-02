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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.core.write.NodePropertyExporter;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.termination.TerminationFlag;

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
        ResultStore resultStore,
        NodePropertyExporterBuilder nodePropertyExporterBuilder,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        UserLogRegistryFactory userLogRegistryFactory,
        GraphName graphName,
        GraphWriteNodePropertiesConfig configuration
    ) {
        var validNodeLabels = configuration.validNodeLabels(graphStore);
        var subGraph = graphStore.getGraph(
            validNodeLabels,
            graphStore.relationshipTypes(),
            Optional.empty()
        );


        var task = Tasks.iterativeFixed(
            "Graph :: NodeProperties :: Write",
            () -> List.of(
                NodePropertyExporter.innerTask("Label", subGraph.nodeCount())
            ),
            validNodeLabels.size()
        );
        var progressTracker = new TaskProgressTracker(
            task,
            log,
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
                    resultStore,
                    configuration,
                    subGraph,
                    nodePropertyExporterBuilder,
                    terminationFlag,
                    progressTracker
                );

                builder
                    .withPropertiesWritten(propertiesWritten)
                    .withConfig(configuration.toMap());
            } catch (RuntimeException e) {
                log.warn("Node property writing failed", e);
                throw e;
            }
        }

        return builder.build();
    }

    private static long writeNodeProperties(
        ResultStore resultStore,
        GraphWriteNodePropertiesConfig config,
        Graph subGraph,
        NodePropertyExporterBuilder nodePropertyExporterBuilder,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker
    ) {
        progressTracker.beginSubTask();
        try {
            var exporter = nodePropertyExporterBuilder
                .withIdMap(subGraph)
                .withTerminationFlag(terminationFlag)
                .parallel(DefaultPool.INSTANCE, config.writeConcurrency())
                .withProgressTracker(progressTracker)
                .withResultStore(config.resolveResultStore(resultStore))
                .withJobId(config.jobId())
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

            return exporter.propertiesWritten();

        } finally {
            progressTracker.endSubTask();
        }
    }
}
