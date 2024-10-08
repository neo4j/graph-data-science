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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.config.WriteConfig;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.loading.Capabilities;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.write.NodeProperty;
import org.neo4j.gds.core.write.NodePropertyExporter;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

/**
 * Common bits of node property writes, squirrelled away in one place
 */
public class NodePropertyWriter {
    private final Log log;

    private final NodePropertyExporterBuilder nodePropertyExporterBuilder;
    private final TaskRegistryFactory taskRegistryFactory;
    private final TerminationFlag terminationFlag;

    public NodePropertyWriter(
        Log log,
        NodePropertyExporterBuilder nodePropertyExporterBuilder,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag
    ) {
        this.log = log;
        this.nodePropertyExporterBuilder = nodePropertyExporterBuilder;
        this.taskRegistryFactory = taskRegistryFactory;
        this.terminationFlag = terminationFlag;
    }

    public NodePropertiesWritten writeNodeProperties(
        Graph graph,
        GraphStore graphStore,
        Optional<ResultStore> resultStore,
        Collection<NodeProperty> nodeProperties,
        JobId jobId,
        Label label,
        WriteConfig writeConfig
    ) {
        preFlightCheck(
            graphStore.capabilities().writeMode(),
            graph.schema().nodeSchema().unionProperties(),
            nodeProperties
        );

        var progressTracker = createProgressTracker(graph.nodeCount(), writeConfig.writeConcurrency(), label);

        var nodePropertyExporter = nodePropertyExporterBuilder
            .parallel(DefaultPool.INSTANCE, writeConfig.writeConcurrency())
            .withIdMap(graph)
            .withJobId(jobId)
            .withProgressTracker(progressTracker)
            .withResultStore(resultStore)
            .withTerminationFlag(terminationFlag)
            .build();

        try {
            return writeNodeProperties(nodePropertyExporter, nodeProperties);
        } finally {
            progressTracker.release();
        }
    }

    private ProgressTracker createProgressTracker(
        long taskVolume,
        Concurrency writeConcurrency,
        Label label
    ) {
        var task = NodePropertyExporter.baseTask(label.asString(), taskVolume);

        return new TaskProgressTracker(
            task,
            log,
            writeConcurrency,
            taskRegistryFactory
        );
    }

    private Predicate<PropertyState> expectedPropertyStateForWriteMode(Capabilities.WriteMode writeMode) {
        return switch (writeMode) {
            case LOCAL ->
                // We need to allow persistent and transient as for example algorithms that support seeding will reuse a
                // mutated (transient) property to write back properties that are in fact backed by a database
                state -> state == PropertyState.PERSISTENT || state == PropertyState.TRANSIENT;
            case REMOTE ->
                // We allow transient properties for the same reason as above
                state -> state == PropertyState.REMOTE || state == PropertyState.TRANSIENT;
            default -> throw new IllegalStateException(
                formatWithLocale(
                    "Graph with write mode `%s` cannot write back to a database",
                    writeMode
                )
            );
        };
    }

    private void preFlightCheck(
        Capabilities.WriteMode writeMode,
        Map<String, PropertySchema> propertySchemas,
        Collection<NodeProperty> nodeProperties
    ) {
        if (writeMode == Capabilities.WriteMode.REMOTE) throw new IllegalArgumentException(
            "Missing arrow connection information");

        var expectedPropertyState = expectedPropertyStateForWriteMode(writeMode);

        var unexpectedProperties = nodeProperties.stream()
            .filter(nodeProperty -> {
                var propertySchema = propertySchemas.get(nodeProperty.key());
                if (propertySchema == null) {
                    // We are executing an algorithm write mode and the property we are writing is
                    // not in the GraphStore, therefore we do not perform any more checks
                    return false;
                }
                var propertyState = propertySchema.state();
                return !expectedPropertyState.test(propertyState);
            })
            .map(
                nodeProperty -> formatWithLocale(
                    "NodeProperty{propertyKey=%s, propertyState=%s}",
                    nodeProperty.key(),
                    propertySchemas.get(nodeProperty.key()).state()
                )
            )
            .toList();

        if (!unexpectedProperties.isEmpty()) {
            throw new IllegalStateException(
                formatWithLocale(
                    "Expected all properties to be of state `%s` but some properties differ: %s",
                    expectedPropertyState,
                    unexpectedProperties
                )
            );
        }
    }

    private NodePropertiesWritten writeNodeProperties(
        NodePropertyExporter nodePropertyExporter,
        Collection<NodeProperty> nodeProperties
    ) {
        nodePropertyExporter.write(nodeProperties);

        return new NodePropertiesWritten(nodePropertyExporter.propertiesWritten());
    }
}
