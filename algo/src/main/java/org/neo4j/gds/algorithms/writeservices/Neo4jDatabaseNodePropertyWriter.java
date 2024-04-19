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
package org.neo4j.gds.algorithms.writeservices;

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.config.ArrowConnectionInfo;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.loading.Capabilities;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.core.write.NodeProperty;
import org.neo4j.gds.core.write.NodePropertyExporter;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

/**
 * Extracting some common code so that it is reusable; eventually this can probably move to where it is used
 */
final class Neo4jDatabaseNodePropertyWriter {

    private Neo4jDatabaseNodePropertyWriter() {
    }

    static WriteNodePropertyResult writeNodeProperty(
        NodePropertyExporterBuilder nodePropertyExporterBuilder,
        TaskRegistryFactory taskRegistryFactory,
        Graph graph,
        GraphStore graphStore,
        NodePropertyValues nodePropertyValues,
        Concurrency writeConcurrency,
        String writeProperty,
        String procedureName,
        Optional<ArrowConnectionInfo> arrowConnectionInfo,
        Optional<ResultStore> resultStore,
        TerminationFlag terminationFlag,
        Log log
    ) {
        var nodeProperties = List.of(ImmutableNodeProperty.of(writeProperty, nodePropertyValues));

        var writeMillis = new AtomicLong();
        var propertiesWritten = new MutableLong();
        try (ProgressTimer ignored = ProgressTimer.start(writeMillis::set)) {
            var progressTracker = createProgressTracker(
                taskRegistryFactory,
                graph.nodeCount(),
                writeConcurrency,
                procedureName,
                log
            );
            var writeMode = graphStore.capabilities().writeMode();
            var nodePropertySchema = graph.schema().nodeSchema().unionProperties();


            validatePropertiesCanBeWritten(
                writeMode,
                nodePropertySchema,
                nodeProperties,
                arrowConnectionInfo.isPresent()
            );

            var exporter = nodePropertyExporterBuilder
                .withIdMap(graph)
                .withTerminationFlag(terminationFlag)
                .withProgressTracker(progressTracker)
                .withArrowConnectionInfo(
                    arrowConnectionInfo,
                    graphStore.databaseInfo().remoteDatabaseId().map(DatabaseId::databaseName)
                )
                .withResultStore(resultStore)
                .parallel(DefaultPool.INSTANCE, writeConcurrency)
                .build();

            try {
                exporter.write(nodeProperties);
                propertiesWritten.setValue(exporter.propertiesWritten());
            } finally {
                progressTracker.release();
            }

        }
        return new WriteNodePropertyResult(propertiesWritten.getValue(), writeMillis.get());
    }

    private static ProgressTracker createProgressTracker(
        TaskRegistryFactory taskRegistryFactory,
        long taskVolume,
        Concurrency writeConcurrency,
        String name,
        Log log
    ) {
        return new TaskProgressTracker(
            NodePropertyExporter.baseTask(name, taskVolume),
            (org.neo4j.logging.Log) log.getNeo4jLog(),
            writeConcurrency.value(),
            taskRegistryFactory
        );
    }

    private static void validatePropertiesCanBeWritten(
        Capabilities.WriteMode writeMode,
        Map<String, PropertySchema> propertySchemas,
        Collection<NodeProperty> nodeProperties,
        boolean hasArrowConnectionInfo
    ) {
        if (writeMode == Capabilities.WriteMode.REMOTE && !hasArrowConnectionInfo) {
            throw new IllegalArgumentException("Missing arrow connection information");
        }
        if (writeMode == Capabilities.WriteMode.LOCAL && hasArrowConnectionInfo) {
            throw new IllegalArgumentException(
                "Arrow connection info was given although the write operation is targeting a local database"
            );
        }

        var expectedPropertyState = expectedPropertyStateForWriteMode(writeMode);

        var unexpectedProperties = nodeProperties
            .stream()
            .filter(nodeProperty -> {
                var propertySchema = propertySchemas.get(nodeProperty.propertyKey());
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
                    nodeProperty.propertyKey(),
                    propertySchemas.get(nodeProperty.propertyKey()).state()
                )
            )
            .collect(Collectors.toList());

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

    private static Predicate<PropertyState> expectedPropertyStateForWriteMode(Capabilities.WriteMode writeMode) {
        switch (writeMode) {
            case LOCAL:
                // We need to allow persistent and transient as for example algorithms that support seeding will reuse a
                // mutated (transient) property to write back properties that are in fact backed by a database
                return state -> state == PropertyState.PERSISTENT || state == PropertyState.TRANSIENT;
            case REMOTE:
                // We allow transient properties for the same reason as above
                return state -> state == PropertyState.REMOTE || state == PropertyState.TRANSIENT;
            default:
                throw new IllegalStateException(
                    formatWithLocale(
                        "Graph with write mode `%s` cannot write back to a database",
                        writeMode
                    )
                );
        }
    }

}
