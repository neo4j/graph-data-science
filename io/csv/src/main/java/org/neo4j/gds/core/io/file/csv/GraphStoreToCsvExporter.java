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
package org.neo4j.gds.core.io.file.csv;

import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.MutableNodeSchema;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.io.GraphStoreExporterParameters;
import org.neo4j.gds.core.io.NeoNodeProperties;
import org.neo4j.gds.core.io.NodeLabelMapping;
import org.neo4j.gds.core.io.file.GraphStoreToFileExporter;
import org.neo4j.gds.core.io.file.GraphStoreToFileExporterConfig;
import org.neo4j.gds.core.io.file.GraphStoreToFileExporterParameters;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

public final class GraphStoreToCsvExporter {

    @TestOnly
    public static GraphStoreToFileExporter create(
        GraphStore graphStore,
        GraphStoreToFileExporterConfig config,
        Path exportPath
    ) {
        var toFileExporterParameters = GraphStoreToFileExporterParameters.create(config.exportName(), config.username(), config.includeMetaData(), config.useLabelMapping());
        var commonExporterParameters = GraphStoreExporterParameters.create(config.defaultRelationshipType(), config.batchSize(), config.writeConcurrency());
        return create(graphStore,
            toFileExporterParameters,
            commonExporterParameters,
            exportPath,
            Optional.empty(),
            TaskRegistryFactory.empty(),
            NullLog.getInstance(),
            DefaultPool.INSTANCE
        );
    }

    public static GraphStoreToFileExporter create(
        GraphStore graphStore,
        GraphStoreToFileExporterParameters toFileExporterParameters,
        GraphStoreExporterParameters commonExporterParameters,
        Path exportPath,
        Optional<NeoNodeProperties> neoNodeProperties,
        TaskRegistryFactory taskRegistryFactory,
        Log log,
        ExecutorService executorService
    ) {
        Set<String> headerFiles = ConcurrentHashMap.newKeySet();

        var nodeSchema = graphStore.schema().nodeSchema();
        var relationshipSchema = graphStore.schema().relationshipSchema();

        var neoNodeSchema = MutableNodeSchema.empty();

        // Add additional properties to each label present in the graph store.
        neoNodeProperties.ifPresent(additionalProps -> additionalProps
            .neoNodeProperties()
            .forEach((key, ignore) -> nodeSchema
                .availableLabels()
                .forEach(label -> neoNodeSchema.getOrCreateLabel(label).addProperty(key, ValueType.STRING))
            ));

        Optional<NodeLabelMapping> nodeLabelMapping = toFileExporterParameters.useLabelMapping()
            ? Optional.of(new NodeLabelMapping(graphStore.nodeLabels()))
            : Optional.empty();

        return new GraphStoreToFileExporter(
            graphStore,
            toFileExporterParameters,
            commonExporterParameters,
            neoNodeProperties,
            nodeLabelMapping,
            () -> new UserInfoVisitor(exportPath),
            () -> new CsvGraphInfoVisitor(exportPath),
            () -> new CsvNodeSchemaVisitor(exportPath),
            () -> new CsvNodeLabelMappingVisitor(exportPath),
            () -> new CsvRelationshipSchemaVisitor(exportPath),
            () -> new CsvGraphPropertySchemaVisitor(exportPath),
            () -> new CsvGraphCapabilitiesWriter(exportPath),
            (index) -> new CsvNodeVisitor(
                exportPath,
                nodeSchema.union(neoNodeSchema),
                headerFiles,
                index,
                nodeLabelMapping
            ),
            (index) -> new CsvRelationshipVisitor(exportPath, relationshipSchema, headerFiles, index),
            (index) -> new CsvGraphPropertyVisitor(
                exportPath,
                graphStore.schema().graphProperties(),
                headerFiles,
                index
            ),
            taskRegistryFactory,
            log,
            "Csv",
            executorService
        );
    }

    private GraphStoreToCsvExporter() {}
}
