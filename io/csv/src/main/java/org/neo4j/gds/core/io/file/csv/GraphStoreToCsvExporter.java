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

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.RequestCorrelationId;
import org.neo4j.gds.core.io.IdentifierMapper;
import org.neo4j.gds.core.io.NeoNodeProperties;
import org.neo4j.gds.core.io.file.GraphStoreToFileExporter;
import org.neo4j.gds.core.io.file.GraphStoreToFileExporterParameters;
import org.neo4j.gds.progress.registration.TaskRegistryFactory;
import org.neo4j.gds.progress.logging.LoggerForProgressTracking;
import org.neo4j.gds.logging.Log;

import java.nio.file.Path;
import java.util.Comparator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

public final class GraphStoreToCsvExporter {

    public static GraphStoreToFileExporter create(
        Log log,
        LoggerForProgressTracking loggerForProgressTracking,
        GraphStore graphStore,
        GraphStoreToFileExporterParameters parameters,
        Path exportPath,
        Optional<NeoNodeProperties> neoNodeProperties,
        RequestCorrelationId requestCorrelationId,
        JobId jobId,
        TaskRegistryFactory taskRegistryFactory,
        ExecutorService executorService
    ) {
        Set<String> headerFiles = ConcurrentHashMap.newKeySet();

        var nodeSchema = graphStore.schema().nodeSchema();
        var relationshipSchema = graphStore.schema().relationshipSchema();

        var neoNodeSchemaBuilder = NodeSchema.builder().addSchema(nodeSchema);

        // Add additional properties to each label present in the graph store.
        neoNodeProperties.ifPresent(additionalProps -> additionalProps
            .neoNodeProperties()
            .forEach((key, ignore) -> nodeSchema
                .availableLabels()
                .forEach(label -> neoNodeSchemaBuilder.addProperty(label.name(), key, ValueType.STRING))
            ));

        var labelMapperBuilder = IdentifierMapper.<NodeLabel>builder("label");
        // We sort the node labels to get the rows in predictable order
        var sortedNodeLabels = graphStore.nodeLabels().stream()
            .sorted(Comparator.comparing(NodeLabel::name))
            .toList();
        for (var nodeLabel : sortedNodeLabels) {
            labelMapperBuilder.getOrCreateIdentifierFor(nodeLabel);
        }
        var labelMapper = labelMapperBuilder.build();
        var relationshipTypeMapperBuilder = IdentifierMapper.<RelationshipType>builder("type");
        for (var relationshipType : graphStore.relationshipTypes()) {
            relationshipTypeMapperBuilder.getOrCreateIdentifierFor(relationshipType);
        }
        var relationshipTypeMapper = relationshipTypeMapperBuilder.build();

        return new GraphStoreToFileExporter(
            log,
            loggerForProgressTracking,
            graphStore,
            parameters,
            neoNodeProperties,
            labelMapper,
            relationshipTypeMapper,
            () -> new UserInfoVisitor(exportPath),
            () -> new CsvGraphInfoVisitor(exportPath),
            () -> new CsvNodeSchemaVisitor(exportPath),
            () -> new CsvNodeLabelMappingVisitor(exportPath),
            () -> new CsvRelationshipTypeMappingVisitor(exportPath),
            () -> new CsvRelationshipSchemaVisitor(exportPath),
            () -> new CsvGraphCapabilitiesWriter(exportPath),
            (index) -> new CsvNodeVisitor(
                exportPath,
                neoNodeSchemaBuilder.build(),
                headerFiles,
                index,
                labelMapper
            ),
            (index) -> new CsvRelationshipVisitor(exportPath, relationshipSchema, headerFiles, index, relationshipTypeMapper),
            requestCorrelationId,
            jobId,
            taskRegistryFactory,
            "Csv",
            executorService
        );
    }

    private GraphStoreToCsvExporter() {}
}
