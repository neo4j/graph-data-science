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
package org.neo4j.gds.core.io.db;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.schema.NodePropertySchema;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.core.io.GraphStoreExporter;
import org.neo4j.gds.core.io.GraphStoreInput;
import org.neo4j.gds.core.io.IdentifierMapper;
import org.neo4j.gds.core.io.NeoNodeProperties;
import org.neo4j.gds.progress.tracking.ProgressTracker;
import org.neo4j.gds.logging.Log;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.values.storable.Values;
import org.neo4j.values.storable.VectorValue;

import java.util.Optional;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class GraphStoreToDatabaseExporter extends GraphStoreExporter {

    private final GdsParallelBatchImporter parallelBatchImporter;
    private final String databaseFormat;

    public static GraphStoreToDatabaseExporter of(
        GraphStore graphStore,
        GraphDatabaseService databaseService,
        GraphStoreToDatabaseExporterParameters parameters,
        Log log,
        ProgressTracker progressTracker
    ) {
        return of(graphStore, databaseService, parameters, Optional.empty(), log, progressTracker);
    }

    public static GraphStoreToDatabaseExporter of(
        GraphStore graphStore,
        GraphDatabaseService databaseService,
        GraphStoreToDatabaseExporterParameters parameters,
        Optional<NeoNodeProperties> neoNodeProperties,
        Log log,
        ProgressTracker progressTracker
    ) {
        var pbiConfig = new GdsParallelBatchImporter.Config(
            parameters.databaseName(),
            parameters.writeConcurrency().value(),
            parameters.batchSize(),
            parameters.defaultRelationshipType(),
            parameters.enableDebugLog(),
            parameters.databaseFormat(),
            false,
            false,
            false
        );

        var executionMonitor = new ProgressTrackerExecutionMonitor(graphStore, progressTracker);

        var parallelBatchImporter = GdsParallelBatchImporter.fromDb(databaseService, pbiConfig, log, executionMonitor);

        return new GraphStoreToDatabaseExporter(
            graphStore,
            parallelBatchImporter,
            parameters,
            neoNodeProperties
        );
    }

    private GraphStoreToDatabaseExporter(
        GraphStore graphStore,
        GdsParallelBatchImporter parallelBatchImporter,
        GraphStoreToDatabaseExporterParameters parameters,
        Optional<NeoNodeProperties> neoNodeProperties
    ) {
        super(
            graphStore,
            neoNodeProperties,
            IdentifierMapper.biject(NodeLabel::name, NodeLabel::of),
            IdentifierMapper.biject(RelationshipType::name, RelationshipType::of),
            parameters.defaultRelationshipType(),
            parameters.writeConcurrency(),
            parameters.batchSize()
        );
        this.parallelBatchImporter = parallelBatchImporter;
        this.databaseFormat = parameters.databaseFormat();
    }

    @Override
    public void export(GraphStoreInput graphStoreInput) {
        validateVectorProperties(graphStoreInput.metaDataStore().nodeSchema());
        parallelBatchImporter.writeDatabase(graphStoreInput.toInput(), false);
    }

    @Override
    protected @Nullable Object nodePropertyValue(NodePropertyValues values, long nodeId) {
        var value = values.getObject(nodeId);
        if (value == null) {
            return null;
        }
        return switch (values.valueType()) {
            case FLOAT_VECTOR -> Values.float32Vector((float[]) value);
            case DOUBLE_VECTOR -> Values.float64Vector((double[]) value);
            default -> value;
        };
    }

    private void validateVectorProperties(NodeSchema nodeSchema) {
        var vectorProperties = nodeSchema.allProperties().stream()
            .filter(propertySchema -> propertySchema.valueType().isVector())
            .toList();

        if (vectorProperties.isEmpty()) {
            return;
        }

        if (!parallelBatchImporter.supportsVectorData()) {
            throw new IllegalArgumentException(formatWithLocale(
                "Exporting a node property as a vector requires a database format that supports " +
                    "vector properties, but the format `%s` does not. Affected properties: %s.",
                databaseFormat,
                vectorProperties.stream().map(NodePropertySchema::key).distinct().toList()
            ));
        }

        for (NodePropertySchema vectorProperty : vectorProperties) {
            var dimension = vectorProperty.dimension().orElseThrow();
            if (dimension < VectorValue.MIN_VECTOR_DIMENSIONS || dimension > VectorValue.MAX_VECTOR_DIMENSIONS) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Exporting the node property `%s` as a vector requires between %d and %d dimensions, but it has %d.",
                    vectorProperty.key(),
                    VectorValue.MIN_VECTOR_DIMENSIONS,
                    VectorValue.MAX_VECTOR_DIMENSIONS,
                    dimension
                ));
            }
        }
    }

    @Override
    protected IdMappingType idMappingType() {
        return IdMappingType.MAPPED;
    }
}
