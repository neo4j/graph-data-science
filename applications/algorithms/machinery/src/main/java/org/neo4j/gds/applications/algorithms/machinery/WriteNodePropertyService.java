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

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.api.nodes.NodeTranslator;
import org.neo4j.gds.api.properties.nodes.NodePropertyRecord;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.config.WriteConfig;
import org.neo4j.gds.config.WritePropertyConfig;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.logging.Log;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public class WriteNodePropertyService {
    private final Log log;
    private final RequestScopedDependencies requestScopedDependencies;
    private final WriteContext writeContext;

    public WriteNodePropertyService(Log log, RequestScopedDependencies requestScopedDependencies, WriteContext writeContext) {
        this.log = log;
        this.requestScopedDependencies = requestScopedDependencies;
        this.writeContext = writeContext;
    }

    public NodePropertiesWritten perform(
        NodeTranslator nodeTranslator,
        Set<NodeLabel> nodeLabels,
        GraphStore graphStore,
        ResultStore resultStore,
        WritePropertyConfig writePropertyConfiguration,
        Label label,
        JobId jobId,
        NodePropertyValues nodePropertyValues
    ) {

        var nodePropertyRecord = NodePropertyRecord.of(writePropertyConfiguration.writeProperty(), nodePropertyValues);
        return perform(
            nodeTranslator,
            nodeLabels,
            graphStore,
            resultStore,
            writePropertyConfiguration,
            label,
            jobId,
            List.of(nodePropertyRecord)
        );
    }

    public NodePropertiesWritten perform(
        NodeTranslator nodeTranslator,
        Set<NodeLabel> nodeLabels,
        GraphStore graphStore,
        ResultStore resultStore,
        WriteConfig writeConfiguration,
        Label label,
        JobId jobId,
        List<NodePropertyRecord> nodeProperties
    ) {
        return Neo4jDatabaseNodePropertyWriter.writeNodeProperties(
            requestScopedDependencies.correlationId(),
            writeContext.nodePropertyExporterBuilder(),
            requestScopedDependencies.taskRegistryFactory(),
            nodeTranslator,
            nodeLabels,
            graphStore,
            writeConfiguration.writeConcurrency(),
            nodeProperties,
            label.asString(),
            writeConfiguration.resolveResultStore(resultStore),
            jobId,
            requestScopedDependencies.terminationFlag(),
            log
        );
    }

    public NodePropertiesWritten perform(
        NodeTranslator nodeTranslator,
        Set<NodeLabel> nodeLabels,
        GraphStore graphStore,
        Optional<ResultStore> resolvedResultStore,
        Concurrency writeConcurrency,
        Label label,
        JobId jobId,
        String writeProperty,
        NodePropertyValues nodePropertyValues
    ) {

        var nodePropertyRecord = NodePropertyRecord.of(writeProperty, nodePropertyValues);

        return perform(
                nodeTranslator,
                nodeLabels,
                graphStore,
                resolvedResultStore,
                writeConcurrency,
                label,
                jobId,
                List.of(nodePropertyRecord)
            );
    }

    public NodePropertiesWritten perform(
        NodeTranslator nodeTranslator,
        Set<NodeLabel> nodeLabels,
        GraphStore graphStore,
        Optional<ResultStore> resolvedResultStore,
        Concurrency writeConcurrency,
        Label label,
        JobId jobId,
        List<NodePropertyRecord> nodeProperties
    ) {
        return Neo4jDatabaseNodePropertyWriter.writeNodeProperties(
            requestScopedDependencies.correlationId(),
            writeContext.nodePropertyExporterBuilder(),
            requestScopedDependencies.taskRegistryFactory(),
            nodeTranslator,
            nodeLabels,
            graphStore,
            writeConcurrency,
            nodeProperties,
            label.asString(),
            resolvedResultStore,
            jobId,
            requestScopedDependencies.terminationFlag(),
            log
        );
    }
}
