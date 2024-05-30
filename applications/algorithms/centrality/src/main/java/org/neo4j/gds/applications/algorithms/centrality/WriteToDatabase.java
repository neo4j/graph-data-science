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
package org.neo4j.gds.applications.algorithms.centrality;

import org.neo4j.gds.algorithms.writeservices.WriteNodePropertyService;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.config.WriteConfig;
import org.neo4j.gds.config.WritePropertyConfig;
import org.neo4j.gds.core.utils.progress.JobId;

class WriteToDatabase {
    private final WriteNodePropertyService writeNodePropertyService;

    WriteToDatabase(WriteNodePropertyService writeNodePropertyService) {
        this.writeNodePropertyService = writeNodePropertyService;
    }

    NodePropertiesWritten perform(
        Graph graph,
        GraphStore graphStore,
        ResultStore resultStore,
        WriteConfig writeConfiguration,
        WritePropertyConfig writePropertyConfiguration,
        LabelForProgressTracking label,
        JobId jobId,
        NodePropertyValues nodePropertyValues
    ) {
        var writeNodePropertyResult = writeNodePropertyService.write(
            graph,
            graphStore,
            nodePropertyValues,
            writeConfiguration.writeConcurrency(),
            writePropertyConfiguration.writeProperty(),
            label.value,
            writeConfiguration.arrowConnectionInfo(),
            writeConfiguration.resolveResultStore(resultStore),
            jobId
        );

        return new NodePropertiesWritten(writeNodePropertyResult.nodePropertiesWritten());
    }
}
