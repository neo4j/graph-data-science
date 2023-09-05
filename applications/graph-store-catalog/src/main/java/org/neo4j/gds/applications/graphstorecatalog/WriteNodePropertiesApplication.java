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

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.logging.Log;

/**
 * @deprecated collapse either this or NodePropertiesWriter - is this app or generic functionality?
 */
@Deprecated
public class WriteNodePropertiesApplication {
    private final Log log;
    private final NodePropertyExporterBuilder nodePropertyExporterBuilder;

    public WriteNodePropertiesApplication(Log log, NodePropertyExporterBuilder nodePropertyExporterBuilder) {
        this.log = log;
        this.nodePropertyExporterBuilder = nodePropertyExporterBuilder;
    }

    NodePropertiesWriteResult compute(
        GraphName graphName,
        GraphStore graphStore,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        UserLogRegistryFactory userLogRegistryFactory,
        GraphWriteNodePropertiesConfig configuration
    ) {
        var nodePropertiesWriter = new NodePropertiesWriter(log, nodePropertyExporterBuilder, terminationFlag);

        return nodePropertiesWriter.write(
            graphStore,
            taskRegistryFactory,
            userLogRegistryFactory,
            graphName.getValue(),
            configuration
        );
    }
}
