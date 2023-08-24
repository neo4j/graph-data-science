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
package org.neo4j.gds.catalog;

import org.neo4j.gds.applications.graphstorecatalog.GraphStreamNodePropertiesResult;
import org.neo4j.gds.applications.graphstorecatalog.GraphStreamNodePropertyResult;
import org.neo4j.gds.procedures.OpenGraphDataScience;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.catalog.GraphCatalogProcedureConstants.STREAM_NODE_PROPERTIES_DESCRIPTION;
import static org.neo4j.gds.catalog.GraphCatalogProcedureConstants.STREAM_NODE_PROPERTY_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class GraphStreamNodePropertiesProc {
    @Context
    public OpenGraphDataScience facade;

    @SuppressWarnings("unused")
    @Procedure(name = "gds.graph.nodeProperties.stream", mode = READ)
    @Description(STREAM_NODE_PROPERTIES_DESCRIPTION)
    public Stream<GraphStreamNodePropertiesResult> streamNodeProperties(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeProperties") Object nodeProperties,
        @Name(value = "nodeLabels", defaultValue = "['*']") Object nodeLabels,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.catalog().streamNodeProperties(
            graphName,
            nodeProperties,
            nodeLabels,
            configuration
        );
    }

    @SuppressWarnings("unused")
    @Procedure(name = "gds.graph.streamNodeProperties", mode = READ, deprecatedBy = "gds.graph.nodeProperties.stream")
    @Description(STREAM_NODE_PROPERTIES_DESCRIPTION)
    @Deprecated(forRemoval = true)
    @Internal
    public Stream<GraphStreamNodePropertiesResult> deprecatedStreamNodeProperties(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeProperties") Object nodeProperties,
        @Name(value = "nodeLabels", defaultValue = "['*']") Object nodeLabels,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade
            .log()
            .warn(
                "Procedure `gds.graph.streamNodeProperties` has been deprecated, please use `gds.graph.nodeProperties.stream`.");

        return facade.catalog().streamNodeProperties(
            graphName,
            nodeProperties,
            nodeLabels,
            configuration
        );
    }

    @SuppressWarnings("unused")
    @Procedure(name = "gds.graph.nodeProperty.stream", mode = READ)
    @Description(STREAM_NODE_PROPERTY_DESCRIPTION)
    public Stream<GraphStreamNodePropertyResult> streamNodeProperty(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeProperties") String nodeProperty,
        @Name(value = "nodeLabels", defaultValue = "['*']") Object nodeLabels,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.catalog().streamNodeProperty(
            graphName,
            nodeProperty,
            nodeLabels,
            configuration
        );
    }

    @Procedure(name = "gds.graph.streamNodeProperty", mode = READ, deprecatedBy = "gds.graph.nodeProperty.stream")
    @Description(STREAM_NODE_PROPERTY_DESCRIPTION)
    @Internal
    @Deprecated(forRemoval = true)
    public Stream<GraphStreamNodePropertyResult> streamProperty(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeProperties") String nodeProperty,
        @Name(value = "nodeLabels", defaultValue = "['*']") Object nodeLabels,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade
            .log()
            .warn(
                "Procedure `gds.graph.streamNodeProperty` has been deprecated, please use `gds.graph.nodeProperty.stream`.");

        return facade.catalog().streamNodeProperty(
            graphName,
            nodeProperty,
            nodeLabels,
            configuration
        );
    }
}
