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

import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ProcPreconditions;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.WRITE;

public class GraphWriteNodePropertiesProc extends CatalogProc {

    @Context
    public NodePropertyExporterBuilder nodePropertyExporterBuilder;

    @Procedure(name = "gds.graph.nodeProperties.write", mode = WRITE)
    @Description("Writes the given node properties to an online Neo4j database.")
    public Stream<NodePropertiesWriteResult> writeNodeProperties(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeProperties") Object nodeProperties,
        @Name(value = "nodeLabels", defaultValue = "['*']") Object nodeLabels,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ProcPreconditions.check();

        return NodePropertiesWriter.write(
            graphName,
            nodeProperties,
            nodeLabels,
            configuration,
            executionContext(),
            Optional.empty()
        );
    }

    @Procedure(name = "gds.graph.writeNodeProperties", mode = WRITE, deprecatedBy = "gds.graph.nodeProperties.write")
    @Description("Writes the given node properties to an online Neo4j database.")
    public Stream<NodePropertiesWriteResult> run(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeProperties") Object nodeProperties,
        @Name(value = "nodeLabels", defaultValue = "['*']") Object nodeLabels,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ProcPreconditions.check();

        var deprecationWarning = "This procedures is deprecated for removal. Please use `gds.graph.nodeProperties.write`";
        return NodePropertiesWriter.write(
            graphName,
            nodeProperties,
            nodeLabels,
            configuration,
            executionContext(),
            Optional.of(deprecationWarning)
        );
    }

    @Override
    public ExecutionContext executionContext() {
        return super.executionContext().withNodePropertyExporterBuilder(nodePropertyExporterBuilder);
    }
}
