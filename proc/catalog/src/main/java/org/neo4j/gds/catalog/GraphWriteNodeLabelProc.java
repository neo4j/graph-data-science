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

import org.neo4j.gds.applications.graphstorecatalog.WriteLabelResult;
import org.neo4j.gds.procedures.GraphDataScience;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.catalog.GraphCatalogProcedureConstants.WRITE_NODE_LABEL_DESCRIPTION;
import static org.neo4j.procedure.Mode.WRITE;

public class GraphWriteNodeLabelProc {
    @Context
    public GraphDataScience facade;

    @Procedure(name = "gds.graph.nodeLabel.write", mode = WRITE)
    @Description(WRITE_NODE_LABEL_DESCRIPTION)
    public Stream<WriteLabelResult> write(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeLabel") String nodeLabel,
        @Name(value = "configuration") Map<String, Object> configuration
    ) {
        return facade.catalog().writeNodeLabel(graphName, nodeLabel, configuration);
    }

    @Procedure(name = "gds.alpha.graph.nodeLabel.write", mode = WRITE, deprecatedBy = "gds.graph.nodeLabel.write")
    @Description(WRITE_NODE_LABEL_DESCRIPTION)
    @Internal
    @Deprecated(forRemoval = true)
    public Stream<WriteLabelResult> alphaWrite(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeLabel") String nodeLabel,
        @Name(value = "configuration") Map<String, Object> configuration
    ) {
        facade.deprecatedProcedures().called("gds.alpha.graph.nodeLabel.write");

        facade
            .log()
            .warn(
                "Procedure `gds.alpha.graph.nodeLabel.write` has been deprecated, please use `gds.graph.nodeLabel.write`.");

        return facade.catalog().writeNodeLabel(graphName, nodeLabel, configuration);
    }
}
