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
package org.neo4j.gds.paths.kspanningtree;

import org.neo4j.gds.procedures.GraphDataScience;
import org.neo4j.gds.procedures.algorithms.pathfinding.KSpanningTreeWriteResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.paths.kspanningtree.Constants.K_SPANNING_TREE_DESCRIPTION;
import static org.neo4j.procedure.Mode.WRITE;

public class KSpanningTreeWriteProc {
    @Context
    public GraphDataScience facade;

    @Procedure(value = "gds.kSpanningTree.write", mode = WRITE)
    @Description(K_SPANNING_TREE_DESCRIPTION)
    public Stream<KSpanningTreeWriteResult> write(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.pathFinding().kSpanningTreeWrite(graphName, configuration);
    }

    @Procedure(value = "gds.alpha.kSpanningTree.write", mode = WRITE, deprecatedBy = "gds.kSpanningTree.write")
    @Description(K_SPANNING_TREE_DESCRIPTION)
    @Internal
    @Deprecated(forRemoval = true)
    public Stream<KSpanningTreeWriteResult> alphaWrite(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade.deprecatedProcedures().called("gds.alpha.kSpanningTree.write");
        facade
            .log()
            .warn("Procedure `gds.alpha.kSpanningTree.write` has been deprecated, please use `gds.kSpanningTree.write`.");
        return write(graphName, configuration);
    }
}
