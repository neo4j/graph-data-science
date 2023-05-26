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

import org.neo4j.gds.executor.ProcPreconditions;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.opencypher.v9_0.parser.javacc.ParseException;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class GraphMutateNodeLabelProc extends CatalogProc {

    @Procedure(name = "gds.alpha.graph.nodeLabel.mutate", mode = READ)
    @Description("Mutates the in-memory graph with the given node Label.")
    public Stream<MutateLabelResult> mutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeLabel") String nodeLabel,
        @Name(value = "configuration") Map<String, Object> configuration
    ) throws ParseException {

        ProcPreconditions.check();
        return NodeLabelMutator.mutateNodeLabel(graphName, nodeLabel, configuration, executionContext());

    }
}
