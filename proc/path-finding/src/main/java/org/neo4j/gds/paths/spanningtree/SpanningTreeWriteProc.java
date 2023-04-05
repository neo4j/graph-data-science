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
package org.neo4j.gds.paths.spanningtree;

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.MemoryEstimationExecutor;
import org.neo4j.gds.executor.ProcedureExecutor;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;
public class SpanningTreeWriteProc extends BaseProc {

    static final String procedure = "gds.beta.spanningTree.write";
    static final String DESCRIPTION =
        "The spanning tree algorithm visits all nodes that are in the same connected component as the starting node, " +
        "and returns a spanning tree of all nodes in the component where the total weight of the relationships is either minimized or maximized.";
    @Context
    public RelationshipExporterBuilder relationshipExporterBuilder;

    @Procedure(value = procedure, mode = WRITE)
    @Description(DESCRIPTION)
    public Stream<WriteResult> spanningTree(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return new ProcedureExecutor<>(
            new SpanningTreeWriteSpec(),
            executionContext()
        ).compute(graphName, configuration);
    }

    @Procedure(value = procedure + ".estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphName,
        @Name(value = "algoConfiguration") Map<String, Object> configuration
    ) {
        var spec = new SpanningTreeWriteSpec();
        return new MemoryEstimationExecutor<>(
            spec,
            executionContext(),
            transactionContext()
        ).computeEstimate(graphName, configuration);
    }

    @Override
    public ExecutionContext executionContext() {
        return super.executionContext().withRelationshipExporterBuilder(relationshipExporterBuilder);
    }
}
