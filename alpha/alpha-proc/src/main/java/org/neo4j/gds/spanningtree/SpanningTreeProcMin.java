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
package org.neo4j.gds.spanningtree;

import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.impl.spanningtree.Prim;
import org.neo4j.gds.impl.spanningtree.SpanningTreeConfig;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.WRITE_NODE_PROPERTY;
import static org.neo4j.gds.spanningtree.SpanningTreeProcMin.MIN_DESCRIPTION;
import static org.neo4j.procedure.Mode.WRITE;

// TODO: Always undirected
@GdsCallable(name = "gds.alpha.spanningTree.write", description = MIN_DESCRIPTION, executionMode = WRITE_NODE_PROPERTY)
public class SpanningTreeProcMin extends SpanningTreeProc {

    static final String MIN_DESCRIPTION =
        "Minimum weight spanning tree visits all nodes that are in the same connected component as the starting node, " +
        "and returns a spanning tree of all nodes in the component where the total weight of the relationships is minimized.";

    @Procedure(value = "gds.alpha.spanningTree.write", mode = WRITE)
    @Description(MIN_DESCRIPTION)
    public Stream<Prim.Result> spanningTree(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var computationResult = compute(graphName, configuration);
        return computationResultConsumer().consume(computationResult, executionContext());
    }

    @Procedure(value = "gds.alpha.spanningTree.minimum.write", mode = WRITE)
    @Description(MIN_DESCRIPTION)
    public Stream<Prim.Result> minimumSpanningTree(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var computationResult = compute(graphName, configuration);
        return computationResultConsumer().consume(computationResult, executionContext());
    }

    @Override
    protected SpanningTreeConfig newConfig(String username, CypherMapWrapper config) {
        return SpanningTreeConfig.of(Prim.MIN_OPERATOR, config);
    }
}
