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
import org.neo4j.gds.impl.spanningtree.KSpanningTreeConfig;
import org.neo4j.gds.impl.spanningtree.Prim;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.WRITE_NODE_PROPERTY;
import static org.neo4j.gds.spanningtree.KSpanningTreeMaxProc.DESCRIPTION;
import static org.neo4j.procedure.Mode.WRITE;

@GdsCallable(name = "gds.alpha.spanningTree.kmax.write", description = DESCRIPTION, executionMode = WRITE_NODE_PROPERTY)
public class KSpanningTreeMaxProc extends KSpanningTreeProc {

    public static final String DESCRIPTION =
        "The maximum weight spanning tree (MST) starts from a given node, and finds all its reachable nodes " +
        "and the set of relationships that connect the nodes together with the maximum possible weight.";

    @Procedure(value = "gds.alpha.spanningTree.kmax.write", mode = WRITE)
    @Description(DESCRIPTION)
    public Stream<kWriteResult> kmax(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var computationResult = compute(graphName, configuration);
        return computationResultConsumer().consume(computationResult, executionContext());
    }

    @Override
    protected KSpanningTreeConfig newConfig(String username, CypherMapWrapper config) {
        return KSpanningTreeConfig.of(Prim.MAX_OPERATOR, config);
    }
}
