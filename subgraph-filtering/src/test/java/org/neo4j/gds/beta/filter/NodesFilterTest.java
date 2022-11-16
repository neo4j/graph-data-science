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
package org.neo4j.gds.beta.filter;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.beta.filter.expression.ExpressionParser;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.opencypher.v9_0.parser.javacc.ParseException;

import java.util.ArrayList;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class NodesFilterTest {

    @GdlGraph(idOffset = 17)
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:A {p: 1L})" +
        ", (b:A {p: 2L})" +
        ", (c:A {p: 3L})" +
        ", (d:B)";

    @Inject
    GraphStore graphStore;

    @Inject
    IdFunction idFunction;

    @Test
    void basicFiltering() throws ParseException {
        var filteredNodes = NodesFilter.filterNodes(
            graphStore,
            ExpressionParser.parse("n:A AND n.p > 1.0", Map.of()),
            1,
            Map.of(),
            Pools.DEFAULT_SINGLE_THREAD_POOL,
            ProgressTracker.NULL_TRACKER
        );

        var idMap = filteredNodes.idMap();
        var filteredNodeIds = new ArrayList<Long>();

        assertThat(idMap.availableNodeLabels())
            .containsExactly(NodeLabel.of("A"));

        idMap.forEachNode(n -> {
            filteredNodeIds.add(idMap.toOriginalNodeId(n));
            return true;
        });

        assertThat(filteredNodeIds)
            .containsExactlyInAnyOrder(
                idFunction.of("b"),
                idFunction.of("c")
            );
    }
}
