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
package org.neo4j.gds.allshortestpaths;

import org.apache.logging.log4j.util.TriConsumer;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * 1
 * (0)->(1)
 * 1 | 1 | 1
 * v   v
 * (2)->(3)
 * 1 | 1 | 1
 * v   v
 * (4)->(5)
 * 1 | 1 | 1
 * v   v
 * (6)->(7)
 * 1 | 1  | 1
 * v    v
 * (8)->(9)
 */
@GdlExtension
class MSBFSAllShortestPathsTest {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE " +
            "  (a0:Node)" +
            ", (a1:Node)" +
            ", (a2:Node)" +
            ", (a3:Node)" +
            ", (a4:Node)" +
            "  (a5:Node)" +
            ", (a6:Node)" +
            ", (a7:Node)" +
            ", (a8:Node)" +
            ", (a9:Node)" +

            ", (a0)-[:R {w:1}]->(a1)" +
            ", (a0)-[:R {w:1}]->(a2)" +
            ", (a1)-[:R {w:1}]->(a3)" +
            ", (a2)-[:R {w:1}]->(a3)" +
            ", (a2)-[:R {w:1}]->(a4)" +
            ", (a3)-[:R {w:1}]->(a5)" +
            ", (a4)-[:R {w:1}]->(a5)" +
            ", (a4)-[:R {w:1}]->(a6)" +
            ", (a5)-[:R {w:1}]->(a7)" +
            ", (a6)-[:R {w:1}]->(a7)" +
            ", (a6)-[:R {w:1}]->(a8)" +
            ", (a7)-[:R {w:1}]->(a9)" +
            ", (a8)-[:R {w:1}]->(a9)";

    @Inject
    private Graph graph;

    @Inject
    private IdFunction idFunction;


    @Test
    void testResults() {

        var hugeMSBFSAllShortestPaths = new MSBFSAllShortestPaths(
            graph,
            ConcurrencyConfig.TYPED_DEFAULT_CONCURRENCY,
            DefaultPool.INSTANCE
        );

        TriConsumer<Long, Long, Double> mock = mock(TriConsumer.class);

        hugeMSBFSAllShortestPaths
                .compute()
                .forEach(r -> {
                    assertThat(r.sourceNodeId).isLessThan(r.targetNodeId);
                    mock.accept(r.sourceNodeId, r.targetNodeId, r.distance);
                });

        verify(mock, times(35)).accept(anyLong(), anyLong(), anyDouble());

        long a0 = idFunction.of("a0");
        long a9 = idFunction.of("a9");

        verify(mock, times(1)).accept(a0, a9, 5.0);
    }

}
