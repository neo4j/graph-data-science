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
package org.neo4j.gds.bridges;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class SmallBridgesTest {

    @GdlExtension
    @Nested
    class GraphWithBridges {
        @GdlGraph(orientation = Orientation.UNDIRECTED)
        private static final String GRAPH =
            """
                CREATE
                    (a:Node {id: 0}),
                    (b:Node {id: 1}),
                    (c:Node {id: 2}),
                    (d:Node {id: 3}),
                    (e:Node {id: 4}),
                    (a)-[:R]->(d),
                    (b)-[:R]->(a),
                    (c)-[:R]->(a),
                    (c)-[:R]->(b),
                    (d)-[:R]->(e)
                """;

        @Inject
        private TestGraph graph;


        @Test
        void shouldFindBridges() {
            var bridges = new Bridges(graph);
            var result = bridges.compute().bridges();

            assertThat(result)
                .isNotNull()
                .containsExactlyInAnyOrder(
                     Bridge.create(graph.toMappedNodeId("a"), graph.toMappedNodeId("d")),
                     Bridge.create(graph.toMappedNodeId("d"), graph.toMappedNodeId("e"))
                );
        }
    }

    @GdlExtension
    @Nested
    class GraphWithoutBridges {

        @GdlGraph(orientation = Orientation.UNDIRECTED)
        private static final String GRAPH =
            """
                CREATE
                    (a:Node {id: 0}),
                    (b:Node {id: 1}),
                    (c:Node {id: 2}),
                    (d:Node {id: 3}),
                    (e:Node {id: 4}),
                    (a)-[:R]->(d),
                    (a)-[:R]->(e),
                    (b)-[:R]->(a),
                    (c)-[:R]->(a),
                    (c)-[:R]->(b),
                    (d)-[:R]->(e)
                """;

        @Inject
        private TestGraph graph;


        @Test
        void shouldFindBridges() {
            var bridges = new Bridges(graph);
            var result = bridges.compute().bridges();

            assertThat(result)
                .isNotNull()
                .isEmpty();
        }
    }



}
