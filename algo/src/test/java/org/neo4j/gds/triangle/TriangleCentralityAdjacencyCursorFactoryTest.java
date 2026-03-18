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
package org.neo4j.gds.triangle;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.GdlGraphStoreBuilder;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.core.huge.NodeFilteredGraph;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class TriangleCentralityAdjacencyCursorFactoryTest {

    @Test
    void shouldReuseFilteredCursorAndRefreshItsLookahead() {
        // Reuse one wrapper across exhausted, nonempty, and empty adjacency lists. Resetting its
        // inner cursor must also invalidate the previous list's cached lookahead and end-of-list state.
        var store = new GdlGraphStoreBuilder()
            .gdl("CREATE (outside:Drop), (a:Keep), (b:Keep), (isolated:Keep), " +
                "(a)-[:T]->(outside), (a)-[:T]->(b)")
            .orientation(Orientation.UNDIRECTED)
            .build();
        var graph = store.getGraph(
            List.of(NodeLabel.of("Keep")), List.of(RelationshipType.of("T")), Optional.empty()
        );
        assertThat(graph).isInstanceOf(NodeFilteredGraph.class);
        var factory = TriangleCentralityAdjacencyCursorFactory.forGraph(graph);
        var cursor = factory.cursorForNode(null, 0);
        assertThat(cursor.nextVLong()).isEqualTo(1);
        assertThat(cursor.hasNextVLong()).isFalse();

        var reused = factory.cursorForNode(cursor, 1);
        assertThat(reused).isSameAs(cursor);
        assertThat(reused.peekVLong()).isZero();
        assertThat(reused.nextVLong()).isZero();
        assertThat(reused.hasNextVLong()).isFalse();

        var empty = factory.cursorForNode(reused, 2);
        assertThat(empty.hasNextVLong()).isFalse();
        assertThat(empty.peekVLong()).isEqualTo(AdjacencyCursor.NOT_FOUND);

        var reset = factory.cursorForNode(empty, 0);
        assertThat(reset.nextVLong()).isEqualTo(1);
        assertThat(reset.hasNextVLong()).isFalse();
    }
}
