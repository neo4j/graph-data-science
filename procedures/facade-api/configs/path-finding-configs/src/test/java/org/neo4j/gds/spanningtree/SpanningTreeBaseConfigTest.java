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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.spy;

@GdlExtension
class SpanningTreeBaseConfigTest {

    @GdlGraph
    private static final String GRAPH = "(a)-[:foo{cost:1.0}]->(b)";

    @Inject
    private GraphStore graphStore;

    @Test
    void validateUndirected() {
        var configSpy = spy(SpanningTreeBaseConfig.class);

        assertThatIllegalArgumentException()
            .isThrownBy(() -> configSpy.validateUndirectedGraph(
                    graphStore,
                    null,
                    Set.of(RelationshipType.of("foo"))
                )
            )
            .withMessageContaining("The Spanning Tree algorithm works only with undirected graphs.")
            .withMessageContaining("Selected relationships `[foo]` are not all undirected.")
            .withMessageContaining("Please orient the edges properly");
    }
}
