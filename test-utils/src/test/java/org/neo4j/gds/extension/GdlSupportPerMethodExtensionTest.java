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
package org.neo4j.gds.extension;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class GdlSupportPerMethodExtensionTest {

    @GdlGraph(idOffset = 42, graphNamePrefix = "idOffset")
    public static final String ID_OFFSET_GRAPH = "(a)-[:REL]->(b)";
    @Inject
    private TestGraph idOffsetGraph;
    @Inject
    private IdFunction idOffsetIdFunction;

    @Test
    void testIdOffset() {
        assertThat(idOffsetGraph.nodeCount()).isEqualTo(2L);
        assertThat(idOffsetGraph.relationshipCount()).isEqualTo(1L);
        assertThat(idOffsetIdFunction.of("a")).isEqualTo(42L);
        assertThat(idOffsetIdFunction.of("b")).isEqualTo(43L);
        assertThat(idOffsetGraph.toMappedNodeId(42L)).isEqualTo(0L);
        assertThat(idOffsetGraph.toMappedNodeId(43L)).isEqualTo(1L);
    }
}
