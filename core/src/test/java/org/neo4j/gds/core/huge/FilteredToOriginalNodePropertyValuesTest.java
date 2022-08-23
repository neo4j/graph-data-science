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
package org.neo4j.gds.core.huge;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.CSRGraph;
import org.neo4j.gds.api.FilteredIdMap;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.huge.FilteredNodePropertyValues.FilteredToOriginalNodePropertyValues;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class FilteredToOriginalNodePropertyValuesTest {

    @GdlGraph
    private static final String GDL =
        " (a:A { doubleArray: [1.0], longArray: [1L], floatArray: [1.0F] })" +
        " (b:B { doubleArray: [1.0, 2.22, 1.337], longArray: [1L, 222L, 1337L], floatArray: [1.0F, 2.22F, 1.337F] })" +
        " (c:ignore)";

    @Inject
    private CSRGraph graph;

    @Inject
    private GraphStore graphStore;

    @Inject
    private IdFunction idFunction;

    @Test
    void testDoubleArray() {
        var filteredNodeProperties = new FilteredToOriginalNodePropertyValues(
            graph.nodeProperties("doubleArray"),
            new NodeFilteredGraph(
                graph,
                (FilteredIdMap) graphStore.getGraph(Set.of(NodeLabel.of("A"), NodeLabel.of("B")))
            )
        );

        assertThat(filteredNodeProperties.doubleArrayValue(idFunction.of("a"))).containsExactly(1D);
        assertThat(filteredNodeProperties.doubleArrayValue(idFunction.of("b"))).containsExactly(
            new double[]{1D, 2.22D, 1.337D},
            Offset.offset(1e-5)
        );
    }

    @Test
    void testLongArray() {
        var filteredNodeProperties = new FilteredToOriginalNodePropertyValues(
            graph.nodeProperties("longArray"),
            new NodeFilteredGraph(
                graph,
                (FilteredIdMap) graphStore.getGraph(Set.of(NodeLabel.of("A"), NodeLabel.of("B")))
            )
        );

        assertThat(filteredNodeProperties.longArrayValue(idFunction.of("a"))).containsExactly(1L);
        assertThat(filteredNodeProperties.longArrayValue(idFunction.of("b"))).containsExactly(1L, 222L, 1337L);
    }

    @Test
    void testFloatArray() {
        var filteredNodeProperties = new FilteredNodePropertyValues.FilteredToOriginalNodePropertyValues(
            graph.nodeProperties("floatArray"),
            new NodeFilteredGraph(
                graph,
                (FilteredIdMap) graphStore.getGraph(Set.of(NodeLabel.of("A"), NodeLabel.of("B")))
            )
        );

        assertThat(filteredNodeProperties.floatArrayValue(idFunction.of("a"))).containsExactly(1.0F);
        assertThat(filteredNodeProperties.floatArrayValue(idFunction.of("b"))).containsExactly(1.0F, 2.22F, 1.337F);
    }
}
