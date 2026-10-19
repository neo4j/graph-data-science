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
package org.neo4j.gds.api;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.nodes.NodeTranslator;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class GraphStoreNodeTranslatorTest {

    @GdlGraph(idOffset = 1337)
    static String GDL = " (x:Ignore)," +
                        " (a:Person)," +
                        " (b:Ignore:Person)," +
                        " (c:Person)," +
                        " (y:Ignore)," +
                        " (a)-->(b)," +
                        " (b)-->(c)," +
                        " (x)-->(y)";

    @Inject
    private GraphStore graphStore;

    @Inject
    private IdFunction idFunction;

    @Test
    void shouldUseRootNodeTranslatorForUnfilteredGraph() {
        var graph = graphStore.getGraph(RelationshipType.ALL_RELATIONSHIPS);

        assertThat(graphStore.nodeTranslator(graph.rootNodeMapping())).isSameAs(graphStore.nodeTranslator());
    }

    @Test
    void shouldTranslateFilteredGraph() {
        var filteredGraph = filteredGraph();

        assertTranslatesLike(graphStore.nodeTranslator(filteredGraph.rootNodeMapping()), filteredGraph);
    }

    @Test
    void shouldTranslateWrappedFilteredGraph() {
        var filteredGraph = filteredGraph();
        var wrappedGraph = new GraphAdapter(filteredGraph) {
            @Override
            public Graph concurrentCopy() {
                return this;
            }
        };

        assertTranslatesLike(graphStore.nodeTranslator(wrappedGraph.rootNodeMapping()), filteredGraph);
    }

    private Graph filteredGraph() {
        return graphStore.getGraph(NodeLabel.of("Person"), RelationshipType.ALL_RELATIONSHIPS, Optional.empty());
    }

    private void assertTranslatesLike(NodeTranslator translator, Graph filteredGraph) {
        assertThat(translator.nodeCount()).isEqualTo(3L);
        assertThat(translator.typeId()).isEqualTo(graphStore.nodes().typeId());
        assertThat(translator.highestOriginalId()).isEqualTo(graphStore.nodes().highestOriginalId());

        filteredGraph.forEachNode(filteredNodeId -> {
            var originalNodeId = filteredGraph.toOriginalNodeId(filteredNodeId);
            assertThat(translator.toOriginalNodeId(filteredNodeId)).isEqualTo(originalNodeId);
            assertThat(translator.toMappedNodeId(originalNodeId)).isEqualTo(filteredNodeId);
            assertThat(translator.containsOriginalId(originalNodeId)).isTrue();
            return true;
        });

        for (var variable : new String[]{"x", "y"}) {
            var originalNodeId = idFunction.of(variable);
            assertThat(translator.toMappedNodeId(originalNodeId)).isEqualTo(NodeTranslator.NOT_FOUND);
            assertThat(translator.containsOriginalId(originalNodeId)).isFalse();
        }

        assertThat(translator.toMappedNodeId(42L)).isEqualTo(NodeTranslator.NOT_FOUND);
        assertThat(translator.containsOriginalId(42L)).isFalse();
    }
}
