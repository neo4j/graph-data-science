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
package org.neo4j.gds.cliqueCounting;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.cliqueCounting.intersect.CliqueAdjacencyFactory;
import org.neo4j.gds.cliqueCounting.intersect.HugeGraphCliqueIntersect;
import org.neo4j.gds.cliqueCounting.intersect.NodeFilteredCliqueIntersect;
import org.neo4j.gds.cliqueCounting.intersect.UnionGraphCliqueIntersect;
import org.neo4j.gds.core.huge.CompositeAdjacencyCursor;
import org.neo4j.gds.core.huge.NodeFilteredAdjacencyCursor;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@GdlExtension
class CliqueAdjacencyFactoryTest {

    @GdlGraph
    private static final String DATA =
        """
            CREATE
                (a:A:B),
                (b:B),
                (c:A),
                (d:B),
                (e:A),
                (a)-[:R1]->(b),
                (a)-[:R]->(c),
                (a)-[:R]->(d),
                (a)-[:R]->(e)
            """;

    @Inject
    private GraphStore graphStore;

    @Inject
    private IdFunction idFunction;

    @Test
    void shouldGenerateNormalCursor(){

        var graph = graphStore.getGraph(RelationshipType.of("R"));

        var idA =  graph.toMappedNodeId(idFunction.of("a"));
        var expectedIds = Stream.of("e","c","d")
            .mapToLong(idFunction::of)
            .map(graph::toMappedNodeId)
            .sorted()
            .toArray();

        var cliqueAdjacency = CliqueAdjacencyFactory.createCliqueAdjacency(graph);
        assertThat(cliqueAdjacency).isInstanceOf(HugeGraphCliqueIntersect.class);

        var cursor = cliqueAdjacency.createCursor(idA);

        assertThat(cursor.peekVLong()).isEqualTo(expectedIds[0]);
        assertThat(cursor.nextVLong()).isEqualTo(expectedIds[0]);

        assertThat(cursor.peekVLong()).isEqualTo(expectedIds[1]);
        assertThat(cursor.nextVLong()).isEqualTo(expectedIds[1]);

        assertThat(cursor.peekVLong()).isEqualTo(expectedIds[2]);
        assertThat(cursor.nextVLong()).isEqualTo(expectedIds[2]);

        assertThat(cursor.hasNextVLong()).isFalse();

    }

    @Test
    void shouldGenerateUnionCursor(){

        var graph = graphStore.getUnion();

        var idA =  graph.toMappedNodeId(idFunction.of("a"));
        var expectedIds = Stream.of("b","e","c","d")
            .mapToLong(idFunction::of)
            .map(graph::toMappedNodeId)
            .sorted()
            .toArray();

        var cliqueAdjacency = CliqueAdjacencyFactory.createCliqueAdjacency(graph);
        assertThat(cliqueAdjacency).isInstanceOf(UnionGraphCliqueIntersect.class);

        var cursor = cliqueAdjacency.createCursor(idA);

        assertThat(cursor.peekVLong()).isEqualTo(expectedIds[0]);
        assertThat(cursor.nextVLong()).isEqualTo(expectedIds[0]);

        assertThat(cursor.peekVLong()).isEqualTo(expectedIds[1]);
        assertThat(cursor.nextVLong()).isEqualTo(expectedIds[1]);

        assertThat(cursor.peekVLong()).isEqualTo(expectedIds[2]);
        assertThat(cursor.nextVLong()).isEqualTo(expectedIds[2]);


        assertThat(cursor.peekVLong()).isEqualTo(expectedIds[3]);
        assertThat(cursor.nextVLong()).isEqualTo(expectedIds[3]);

        assertThat(cursor.hasNextVLong()).isFalse();

    }

    static Stream<Arguments>  unionLabelFilter() {
        return Stream.of(

            arguments("A", Stream.of("e","c")),
            arguments("B", Stream.of("b","d"))
        );
    }

    @ParameterizedTest
    @MethodSource("unionLabelFilter")
    void shouldGenerateUnionCursorForFilteredGraphs(String label, Stream<String>  expectedAnswer){

        var graph = graphStore.getGraph(
            List.of(NodeLabel.of(label)),
            List.of(RelationshipType.of("R1"),RelationshipType.of("R")),
            Optional.empty()
        );

        var idA =  graph.toMappedNodeId(idFunction.of("a"));
        var expectedIds = expectedAnswer
            .mapToLong(idFunction::of)
            .map(graph::toMappedNodeId)
            .sorted()
            .toArray();

        var cliqueAdjacency = CliqueAdjacencyFactory.createCliqueAdjacency(graph);
        assertThat(cliqueAdjacency).isInstanceOf(UnionGraphCliqueIntersect.class);

        var cursor = cliqueAdjacency.createCursor(idA);
        assertThat(cursor).isInstanceOf(CompositeAdjacencyCursor.class);

        assertThat(cursor.peekVLong()).isEqualTo(expectedIds[0]);
        assertThat(cursor.nextVLong()).isEqualTo(expectedIds[0]);

        assertThat(cursor.peekVLong()).isEqualTo(expectedIds[1]);
        assertThat(cursor.nextVLong()).isEqualTo(expectedIds[1]);

        assertThat(cursor.hasNextVLong()).isFalse();

    }

    static Stream<Arguments>  hugeLabelFilter() {
        return Stream.of(

            arguments("A", "R", Stream.of("e","c")),
            arguments("B", "R1",Stream.of("b")),
            arguments("B", "R", Stream.of("d"))
        );
    }

    @ParameterizedTest
    @MethodSource("hugeLabelFilter")
    void shouldGenerateCursorForFilteredGraph(String label, String relType, Stream<String>  expectedAnswer){

        var graph = graphStore.getGraph(
            List.of(NodeLabel.of(label)),
            List.of(RelationshipType.of(relType)),
            Optional.empty()
        );

        var idA =  graph.toMappedNodeId(idFunction.of("a"));
        var expectedIds = expectedAnswer
            .mapToLong(idFunction::of)
            .map(graph::toMappedNodeId)
            .sorted()
            .toArray();

        var cliqueAdjacency = CliqueAdjacencyFactory.createCliqueAdjacency(graph);
        assertThat(cliqueAdjacency).isInstanceOf(NodeFilteredCliqueIntersect.class);

        var cursor = cliqueAdjacency.createCursor(idA);
        assertThat(cursor).isInstanceOf(NodeFilteredAdjacencyCursor.class);
        for (long expectedId : expectedIds) {
            assertThat(cursor.peekVLong()).isEqualTo(expectedId);
            assertThat(cursor.nextVLong()).isEqualTo(expectedId);
        }

        assertThat(cursor.hasNextVLong()).isFalse();

    }



}
