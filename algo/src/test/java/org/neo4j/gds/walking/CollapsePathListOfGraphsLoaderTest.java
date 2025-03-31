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
package org.neo4j.gds.walking;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.collapsepath.CollapsePathParameters;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestSupport.assertGraphEquals;

@GdlExtension
class CollapsePathListOfGraphsLoaderTest {

    @GdlGraph
    private static final String SINGLE =
        "CREATE" +
            "  (a)" +
            "  (b)" +
            "  (c)" +
            "(a)-[:R1]->(b)" +
            "(b)-[:R1]->(c)" +
            "(a)-[:R2]->(c)";


    @Inject
    private GraphStore graphStore;

    @Test
    void shouldLoadGraphsCorrectly(){
        var parameters = new CollapsePathParameters(
            new Concurrency(1),
            List.of(List.of("R1","R2"), List.of("R1"), List.of("R2")),
            Set.of(NodeLabel.ALL_NODES),
            false,
            "foo"
        );

        var graphs = CollapsePathListOfGraphsLoader.graphs(graphStore,parameters);

        assertThat(graphs.size()).isEqualTo(3);

        var graphs12 = graphs.get(0);
        var graphs1 = graphs.get(1);
        var graphs2 = graphs.get(2);

        assertThat(graphs12.length).isEqualTo(2);
        assertThat(graphs1.length).isEqualTo(1);
        assertThat(graphs1.length).isEqualTo(1);

        var expectedR1 = graphStore.getGraph(RelationshipType.of("R1"));
        var expectedR2 = graphStore.getGraph(RelationshipType.of("R2"));

        assertGraphEquals(graphs12[0], expectedR1);
        assertGraphEquals(graphs12[1], expectedR2);
        assertGraphEquals(graphs1[0], expectedR1);
        assertGraphEquals(graphs2[0], expectedR2);

    }

}
