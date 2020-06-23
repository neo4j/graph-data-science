/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.gds.embeddings.graphsage.subgraph;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.embeddings.graphsage.NeighborhoodFunction;
import org.neo4j.gds.embeddings.graphsage.UniformNeighborhoodSampler;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@GdlExtension
class SubGraphBuilderCustomTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static final String GRAPH =
        "(a)-[]->(b)-[]->(c), " +
        "(a)-[]->(d), " +
        "(a)-[]->(e), " +
        "(a)-[]->(f), " +
        "(g)-[]->(i), " +
        "(i)-[]->(a), " +
        "(i)-[]->(j), " +
        "(j)-[]->(b)";

    @Inject
    private IdFunction idFunction;

    @Inject
    private Graph graph;

    private UniformNeighborhoodSampler sampler;
    private NeighborhoodFunction neighborhoodFunction;
    private SubGraphBuilder subGraphBuilder;

    @BeforeEach
    void setup() {
        sampler = new UniformNeighborhoodSampler();
        neighborhoodFunction = (graph, nodeId) ->
            sampler.sample(graph, nodeId, 100, 42);
        subGraphBuilder = new SubGraphBuilderImpl();
    }

    @Test
    void shouldBuildSubGraph() {

        /*
            "(a: b0)-[]->(b: a1)-[]->(c: b7), " +
            "(a)-[]->(d: a2), " +
            "(a)-[]->(e: a3), " +
            "(a)-[]->(f: a4), " +
            "(g)-[]->(i), " +
            "(i: a5)-[]->(a), " +
            "(i)-[]->(j), " +
            "(j: b8)-[]->(b)";
         */

        SubGraph subGraph = subGraphBuilder.buildSubGraph(
            List.of(
                idFunction.of("a"),
                idFunction.of("b"),
                idFunction.of("i"),
                idFunction.of("i"),
                idFunction.of("a"),
                idFunction.of("a")
            ),
            neighborhoodFunction,
            graph
        );

        assertEquals(6, subGraph.adjacency.length);
    }
}
