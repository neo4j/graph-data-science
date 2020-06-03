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
package org.neo4j.graphalgo.pagerank;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.gdl.GdlFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@GdlExtension
class WeightedPageRankTest {

    private static final PageRankBaseConfig DEFAULT_CONFIG = ImmutablePageRankStreamConfig
        .builder()
        .maxIterations(40)
        .build();

    @GdlGraph
    private static final String DB_CYPHER =
            "CREATE" +
            "  (a:Label)" +
            ", (b:Label)" +
            ", (c:Label)" +
            ", (d:Label)" +
            ", (e:Label)" +
            ", (f:Label)" +
            ", (g:Label)" +
            ", (h:Label)" +
            ", (i:Label)" +
            ", (j:Label)" +

            ", (b)-[:TYPE1 {weight: 0}]->(c)" +
            ", (c)-[:TYPE1 {weight: 0}]->(b)" +
            ", (d)-[:TYPE1 {weight: 0}]->(a)" +
            ", (d)-[:TYPE1 {weight: 0}]->(b)" +
            ", (e)-[:TYPE1 {weight: 0}]->(b)" +
            ", (e)-[:TYPE1 {weight: 0}]->(d)" +
            ", (e)-[:TYPE1 {weight: 0}]->(f)" +
            ", (f)-[:TYPE1 {weight: 0}]->(b)" +
            ", (f)-[:TYPE1 {weight: 0}]->(e)" +

            ", (b)-[:TYPE2 {weight: 1}]->(c)" +
            ", (c)-[:TYPE2 {weight: 1}]->(b)" +
            ", (d)-[:TYPE2 {weight: 1}]->(a)" +
            ", (d)-[:TYPE2 {weight: 1}]->(b)" +
            ", (e)-[:TYPE2 {weight: 1}]->(b)" +
            ", (e)-[:TYPE2 {weight: 1}]->(d)" +
            ", (e)-[:TYPE2 {weight: 1}]->(f)" +
            ", (f)-[:TYPE2 {weight: 1}]->(b)" +
            ", (f)-[:TYPE2 {weight: 1}]->(e)" +

            ", (b)-[:TYPE3 {weight: 1.0}]->(c)" +
            ", (c)-[:TYPE3 {weight: 1.0}]->(b)" +
            ", (d)-[:TYPE3 {weight: 0.3}]->(a)" +
            ", (d)-[:TYPE3 {weight: 0.7}]->(b)" +
            ", (e)-[:TYPE3 {weight: 0.9}]->(b)" +
            ", (e)-[:TYPE3 {weight: 0.05}]->(d)" +
            ", (e)-[:TYPE3 {weight: 0.05}]->(f)" +
            ", (f)-[:TYPE3 {weight: 0.9}]->(b)" +
            ", (f)-[:TYPE3 {weight: 0.1}]->(e)" +

            ", (b)-[:TYPE4 {weight: 1.0}]->(c)" +
            ", (c)-[:TYPE4 {weight: 1.0}]->(b)" +
            ", (d)-[:TYPE4 {weight: 0.3}]->(a)" +
            ", (d)-[:TYPE4 {weight: 0.7}]->(b)" +
            ", (e)-[:TYPE4 {weight: 0.9}]->(b)" +
            ", (e)-[:TYPE4 {weight: 0.05}]->(d)" +
            ", (e)-[:TYPE4 {weight: 0.05}]->(f)" +
            ", (f)-[:TYPE4 {weight: 0.9}]->(b)" +
            ", (f)-[:TYPE4 {weight: -0.9}]->(a)" +
            ", (f)-[:TYPE4 {weight: 0.1}]->(e)";

    @Inject
    private GraphStore graphStore;

    @Inject
    private GdlFactory gdlFactory;

    @Test
    void defaultWeightOf0MeansNoDiffusionOfPageRank() {
        var expected = Map.of(
            gdlFactory.nodeId("a"), 0.15,
            gdlFactory.nodeId("b"), 0.15,
            gdlFactory.nodeId("c"), 0.15,
            gdlFactory.nodeId("d"), 0.15,
            gdlFactory.nodeId("e"), 0.15,
            gdlFactory.nodeId("f"), 0.15,
            gdlFactory.nodeId("g"), 0.15,
            gdlFactory.nodeId("h"), 0.15,
            gdlFactory.nodeId("i"), 0.15,
            gdlFactory.nodeId("j"), 0.15
        );

        var graph = graphStore.getGraph(
            List.of(NodeLabel.of("Label")),
            List.of(RelationshipType.of("TYPE1")),
            Optional.of("weight")
        );

        PageRankTest.assertResult(graph, PageRankAlgorithmType.WEIGHTED, expected);
    }

    @Test
    void allWeightsTheSameShouldBeTheSameAsPageRank() {
        var expected = Map.of(
            gdlFactory.nodeId("a"), 0.243007,
            gdlFactory.nodeId("b"), 1.9183995,
            gdlFactory.nodeId("c"), 1.7806315,
            gdlFactory.nodeId("d"), 0.21885,
            gdlFactory.nodeId("e"), 0.243007,
            gdlFactory.nodeId("f"), 0.21885,
            gdlFactory.nodeId("g"), 0.15,
            gdlFactory.nodeId("h"), 0.15,
            gdlFactory.nodeId("i"), 0.15,
            gdlFactory.nodeId("j"), 0.15
        );

        var graph = graphStore.getGraph(
            List.of(NodeLabel.of("Label")),
            List.of(RelationshipType.of("TYPE2")),
            Optional.of("weight")
        );

        PageRankTest.assertResult(graph, PageRankAlgorithmType.WEIGHTED, expected);
    }

    @Test
    void higherWeightsLeadToHigherPageRank() {
        var expected = Map.of(
            gdlFactory.nodeId("a"), 0.1900095,
            gdlFactory.nodeId("b"), 2.2152279,
            gdlFactory.nodeId("c"), 2.0325884,
            gdlFactory.nodeId("d"), 0.1569275,
            gdlFactory.nodeId("e"), 0.1633280,
            gdlFactory.nodeId("f"), 0.1569275,
            gdlFactory.nodeId("g"), 0.15,
            gdlFactory.nodeId("h"), 0.15,
            gdlFactory.nodeId("i"), 0.15,
            gdlFactory.nodeId("j"), 0.15
        );

        var graph = graphStore.getGraph(
            List.of(NodeLabel.of("Label")),
            List.of(RelationshipType.of("TYPE3")),
            Optional.of("weight")
        );

        PageRankTest.assertResult(graph, PageRankAlgorithmType.WEIGHTED, expected);
    }

    @Test
    void shouldExcludeNegativeWeights() {
        var expected = Map.of(
            gdlFactory.nodeId("a"), 0.1900095,
            gdlFactory.nodeId("b"), 2.2152279,
            gdlFactory.nodeId("c"), 2.0325884,
            gdlFactory.nodeId("d"), 0.1569275,
            gdlFactory.nodeId("e"), 0.1633280,
            gdlFactory.nodeId("f"), 0.1569275,
            gdlFactory.nodeId("g"), 0.15,
            gdlFactory.nodeId("h"), 0.15,
            gdlFactory.nodeId("i"), 0.15,
            gdlFactory.nodeId("j"), 0.15
        );

        var graph = graphStore.getGraph(
            List.of(NodeLabel.of("Label")),
            List.of(RelationshipType.of("TYPE4")),
            Optional.of("weight")
        );

        PageRankTest.assertResult(graph, PageRankAlgorithmType.WEIGHTED, expected);
    }
}
