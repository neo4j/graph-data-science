/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;

import java.util.Map;

@GdlExtension
class WeightedPageRankTest {

    private static final String NODE_CREATE_QUERY =
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
        ", (j:Label)";

    @GdlGraph(graphNamePrefix = "zeroWeights")
    private static final String ZERO_WEIGHTS_GRAPH =
        NODE_CREATE_QUERY +
        "  (b)-[:TYPE1 {weight: 0}]->(c)" +
        ", (c)-[:TYPE1 {weight: 0}]->(b)" +
        ", (d)-[:TYPE1 {weight: 0}]->(a)" +
        ", (d)-[:TYPE1 {weight: 0}]->(b)" +
        ", (e)-[:TYPE1 {weight: 0}]->(b)" +
        ", (e)-[:TYPE1 {weight: 0}]->(d)" +
        ", (e)-[:TYPE1 {weight: 0}]->(f)" +
        ", (f)-[:TYPE1 {weight: 0}]->(b)" +
        ", (f)-[:TYPE1 {weight: 0}]->(e)";

    @GdlGraph(graphNamePrefix = "constantWeight")
    private static final String CONSTANT_WEIGHT_GRAPH =
        NODE_CREATE_QUERY +
        "  (b)-[:TYPE2 {weight: 1}]->(c)" +
        ", (c)-[:TYPE2 {weight: 1}]->(b)" +
        ", (d)-[:TYPE2 {weight: 1}]->(a)" +
        ", (d)-[:TYPE2 {weight: 1}]->(b)" +
        ", (e)-[:TYPE2 {weight: 1}]->(b)" +
        ", (e)-[:TYPE2 {weight: 1}]->(d)" +
        ", (e)-[:TYPE2 {weight: 1}]->(f)" +
        ", (f)-[:TYPE2 {weight: 1}]->(b)" +
        ", (f)-[:TYPE2 {weight: 1}]->(e)";

    @GdlGraph
    private static final String GRAPH =
        NODE_CREATE_QUERY +
        "  (b)-[:TYPE3 {weight: 1.0}]->(c)" +
        ", (c)-[:TYPE3 {weight: 1.0}]->(b)" +
        ", (d)-[:TYPE3 {weight: 0.3}]->(a)" +
        ", (d)-[:TYPE3 {weight: 0.7}]->(b)" +
        ", (e)-[:TYPE3 {weight: 0.9}]->(b)" +
        ", (e)-[:TYPE3 {weight: 0.05}]->(d)" +
        ", (e)-[:TYPE3 {weight: 0.05}]->(f)" +
        ", (f)-[:TYPE3 {weight: 0.9}]->(b)" +
        ", (f)-[:TYPE3 {weight: 0.1}]->(e)";

    @GdlGraph(graphNamePrefix = "negativeWeights")
    private static final String NEGATIVE_WEIGHTS_GRAPH =
        NODE_CREATE_QUERY +
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
    private TestGraph zeroWeightsGraph;

    @Inject
    private TestGraph constantWeightGraph;

    @Inject
    private TestGraph graph;

    @Inject
    private TestGraph negativeWeightsGraph;

    @Test
    void defaultWeightOf0MeansNoDiffusionOfPageRank() {
        var expected = Map.of(
            "a", 0.15,
            "b", 0.15,
            "c", 0.15,
            "d", 0.15,
            "e", 0.15,
            "f", 0.15,
            "g", 0.15,
            "h", 0.15,
            "i", 0.15,
            "j", 0.15
        );

        PageRankTest.assertResult(zeroWeightsGraph, PageRankAlgorithmType.WEIGHTED, expected);
    }

    @Test
    void allWeightsTheSameShouldBeTheSameAsPageRank() {
        var expected = Map.of(
            "a", 0.243007,
            "b", 1.9183995,
            "c", 1.7806315,
            "d", 0.21885,
            "e", 0.243007,
            "f", 0.21885,
            "g", 0.15,
            "h", 0.15,
            "i", 0.15,
            "j", 0.15
        );

        PageRankTest.assertResult(constantWeightGraph, PageRankAlgorithmType.WEIGHTED, expected);
    }

    @Test
    void higherWeightsLeadToHigherPageRank() {
        var expected = Map.of(
            "a", 0.1900095,
            "b", 2.2152279,
            "c", 2.0325884,
            "d", 0.1569275,
            "e", 0.1633280,
            "f", 0.1569275,
            "g", 0.15,
            "h", 0.15,
            "i", 0.15,
            "j", 0.15
        );

        PageRankTest.assertResult(graph, PageRankAlgorithmType.WEIGHTED, expected);
    }

    @Test
    void shouldExcludeNegativeWeights() {
        var expected = Map.of(
            "a", 0.1900095,
            "b", 2.2152279,
            "c", 2.0325884,
            "d", 0.1569275,
            "e", 0.1633280,
            "f", 0.1569275,
            "g", 0.15,
            "h", 0.15,
            "i", 0.15,
            "j", 0.15
        );

        PageRankTest.assertResult(negativeWeightsGraph, PageRankAlgorithmType.WEIGHTED, expected);
    }
}
