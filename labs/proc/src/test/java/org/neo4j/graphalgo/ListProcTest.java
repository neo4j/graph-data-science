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
package org.neo4j.graphalgo;

import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.centrality.ArticleRankProc;
import org.neo4j.graphalgo.centrality.BetweennessCentralityProc;
import org.neo4j.graphalgo.centrality.ClosenessCentralityProc;
import org.neo4j.graphalgo.centrality.DegreeCentralityProc;
import org.neo4j.graphalgo.centrality.SampledBetweennessCentralityProc;
import org.neo4j.graphalgo.centrality.eigenvector.EigenvectorCentralityProc;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.linkprediction.LinkPredictionFunc;
import org.neo4j.graphalgo.shortestpath.DijkstraProc;
import org.neo4j.graphalgo.shortestpath.KShortestPathsProc;
import org.neo4j.graphalgo.similarity.ApproxNearestNeighborsProc;
import org.neo4j.graphalgo.similarity.CosineProc;
import org.neo4j.graphalgo.similarity.EuclideanProc;
import org.neo4j.graphalgo.similarity.OverlapProc;
import org.neo4j.graphalgo.similarity.PearsonProc;
import org.neo4j.graphalgo.similarity.SimilaritiesFunc;
import org.neo4j.graphalgo.spanningtree.KSpanningTreeProc;
import org.neo4j.graphalgo.spanningtree.SpanningTreeProc;
import org.neo4j.graphalgo.traverse.TraverseProc;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ListProcTest extends BaseProcTest {

    private static final Collection<String> PROCEDURES = new HashSet<>(asList(
        "algo.allShortestPaths.stream",
        "gds.alpha.articleRank.write",
        "gds.alpha.articleRank.stream",
        "algo.asPath",
        "gds.alpha.balancedTriads.write",
        "gds.alpha.balancedTriads.stream",
        "gds.alpha.betweenness.write",
        "gds.alpha.betweenness.stream",
        "gds.alpha.betweenness.sampled.write",
        "gds.alpha.betweenness.sampled.stream",
        "gds.alpha.bfs.stream",
        "gds.alpha.closeness.write",
        "gds.alpha.closeness.stream",
        "gds.alpha.degree.write",
        "gds.alpha.degree.stream",
        "gds.alpha.dfs.stream",
        "gds.alpha.scc.write",
        "gds.alpha.scc.stream",
        "gds.alpha.shortestPath.write",
        "gds.alpha.shortestPath.stream",
        "gds.alpha.shortestPaths.write",
        "gds.alpha.shortestPaths.stream",
        "gds.alpha.similarity.cosine.write",
        "gds.alpha.similarity.cosine.stream",
        "gds.alpha.similarity.euclidean.write",
        "gds.alpha.similarity.euclidean.stream",
        "gds.alpha.similarity.overlap.write",
        "gds.alpha.similarity.overlap.stream",
        "gds.alpha.similarity.pearson.write",
        "gds.alpha.similarity.pearson.stream",
        "gds.alpha.spanningTree.write",
        "gds.alpha.spanningTree.kmax.write",
        "gds.alpha.spanningTree.kmin.write",
        "gds.alpha.spanningTree.maximum.write",
        "gds.alpha.spanningTree.minimum.write",
        "gds.alpha.triangle.stream",
        "gds.alpha.triangleCount.write",
        "gds.alpha.triangleCount.stream",
        "gds.alpha.eigenvector.write",
        "gds.alpha.eigenvector.stream",
        "gds.alpha.kShortestPaths.write",
        "gds.alpha.kShortestPaths.stream",
        "gds.alpha.ml.ann.write",
        "gds.alpha.ml.ann.stream",
        "algo.randomWalk.stream",
        "algo.shortestPath.astar.stream",
        "algo.shortestPath.deltaStepping",
        "algo.shortestPath.deltaStepping.stream"
    ));

    private static final Collection<String> FUNCTIONS = new HashSet<>(asList(
        "gds.alpha.linkprediction.adamicAdar",
        "gds.alpha.linkprediction.resourceAllocation",
        "gds.alpha.linkprediction.commonNeighbors",
        "gds.alpha.linkprediction.preferentialAttachment",
        "gds.alpha.linkprediction.totalNeighbors",
        "gds.alpha.linkprediction.sameCommunity",
        "gds.alpha.similarity.cosine",
        "gds.alpha.similarity.euclidean",
        "gds.alpha.similarity.euclideanDistance",
        "gds.alpha.similarity.jaccard",
        "gds.alpha.similarity.overlap",
        "gds.alpha.similarity.pearson"
    ));

    private static final Set<String> SPANNING_TREE = new HashSet<>(asList(
        "gds.alpha.spanningTree.write",
        "gds.alpha.spanningTree.kmax.write",
        "gds.alpha.spanningTree.kmin.write",
        "gds.alpha.spanningTree.maximum.write",
        "gds.alpha.spanningTree.minimum.write"
    ));

    private static final Set<String> ALL = Stream.concat(PROCEDURES.stream(), FUNCTIONS.stream()).collect(Collectors.toSet());

    @BeforeEach
    void setUp() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(
            AllShortestPathsProc.class,
            ApproxNearestNeighborsProc.class,
            ArticleRankProc.class,
            BalancedTriadsProc.class,
            BetweennessCentralityProc.class,
            ClosenessCentralityProc.class,
            DegreeCentralityProc.class,
            DijkstraProc.class,
            EigenvectorCentralityProc.class,
            IsFiniteFunc.class,
            KShortestPathsProc.class,
            KSpanningTreeProc.class,
            ListProc.class,
            CosineProc.class,
            EuclideanProc.class,
            OverlapProc.class,
            PearsonProc.class,
            NodeWalkerProc.class,
            OneHotEncodingFunc.class,
            SpanningTreeProc.class,
            SampledBetweennessCentralityProc.class,
            ShortestPathDeltaSteppingProc.class,
            ShortestPathProc.class,
            ShortestPathsProc.class,
            SimilaritiesFunc.class,
            SimilarityProc.class,
            SccProc.class,
            TraverseProc.class,
            TriangleCountProc.class,
            TriangleProc.class,
            UtilityProc.class
        );
        registerFunctions(
            LinkPredictionFunc.class,
            SimilaritiesFunc.class
        );
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void shouldListAllThingsExceptTheListProcedure() {
        assertEquals(ALL, listProcs(null));
    }

    @Test
    void listFilteredResult() {
        assertEquals(SPANNING_TREE, listProcs("spanningTree"));
        assertEquals(singleton("gds.alpha.articleRank.stream"), listProcs("gds.alpha.articleRank.stream"));
        assertEquals(emptySet(), listProcs("foo"));
    }

    @Test
    void listFunctions() {
        Set<String> actual = listProcs("adamicAdar");
        actual.addAll(listProcs("linkprediction"));
        Set<String> similarity = listProcs("similarity");
        actual.addAll(similarity.stream()
            .filter(name -> !name.matches(".*(stream|write|stats)$")).collect(
            Collectors.toList()));
        assertEquals(FUNCTIONS, actual);
    }

    @Test
    void listEmpty() {
        String query = "CALL gds.list()";
        assertEquals(
            ALL,
            runQuery(query, result -> result
                .<String>columnAs("name")
                .stream()
                .collect(Collectors.toSet())
            )
        );
    }

    private Set<String> listProcs(@Nullable Object name) {
        String query = "CALL gds.list($name)";
        return runQuery(query, MapUtil.map("name", name), result -> result
            .<String > columnAs("name")
            .stream()
            .collect(Collectors.toSet())
        );
    }
}
