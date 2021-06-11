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
package org.neo4j.gds;

import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.nodemodels.NodeClassificationTrainProc;
import org.neo4j.gds.ml.splitting.SplitRelationshipsMutateProc;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.centrality.ClosenessCentralityProc;
import org.neo4j.graphalgo.centrality.HarmonicCentralityProc;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.functions.IsFiniteFunc;
import org.neo4j.graphalgo.functions.OneHotEncodingFunc;
import org.neo4j.graphalgo.influenceΜaximization.CELFProc;
import org.neo4j.graphalgo.influenceΜaximization.GreedyProc;
import org.neo4j.graphalgo.linkprediction.LinkPredictionFunc;
import org.neo4j.graphalgo.scc.SccProc;
import org.neo4j.graphalgo.shortestpath.ShortestPathDeltaSteppingProc;
import org.neo4j.graphalgo.shortestpaths.AllShortestPathsProc;
import org.neo4j.graphalgo.similarity.ApproxNearestNeighborsProc;
import org.neo4j.graphalgo.similarity.CosineProc;
import org.neo4j.graphalgo.similarity.EuclideanProc;
import org.neo4j.graphalgo.similarity.OverlapProc;
import org.neo4j.graphalgo.similarity.PearsonProc;
import org.neo4j.graphalgo.similarity.SimilaritiesFunc;
import org.neo4j.graphalgo.spanningtree.KSpanningTreeProc;
import org.neo4j.graphalgo.spanningtree.SpanningTreeProc;
import org.neo4j.graphalgo.traverse.TraverseProc;
import org.neo4j.graphalgo.triangle.TriangleProc;
import org.neo4j.graphalgo.walking.RandomWalkProc;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class AlphaListProcTest extends BaseProcTest {

    private static final Collection<String> PROCEDURES = new HashSet<>(asList(
        "gds.alpha.allShortestPaths.stream",
        "gds.alpha.bfs.stream",
        "gds.alpha.closeness.write",
        "gds.alpha.closeness.stream",
        "gds.alpha.closeness.harmonic.write",
        "gds.alpha.closeness.harmonic.stream",
        "gds.alpha.dfs.stream",
        "gds.alpha.scc.write",
        "gds.alpha.scc.stream",
        "gds.alpha.shortestPath.deltaStepping.write",
        "gds.alpha.shortestPath.deltaStepping.stream",
        "gds.alpha.randomWalk.stream",
        "gds.alpha.similarity.cosine.write",
        "gds.alpha.similarity.cosine.stream",
        "gds.alpha.similarity.cosine.stats",
        "gds.alpha.similarity.euclidean.write",
        "gds.alpha.similarity.euclidean.stream",
        "gds.alpha.similarity.euclidean.stats",
        "gds.alpha.similarity.overlap.write",
        "gds.alpha.similarity.overlap.stream",
        "gds.alpha.similarity.overlap.stats",
        "gds.alpha.similarity.pearson.write",
        "gds.alpha.similarity.pearson.stream",
        "gds.alpha.similarity.pearson.stats",
        "gds.alpha.spanningTree.write",
        "gds.alpha.spanningTree.kmax.write",
        "gds.alpha.spanningTree.kmin.write",
        "gds.alpha.spanningTree.maximum.write",
        "gds.alpha.spanningTree.minimum.write",
        "gds.alpha.triangles",
        "gds.alpha.ml.ann.write",
        "gds.alpha.ml.ann.stream",
        "gds.alpha.ml.nodeClassification.train",
        "gds.alpha.ml.nodeClassification.train.estimate",
        "gds.alpha.ml.splitRelationships.mutate",
        "gds.alpha.influenceMaximization.greedy.stream",
        "gds.alpha.influenceMaximization.celf.stream"
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
        registerProcedures(
            AllShortestPathsProc.class,
            ApproxNearestNeighborsProc.class,
            ClosenessCentralityProc.class,
            HarmonicCentralityProc.class,
            IsFiniteFunc.class,
            KSpanningTreeProc.class,
            ListProc.class,
            CosineProc.class,
            EuclideanProc.class,
            NodeClassificationTrainProc.class,
            OverlapProc.class,
            PearsonProc.class,
            RandomWalkProc.class,
            OneHotEncodingFunc.class,
            SpanningTreeProc.class,
            SplitRelationshipsMutateProc.class,
            ShortestPathDeltaSteppingProc.class,
            SimilaritiesFunc.class,
            SccProc.class,
            TraverseProc.class,
            TriangleProc.class,
            GreedyProc.class,
            CELFProc.class
        );
        registerFunctions(
            LinkPredictionFunc.class,
            SimilaritiesFunc.class
        );
    }

    @Test
    void shouldListAllThingsExceptTheListProcedure() {
        assertEquals(ALL, listProcs(null));
    }

    @Test
    void listFilteredResult() {
        assertEquals(SPANNING_TREE, listProcs("spanningTree"));
        assertEquals(singleton("gds.alpha.dfs.stream"), listProcs("gds.alpha.dfs.stream"));
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

    @Test
    void allProcsHaveDescriptions() {
        String query = "CALL gds.list()";
        runQueryWithRowConsumer(query, resultRow ->
            assertFalse(resultRow.getString("description").isEmpty(), resultRow.get("name") + " has no description")
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
