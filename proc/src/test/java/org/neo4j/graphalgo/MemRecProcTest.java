/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphalgo.louvain.LouvainStreamProc;
import org.neo4j.graphalgo.louvain.LouvainWriteProc;
import org.neo4j.graphalgo.wcc.WccStreamProc;
import org.neo4j.graphalgo.wcc.WccWriteProc;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphalgo.compat.MapUtil.map;

class MemRecProcTest extends ProcTestBase {

    private final String availableAlgoProcedures = String.format(
        "the available and supported procedures are {%s}.",
        String.join(
            ", ",
            "algo.beta.k1coloring",
            "algo.beta.modularityOptimization",
            "algo.graph.load",
            "algo.labelPropagation",
            "algo.pageRank",
            "gds.algo.louvain.stats",
            "gds.algo.louvain.stream",
            "gds.algo.louvain.write",
            "gds.algo.wcc.stats",
            "gds.algo.wcc.stream",
            "gds.algo.wcc.write"
        )
    );

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:A {class: 42.0})" +
        ", (b:B {class: 42.0})" +

        ", (a)-[:X { weight: 1.0 }]->(:A {class: 42.0})" +
        ", (a)-[:X { weight: 1.0 }]->(:A {class: 42.0})" +
        ", (a)-[:X { weight: 1.0 }]->(:A {class: 42.0})" +
        ", (a)-[:Y { weight: 1.0 }]->(:A {class: 42.0})" +
        ", (a)-[:Z { weight: 1.0 }]->(:A {class: 42.0})";

    private static final long NODE_COUNT = 7L;
    private static final long RELATIONSHIP_COUNT = 5L;

    @BeforeEach
    void setUp() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(
                GraphLoadProc.class,
                MemRecProc.class,
                PageRankProc.class,
                LabelPropagationProc.class,
                WccStreamProc.class,
                WccWriteProc.class,
                LouvainWriteProc.class,
                LouvainStreamProc.class,
                K1ColoringProc.class,
                ModularityOptimizationProc.class
        );

        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void memrecProcedure() {
        test(
                "algo.memrec(null, null, null, {})",
                "Missing procedure parameter, " + availableAlgoProcedures);
        test(
                "algo.memrec(null, null, 'doesNotExist', {direction: 'BOTH', graph: 'huge'})",
                "The procedure [doesNotExist] does not support memrec or does not exist, " + availableAlgoProcedures);

        test("algo.memrec(null, null, 'graph.load')");
        test("algo.graph.load.memrec(null, null)");

        test("algo.memrec(null, null, 'pageRank')");
        test("algo.memrec(null, null, 'pageRank', {direction: 'BOTH', graph: 'huge'})");
        test("algo.pageRank.memrec(null, null, {direction: 'BOTH', graph: 'huge'})");
        test("algo.pageRank.memrec(null, null, { graph: 'huge'})");

        test("algo.memrec(null, null, 'labelPropagation')");
        test("algo.memrec(null, null, 'labelPropagation', {direction: 'BOTH', graph: 'huge'})");
        test("algo.labelPropagation.memrec(null, null)");
        test("algo.labelPropagation.memrec(null, null, {direction: 'BOTH', graph: 'huge'})");

        test("gds.algo.louvain.write.estimate({writeProperty: 'foo'})");
        test("gds.algo.louvain.stream.estimate({})");

        test("gds.algo.wcc.write.estimate({writeProperty: 'foo'})");
        test("gds.algo.wcc.stream.estimate({})");

        test("algo.memrec(null, null, 'beta.k1coloring')");
        test("algo.memrec(null, null, 'beta.k1coloring', {direction: 'BOTH', graph: 'huge'})");
        test("algo.beta.k1coloring.memrec(null, null)");
        test("algo.beta.k1coloring.memrec(null, null, {direction: 'BOTH', graph: 'huge'})");

        test("algo.memrec(null, null, 'beta.modularityOptimization')");
        test("algo.memrec(null, null, 'beta.modularityOptimization', {direction: 'BOTH', graph: 'huge'})");
        test("algo.beta.modularityOptimization.memrec(null, null)");
        test("algo.beta.modularityOptimization.memrec(null, null, {direction: 'BOTH', graph: 'huge'})");
    }

    private void test(final String s, final String expectedMessage) {
        test(s, Optional.ofNullable(expectedMessage));
    }

    private void test(final String s) {
        test(s, Optional.empty());
    }

    private void test(final String s, final Optional<String> expectedMessage) {
        String queryTemplate = "CALL %s YIELD nodeCount, relationshipCount, requiredMemory, bytesMin, bytesMax RETURN nodeCount, relationshipCount, requiredMemory, bytesMin, bytesMax";
        String query = String.format(queryTemplate, s);

        try {
            runQuery(query).resultAsString();
            expectedMessage.ifPresent(value -> fail("Call should have failed with " + value));
        } catch (QueryExecutionException e) {
            if (expectedMessage.isPresent()) {
                assertEquals(expectedMessage.get(), ExceptionUtil.rootCause(e).getMessage());
            } else {
                fail(e.getMessage());
            }
        }
    }

    private void testAgainstLoadedGraph(String algoName,Map<String, Object> parameters) {
        GraphCatalog.removeAllLoadedGraphs();

        parameters.putIfAbsent("relationshipProperties", null);
        parameters.putIfAbsent("nodeProperties", null);
        parameters.put("algo", algoName);

        if (algoName.equals("graph.load")) {
            parameters.put("graph", null);
        }
        else {
            String loadGraphQuery = "CALL algo.graph.load('foo', '', '', {nodeProperty: $nodeProperties, relationshipProperties: $relationshipProperties})";
            parameters.put("graph", "foo");
            runQuery(loadGraphQuery, parameters);
        }

        String loadedMemRecQuery = "CALL algo.memrec('', '', $algo, {" +
                                   "graph: $graph, relationshipProperties: $relationshipProperties, nodeProperties: $nodeProperties  " +
                                   "})" +
                                   " YIELD nodeCount, relationshipCount, requiredMemory, bytesMin, bytesMax";

        Map<String, Object> memRecOnLoadedGraph = runQuery(loadedMemRecQuery, parameters).next();

        parameters.put("nodeCount", NODE_COUNT);
        parameters.put("relationshipCount", RELATIONSHIP_COUNT);

        String nonExistingGraph = "CALL algo.memrec('', '', $algo, {" +
                                  " nodeCount: $nodeCount, relationshipCount: $relationshipCount, " +
                                  " relationshipProperties: $relationshipProperties, nodeProperties: $nodeProperties" +
                                  " }) " +
                                  "YIELD nodeCount, relationshipCount, requiredMemory, bytesMin, bytesMax";
        Map<String, Object> memRecOnStats = runQuery(nonExistingGraph, parameters).next();

        assertEquals(memRecOnLoadedGraph, memRecOnStats);
    }

    @Test
    void memRecOnGraphStats() {
        testAgainstLoadedGraph("graph.load", map());
        testAgainstLoadedGraph("graph.load", map("relationshipProperties", "weight"));
        testAgainstLoadedGraph("graph.load", map("relationshipProperties", "weight", "nodeProperties", "class"));
        test("algo.memrec(null, null, 'pageRank', {graph: 'myGraph', nodeCount: 10})", "Unknown impl: myGraph");
        test("algo.memrec(null, null, 'pageRank', {graph: 'cypher', nodeCount: 10})");
        test("algo.memrec(null, null, 'pageRank', {graph: 'huge', nodeCount: 10})");
    }
}
