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
package org.neo4j.graphalgo.impl;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.QueryRunner;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.msbfs.MSBFSASPAlgorithm;
import org.neo4j.graphalgo.impl.msbfs.MSBFSAllShortestPaths;
import org.neo4j.graphalgo.impl.msbfs.WeightedAllShortestPaths;
import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPathDijkstra;
import org.neo4j.graphalgo.impl.shortestpaths.DijkstraConfig;
import org.neo4j.graphalgo.impl.shortestpaths.ShortestPathDijkstra;
import org.neo4j.graphalgo.impl.util.DoubleAdder;
import org.neo4j.graphalgo.impl.util.DoubleEvaluator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

class WeightedAllShortestPaths427Test extends AlgoTestBase {

    private static final String DB_CYPHER =
            "CREATE" +
            "  (n00:Node)" +
            ", (n01:Node)" +
            ", (n02:Node)" +
            ", (n03:Node)" +
            ", (n04:Node)" +
            ", (n05:Node)" +
            ", (n06:Node)" +
            ", (n07:Node)" +
            ", (n08:Node)" +
            ", (n09:Node)" +
            ", (n10:Node)" +
            ", (n11:Node)" +
            ", (n12:Node)" +
            ", (n13:Node)" +
            ", (n14:Node)" +
            ", (n15:Node)" +
            ", (n16:Node)" +
            ", (n17:Node)" +
            ", (n18:Node)" +
            ", (n19:Node)" +
            ", (n20:Node)" +
            ", (n21:Node)" +
            ", (n22:Node)" +
            ", (n23:Node)" +
            ", (n24:Node)" +
            ", (n25:Node)" +
            ", (n26:Node)" +
            ", (n27:Node)" +
            ", (n28:Node)" +
            ", (n29:Node)" +
            ", (n30:Node)" +
            ", (n31:Node)" +
            ", (n32:Node)" +
            ", (n33:Node)" +
            ", (n34:Node)" +
            ", (n35:Node)" +
            ", (n36:Node)" +
            ", (n37:Node)" +
            ", (n38:Node)" +
            ", (n39:Node)" +
            ", (n40:Node)" +
            ", (n41:Node)" +
            ", (n42:Node)" +
            ", (n43:Node)" +
            ", (n44:Node)" +
            ", (n45:Node)" +
            ", (n46:Node)" +
            ", (n47:Node)" +
            ", (n48:Node)" +
            ", (n49:Node)" +
            ", (n50:Node)" +
            ", (n51:Node)" +
            ", (n52:Node)" +
            ", (n53:Node)" +
            ", (n54:Node)" +
            ", (n55:Node)" +
            ", (n56:Node)" +
            ", (n57:Node)" +
            ", (n58:Node)" +
            ", (n59:Node)" +
            ", (n60:Node)" +
            ", (n61:Node)" +
            ", (n62:Node)" +
            ", (n63:Node)" +
            ", (n64:Node)" +
            ", (n65:Node)" +
            ", (n66:Node)" +
            ", (n67:Node)" +
            ", (n68:Node)" +
            ", (n69:Node)" +
            ", (n70:Node)" +
            ", (n71:Node)" +
            ", (n72:Node)" +
            ", (n73:Node)" +
            ", (n74:Node)" +
            ", (n75:Node)" +
            ", (n76:Node)" +
            ", (n01)-[:LINK {weight:1}]->(n00)" +
            ", (n02)-[:LINK {weight:8}]->(n00)" +
            ", (n03)-[:LINK {weight:0}]->(n00)" +
            ", (n03)-[:LINK {weight:6}]->(n02)" +
            ", (n04)-[:LINK {weight:1}]->(n00)" +
            ", (n05)-[:LINK {weight:1}]->(n00)" +
            ", (n06)-[:LINK {weight:1}]->(n00)" +
            ", (n07)-[:LINK {weight:1}]->(n00)" +
            ", (n08)-[:LINK {weight:2}]->(n00)" +
            ", (n09)-[:LINK {weight:1}]->(n00)" +
            ", (n11)-[:LINK {weight:1}]->(n10)" +
            ", (n11)-[:LINK {weight:3}]->(n02)" +
            ", (n11)-[:LINK {weight:3}]->(n03)" +
            ", (n11)-[:LINK {weight:5}]->(n00)" +
            ", (n12)-[:LINK {weight:1}]->(n11)" +
            ", (n13)-[:LINK {weight:1}]->(n11)" +
            ", (n14)-[:LINK {weight:1}]->(n11)" +
            ", (n15)-[:LINK {weight:1}]->(n11)" +
            ", (n17)-[:LINK {weight:4}]->(n16)" +
            ", (n18)-[:LINK {weight:4}]->(n16)" +
            ", (n18)-[:LINK {weight:4}]->(n17)" +
            ", (n19)-[:LINK {weight:4}]->(n16)" +
            ", (n19)-[:LINK {weight:4}]->(n17)" +
            ", (n19)-[:LINK {weight:4}]->(n18)" +
            ", (n20)-[:LINK {weight:3}]->(n16)" +
            ", (n20)-[:LINK {weight:3}]->(n17)" +
            ", (n20)-[:LINK {weight:3}]->(n18)" +
            ", (n20)-[:LINK {weight:4}]->(n19)" +
            ", (n21)-[:LINK {weight:3}]->(n16)" +
            ", (n21)-[:LINK {weight:3}]->(n17)" +
            ", (n21)-[:LINK {weight:3}]->(n18)" +
            ", (n21)-[:LINK {weight:3}]->(n19)" +
            ", (n21)-[:LINK {weight:5}]->(n20)" +
            ", (n22)-[:LINK {weight:3}]->(n16)" +
            ", (n22)-[:LINK {weight:3}]->(n17)" +
            ", (n22)-[:LINK {weight:3}]->(n18)" +
            ", (n22)-[:LINK {weight:3}]->(n19)" +
            ", (n22)-[:LINK {weight:4}]->(n20)" +
            ", (n22)-[:LINK {weight:4}]->(n21)" +
            ", (n23)-[:LINK {weight:2}]->(n12)" +
            ", (n23)-[:LINK {weight:3}]->(n16)" +
            ", (n23)-[:LINK {weight:3}]->(n17)" +
            ", (n23)-[:LINK {weight:3}]->(n18)" +
            ", (n23)-[:LINK {weight:3}]->(n19)" +
            ", (n23)-[:LINK {weight:4}]->(n20)" +
            ", (n23)-[:LINK {weight:4}]->(n21)" +
            ", (n23)-[:LINK {weight:4}]->(n22)" +
            ", (n23)-[:LINK {weight:9}]->(n11)" +
            ", (n24)-[:LINK {weight:2}]->(n23)" +
            ", (n24)-[:LINK {weight:7}]->(n11)" +
            ", (n25)-[:LINK {weight:1}]->(n23)" +
            ", (n25)-[:LINK {weight:2}]->(n11)" +
            ", (n25)-[:LINK {weight:3}]->(n24)" +
            ", (n26)-[:LINK {weight:1}]->(n11)" +
            ", (n26)-[:LINK {weight:1}]->(n16)" +
            ", (n26)-[:LINK {weight:1}]->(n25)" +
            ", (n26)-[:LINK {weight:4}]->(n24)" +
            ", (n27)-[:LINK {weight:1}]->(n24)" +
            ", (n27)-[:LINK {weight:1}]->(n26)" +
            ", (n27)-[:LINK {weight:5}]->(n23)" +
            ", (n27)-[:LINK {weight:5}]->(n25)" +
            ", (n27)-[:LINK {weight:7}]->(n11)" +
            ", (n28)-[:LINK {weight:1}]->(n27)" +
            ", (n28)-[:LINK {weight:8}]->(n11)" +
            ", (n29)-[:LINK {weight:1}]->(n23)" +
            ", (n29)-[:LINK {weight:1}]->(n27)" +
            ", (n29)-[:LINK {weight:2}]->(n11)" +
            ", (n30)-[:LINK {weight:1}]->(n23)" +
            ", (n31)-[:LINK {weight:1}]->(n27)" +
            ", (n31)-[:LINK {weight:2}]->(n23)" +
            ", (n31)-[:LINK {weight:2}]->(n30)" +
            ", (n31)-[:LINK {weight:3}]->(n11)" +
            ", (n32)-[:LINK {weight:1}]->(n11)" +
            ", (n33)-[:LINK {weight:1}]->(n27)" +
            ", (n33)-[:LINK {weight:2}]->(n11)" +
            ", (n34)-[:LINK {weight:2}]->(n29)" +
            ", (n34)-[:LINK {weight:3}]->(n11)" +
            ", (n35)-[:LINK {weight:2}]->(n29)" +
            ", (n35)-[:LINK {weight:3}]->(n11)" +
            ", (n35)-[:LINK {weight:3}]->(n34)" +
            ", (n36)-[:LINK {weight:1}]->(n29)" +
            ", (n36)-[:LINK {weight:2}]->(n11)" +
            ", (n36)-[:LINK {weight:2}]->(n34)" +
            ", (n36)-[:LINK {weight:2}]->(n35)" +
            ", (n37)-[:LINK {weight:1}]->(n29)" +
            ", (n37)-[:LINK {weight:2}]->(n11)" +
            ", (n37)-[:LINK {weight:2}]->(n34)" +
            ", (n37)-[:LINK {weight:2}]->(n35)" +
            ", (n37)-[:LINK {weight:2}]->(n36)" +
            ", (n38)-[:LINK {weight:1}]->(n29)" +
            ", (n38)-[:LINK {weight:2}]->(n11)" +
            ", (n38)-[:LINK {weight:2}]->(n34)" +
            ", (n38)-[:LINK {weight:2}]->(n35)" +
            ", (n38)-[:LINK {weight:2}]->(n36)" +
            ", (n38)-[:LINK {weight:2}]->(n37)" +
            ", (n39)-[:LINK {weight:1}]->(n25)" +
            ", (n40)-[:LINK {weight:1}]->(n25)" +
            ", (n41)-[:LINK {weight:2}]->(n24)" +
            ", (n41)-[:LINK {weight:3}]->(n25)" +
            ", (n42)-[:LINK {weight:1}]->(n24)" +
            ", (n42)-[:LINK {weight:2}]->(n25)" +
            ", (n42)-[:LINK {weight:2}]->(n41)" +
            ", (n43)-[:LINK {weight:1}]->(n26)" +
            ", (n43)-[:LINK {weight:1}]->(n27)" +
            ", (n43)-[:LINK {weight:3}]->(n11)" +
            ", (n44)-[:LINK {weight:1}]->(n11)" +
            ", (n44)-[:LINK {weight:3}]->(n28)" +
            ", (n45)-[:LINK {weight:2}]->(n28)" +
            ", (n47)-[:LINK {weight:1}]->(n46)" +
            ", (n48)-[:LINK {weight:1}]->(n11)" +
            ", (n48)-[:LINK {weight:1}]->(n25)" +
            ", (n48)-[:LINK {weight:1}]->(n27)" +
            ", (n48)-[:LINK {weight:2}]->(n47)" +
            ", (n49)-[:LINK {weight:2}]->(n11)" +
            ", (n49)-[:LINK {weight:3}]->(n26)" +
            ", (n50)-[:LINK {weight:1}]->(n24)" +
            ", (n50)-[:LINK {weight:1}]->(n49)" +
            ", (n51)-[:LINK {weight:2}]->(n11)" +
            ", (n51)-[:LINK {weight:2}]->(n26)" +
            ", (n51)-[:LINK {weight:9}]->(n49)" +
            ", (n52)-[:LINK {weight:1}]->(n39)" +
            ", (n52)-[:LINK {weight:1}]->(n51)" +
            ", (n53)-[:LINK {weight:1}]->(n51)" +
            ", (n54)-[:LINK {weight:1}]->(n26)" +
            ", (n54)-[:LINK {weight:1}]->(n49)" +
            ", (n54)-[:LINK {weight:2}]->(n51)" +
            ", (n55)-[:LINK {weight:1}]->(n16)" +
            ", (n55)-[:LINK {weight:1}]->(n26)" +
            ", (n55)-[:LINK {weight:1}]->(n39)" +
            ", (n55)-[:LINK {weight:1}]->(n54)" +
            ", (n55)-[:LINK {weight:2}]->(n25)" +
            ", (n55)-[:LINK {weight:2}]->(n49)" +
            ", (n55)-[:LINK {weight:4}]->(n48)" +
            ", (n55)-[:LINK {weight:5}]->(n41)" +
            ", (n55)-[:LINK {weight:6}]->(n51)" +
            ", (n55)-[:LINK {weight:9}]->(n11)" +
            ", (n56)-[:LINK {weight:1}]->(n49)" +
            ", (n56)-[:LINK {weight:1}]->(n55)" +
            ", (n57)-[:LINK {weight:1}]->(n41)" +
            ", (n57)-[:LINK {weight:1}]->(n48)" +
            ", (n57)-[:LINK {weight:1}]->(n55)" +
            ", (n58)-[:LINK {weight:1}]->(n57)" +
            ", (n58)-[:LINK {weight:4}]->(n11)" +
            ", (n58)-[:LINK {weight:6}]->(n27)" +
            ", (n58)-[:LINK {weight:7}]->(n48)" +
            ", (n58)-[:LINK {weight:7}]->(n55)" +
            ", (n59)-[:LINK {weight:2}]->(n57)" +
            ", (n59)-[:LINK {weight:5}]->(n55)" +
            ", (n59)-[:LINK {weight:5}]->(n58)" +
            ", (n59)-[:LINK {weight:6}]->(n48)" +
            ", (n60)-[:LINK {weight:1}]->(n48)" +
            ", (n60)-[:LINK {weight:2}]->(n59)" +
            ", (n60)-[:LINK {weight:4}]->(n58)" +
            ", (n61)-[:LINK {weight:1}]->(n55)" +
            ", (n61)-[:LINK {weight:1}]->(n57)" +
            ", (n61)-[:LINK {weight:2}]->(n48)" +
            ", (n61)-[:LINK {weight:2}]->(n60)" +
            ", (n61)-[:LINK {weight:5}]->(n59)" +
            ", (n61)-[:LINK {weight:6}]->(n58)" +
            ", (n62)-[:LINK {weight:1}]->(n41)" +
            ", (n62)-[:LINK {weight:2}]->(n57)" +
            ", (n62)-[:LINK {weight:3}]->(n59)" +
            ", (n62)-[:LINK {weight:3}]->(n60)" +
            ", (n62)-[:LINK {weight:6}]->(n61)" +
            ", (n62)-[:LINK {weight:7}]->(n48)" +
            ", (n62)-[:LINK {weight:7}]->(n58)" +
            ", (n62)-[:LINK {weight:9}]->(n55)" +
            ", (n63)-[:LINK {weight:1}]->(n55)" +
            ", (n63)-[:LINK {weight:2}]->(n57)" +
            ", (n63)-[:LINK {weight:2}]->(n60)" +
            ", (n63)-[:LINK {weight:3}]->(n61)" +
            ", (n63)-[:LINK {weight:4}]->(n58)" +
            ", (n63)-[:LINK {weight:5}]->(n48)" +
            ", (n63)-[:LINK {weight:5}]->(n59)" +
            ", (n63)-[:LINK {weight:6}]->(n62)" +
            ", (n64)-[:LINK {weight:0}]->(n58)" +
            ", (n64)-[:LINK {weight:1}]->(n11)" +
            ", (n64)-[:LINK {weight:1}]->(n57)" +
            ", (n64)-[:LINK {weight:2}]->(n60)" +
            ", (n64)-[:LINK {weight:2}]->(n62)" +
            ", (n64)-[:LINK {weight:4}]->(n63)" +
            ", (n64)-[:LINK {weight:5}]->(n48)" +
            ", (n64)-[:LINK {weight:5}]->(n55)" +
            ", (n64)-[:LINK {weight:6}]->(n61)" +
            ", (n64)-[:LINK {weight:9}]->(n59)" +
            ", (n65)-[:LINK {weight:1}]->(n57)" +
            ", (n65)-[:LINK {weight:2}]->(n55)" +
            ", (n65)-[:LINK {weight:2}]->(n60)" +
            ", (n65)-[:LINK {weight:3}]->(n48)" +
            ", (n65)-[:LINK {weight:5}]->(n58)" +
            ", (n65)-[:LINK {weight:5}]->(n59)" +
            ", (n65)-[:LINK {weight:5}]->(n61)" +
            ", (n65)-[:LINK {weight:5}]->(n62)" +
            ", (n65)-[:LINK {weight:5}]->(n63)" +
            ", (n65)-[:LINK {weight:7}]->(n64)" +
            ", (n66)-[:LINK {weight:1}]->(n48)" +
            ", (n66)-[:LINK {weight:1}]->(n59)" +
            ", (n66)-[:LINK {weight:1}]->(n60)" +
            ", (n66)-[:LINK {weight:1}]->(n61)" +
            ", (n66)-[:LINK {weight:1}]->(n63)" +
            ", (n66)-[:LINK {weight:2}]->(n62)" +
            ", (n66)-[:LINK {weight:2}]->(n65)" +
            ", (n66)-[:LINK {weight:3}]->(n58)" +
            ", (n66)-[:LINK {weight:3}]->(n64)" +
            ", (n67)-[:LINK {weight:3}]->(n57)" +
            ", (n68)-[:LINK {weight:1}]->(n11)" +
            ", (n68)-[:LINK {weight:1}]->(n24)" +
            ", (n68)-[:LINK {weight:1}]->(n27)" +
            ", (n68)-[:LINK {weight:1}]->(n41)" +
            ", (n68)-[:LINK {weight:1}]->(n48)" +
            ", (n68)-[:LINK {weight:5}]->(n25)" +
            ", (n69)-[:LINK {weight:1}]->(n11)" +
            ", (n69)-[:LINK {weight:1}]->(n24)" +
            ", (n69)-[:LINK {weight:1}]->(n41)" +
            ", (n69)-[:LINK {weight:1}]->(n48)" +
            ", (n69)-[:LINK {weight:2}]->(n27)" +
            ", (n69)-[:LINK {weight:6}]->(n25)" +
            ", (n69)-[:LINK {weight:6}]->(n68)" +
            ", (n70)-[:LINK {weight:1}]->(n11)" +
            ", (n70)-[:LINK {weight:1}]->(n24)" +
            ", (n70)-[:LINK {weight:1}]->(n27)" +
            ", (n70)-[:LINK {weight:1}]->(n41)" +
            ", (n70)-[:LINK {weight:1}]->(n58)" +
            ", (n70)-[:LINK {weight:4}]->(n25)" +
            ", (n70)-[:LINK {weight:4}]->(n68)" +
            ", (n70)-[:LINK {weight:4}]->(n69)" +
            ", (n71)-[:LINK {weight:1}]->(n11)" +
            ", (n71)-[:LINK {weight:1}]->(n25)" +
            ", (n71)-[:LINK {weight:1}]->(n27)" +
            ", (n71)-[:LINK {weight:1}]->(n41)" +
            ", (n71)-[:LINK {weight:1}]->(n48)" +
            ", (n71)-[:LINK {weight:2}]->(n68)" +
            ", (n71)-[:LINK {weight:2}]->(n69)" +
            ", (n71)-[:LINK {weight:2}]->(n70)" +
            ", (n72)-[:LINK {weight:1}]->(n11)" +
            ", (n72)-[:LINK {weight:1}]->(n27)" +
            ", (n72)-[:LINK {weight:2}]->(n26)" +
            ", (n73)-[:LINK {weight:2}]->(n48)" +
            ", (n74)-[:LINK {weight:2}]->(n48)" +
            ", (n74)-[:LINK {weight:3}]->(n73)" +
            ", (n75)-[:LINK {weight:1}]->(n41)" +
            ", (n75)-[:LINK {weight:1}]->(n48)" +
            ", (n75)-[:LINK {weight:1}]->(n70)" +
            ", (n75)-[:LINK {weight:1}]->(n71)" +
            ", (n75)-[:LINK {weight:3}]->(n25)" +
            ", (n75)-[:LINK {weight:3}]->(n68)" +
            ", (n75)-[:LINK {weight:3}]->(n69)" +
            ", (n76)-[:LINK {weight:1}]->(n48)" +
            ", (n76)-[:LINK {weight:1}]->(n58)" +
            ", (n76)-[:LINK {weight:1}]->(n62)" +
            ", (n76)-[:LINK {weight:1}]->(n63)" +
            ", (n76)-[:LINK {weight:1}]->(n64)" +
            ", (n76)-[:LINK {weight:1}]->(n65)" +
            ", (n76)-[:LINK {weight:1}]->(n66)";

    @BeforeEach
    void setupGraph() {
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void teardownGraph() {
        db.shutdown();
    }

    @Test
    void testWeighted() {
        Graph graph = new StoreLoaderBuilder()
            .api(db)
            .addNodeLabel("Node")
            .addRelationshipType("LINK")
            .addRelationshipProperty(PropertyMapping.of("weight", 1.0))
            .build()
            .load(HugeGraphFactory.class);
        List<Result> expected = calculateExpected(graph, true);
        WeightedAllShortestPaths shortestPaths = new WeightedAllShortestPaths(
                graph,
                Pools.DEFAULT,
                Pools.DEFAULT_CONCURRENCY);
        compare(shortestPaths, expected);
    }

    @Test
    void testMsbfs() {
        Graph graph = new StoreLoaderBuilder()
            .api(db)
            .addNodeLabel("Node")
            .addRelationshipType("LINK")
            .build()
            .load(HugeGraphFactory.class);
        List<Result> expectedNonWeighted = calculateExpected(graph, false);
        MSBFSAllShortestPaths shortestPaths = new MSBFSAllShortestPaths(
                graph,
                AllocationTracker.EMPTY,
                Pools.DEFAULT_CONCURRENCY,
                Pools.DEFAULT);
        compare(shortestPaths, expectedNonWeighted);
    }

    private List<Result> calculateExpected(Graph graph, boolean withWeights) {
        List<Result> expected = new ArrayList<>();
        List<Executable> assertions = new ArrayList<>();
        QueryRunner.runInTransaction(
            db,
            () -> graph.forEachNode(algoSourceId -> {
                long neoSourceId = graph.toOriginalNodeId(algoSourceId);
                TestDijkstra dijkstra = new TestDijkstra(db.getNodeById(neoSourceId), withWeights);
                graph.forEachNode(algoTargetId -> {
                    if (algoSourceId != algoTargetId) {
                        Result neoResult = null;
                        Result algoResult = null;

                        dijkstra.reset();
                        long neoTargetId = graph.toOriginalNodeId(algoTargetId);
                        Node targetNode = db.getNodeById(neoTargetId);
                        List<Node> path = dijkstra.getPathAsNodes(targetNode);

                        if (path != null) {
                            Double cost = dijkstra.getCalculatedCost(targetNode);
                            long[] pathIds = path.stream()
                                .mapToLong(Node::getId)
                                .toArray();
                            neoResult = new Result(neoSourceId, neoTargetId, cost, pathIds);
                            expected.add(neoResult);
                        }

                        if (withWeights) {
                            DijkstraConfig config = DijkstraConfig.of(neoSourceId, neoTargetId);
                            ShortestPathDijkstra spd = new ShortestPathDijkstra(graph, config);
                            spd.compute(neoSourceId, neoTargetId, Direction.OUTGOING);
                            double totalCost = spd.getTotalCost();
                            if (totalCost != ShortestPathDijkstra.NO_PATH_FOUND) {
                                long[] pathIds = Arrays.stream(spd.getFinalPath().toArray())
                                    .mapToLong(graph::toOriginalNodeId)
                                    .toArray();
                                algoResult = new Result(neoSourceId, neoTargetId, totalCost, pathIds);
                            }

                            final Result expect = neoResult;
                            final Result actual = algoResult;
                            assertions.add(
                                () -> assertEquals(
                                    expect,
                                    actual,
                                    String.format("Neo vs Algo (%d)-[*]->(%d)", neoSourceId, neoTargetId)
                                )
                            );
                        }
                    }
                    return true;
                });
                return true;
            })
        );
        assertAll(assertions);

        expected.sort(Comparator.naturalOrder());
        return expected;
    }

    private void compare(MSBFSASPAlgorithm asp, List<Result> expected) {
        List<Result> results = asp.compute()
            .filter(r -> r.sourceNodeId != r.targetNodeId)
            .map(r -> new Result(
                r.sourceNodeId,
                r.targetNodeId,
                r.distance,
                null
            ))
            .sorted()
            .collect(toList());

        assertAll(
            IntStream.range(0, expected.size())
                .mapToObj((i) -> {
                    Result expect = expected.get(i);
                    Result actual = results.get(i);
                    return () -> assertEquals(
                        expect,
                        actual,
                        String.format("Neo vs wASP (%d)-[*]->(%d)", expect.source, expect.target)
                    );
                })
        );
    }

    private static final class TestDijkstra extends SingleSourceShortestPathDijkstra<Double> {

        private static final CostEvaluator<Double> WEIGHT = new DoubleEvaluator("weight");

        TestDijkstra(Node startNode, final boolean withWeights) {
            super(
                    0.0D,
                    startNode,
                    withWeights ? WEIGHT : (relationship, direction) -> 1.0D,
                    new DoubleAdder(),
                    Comparator.comparingDouble(Double::doubleValue),
                    Direction.OUTGOING,
                    RelationshipType.withName("LINK")
            );
        }

        Double getCalculatedCost(Node target) {
            return distances.get(target);
        }
    }

    private static final class Result implements Comparable<Result> {

        private final long source;
        private final long target;
        private final double distance;
        private final long[] path;

        private Result(long source, long target, double distance, long[] path) {
            this.source = source;
            this.target = target;
            this.distance = distance;
            this.path = path;
        }

        @Override
        public String toString() {
            return "Result{" +
                   "source=" + source +
                   ", target=" + target +
                   ", distance=" + distance +
                   ", path=" + Arrays.toString(path) +
                   '}';
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Result result = (Result) o;
            return source == result.source &&
                   target == result.target &&
                   Double.compare(result.distance, distance) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(source, target, distance);
        }

        @Override
        public int compareTo(final Result o) {
            int res = Long.compare(source, o.source);
            if (res == 0) {
                res = Long.compare(target, o.target);
            }
            if (res == 0) {
                res = Double.compare(distance, o.distance);
            }
            return res;
        }
    }
}
