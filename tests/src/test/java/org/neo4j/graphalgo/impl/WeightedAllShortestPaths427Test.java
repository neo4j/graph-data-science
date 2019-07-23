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
package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPathDijkstra;
import org.neo4j.graphalgo.impl.util.DoubleAdder;
import org.neo4j.graphalgo.impl.util.DoubleEvaluator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.is;

@RunWith(Parameterized.class)
public class WeightedAllShortestPaths427Test {

    private static final String GRAPH =
            "CREATE (n00:Node)\n" +
                    ",(n01:Node)\n" +
                    ",(n02:Node)\n" +
                    ",(n03:Node)\n" +
                    ",(n04:Node)\n" +
                    ",(n05:Node)\n" +
                    ",(n06:Node)\n" +
                    ",(n07:Node)\n" +
                    ",(n08:Node)\n" +
                    ",(n09:Node)\n" +
                    ",(n10:Node)\n" +
                    ",(n11:Node)\n" +
                    ",(n12:Node)\n" +
                    ",(n13:Node)\n" +
                    ",(n14:Node)\n" +
                    ",(n15:Node)\n" +
                    ",(n16:Node)\n" +
                    ",(n17:Node)\n" +
                    ",(n18:Node)\n" +
                    ",(n19:Node)\n" +
                    ",(n20:Node)\n" +
                    ",(n21:Node)\n" +
                    ",(n22:Node)\n" +
                    ",(n23:Node)\n" +
                    ",(n24:Node)\n" +
                    ",(n25:Node)\n" +
                    ",(n26:Node)\n" +
                    ",(n27:Node)\n" +
                    ",(n28:Node)\n" +
                    ",(n29:Node)\n" +
                    ",(n30:Node)\n" +
                    ",(n31:Node)\n" +
                    ",(n32:Node)\n" +
                    ",(n33:Node)\n" +
                    ",(n34:Node)\n" +
                    ",(n35:Node)\n" +
                    ",(n36:Node)\n" +
                    ",(n37:Node)\n" +
                    ",(n38:Node)\n" +
                    ",(n39:Node)\n" +
                    ",(n40:Node)\n" +
                    ",(n41:Node)\n" +
                    ",(n42:Node)\n" +
                    ",(n43:Node)\n" +
                    ",(n44:Node)\n" +
                    ",(n45:Node)\n" +
                    ",(n46:Node)\n" +
                    ",(n47:Node)\n" +
                    ",(n48:Node)\n" +
                    ",(n49:Node)\n" +
                    ",(n50:Node)\n" +
                    ",(n51:Node)\n" +
                    ",(n52:Node)\n" +
                    ",(n53:Node)\n" +
                    ",(n54:Node)\n" +
                    ",(n55:Node)\n" +
                    ",(n56:Node)\n" +
                    ",(n57:Node)\n" +
                    ",(n58:Node)\n" +
                    ",(n59:Node)\n" +
                    ",(n60:Node)\n" +
                    ",(n61:Node)\n" +
                    ",(n62:Node)\n" +
                    ",(n63:Node)\n" +
                    ",(n64:Node)\n" +
                    ",(n65:Node)\n" +
                    ",(n66:Node)\n" +
                    ",(n67:Node)\n" +
                    ",(n68:Node)\n" +
                    ",(n69:Node)\n" +
                    ",(n70:Node)\n" +
                    ",(n71:Node)\n" +
                    ",(n72:Node)\n" +
                    ",(n73:Node)\n" +
                    ",(n74:Node)\n" +
                    ",(n75:Node)\n" +
                    ",(n76:Node)\n" +
                    "CREATE (n01)-[:LINK {weight:1}]->(n00)\n" +
                    ",(n02)-[:LINK {weight:8}]->(n00)\n" +
                    ",(n03)-[:LINK {weight:0}]->(n00)\n" +
                    ",(n03)-[:LINK {weight:6}]->(n02)\n" +
                    ",(n04)-[:LINK {weight:1}]->(n00)\n" +
                    ",(n05)-[:LINK {weight:1}]->(n00)\n" +
                    ",(n06)-[:LINK {weight:1}]->(n00)\n" +
                    ",(n07)-[:LINK {weight:1}]->(n00)\n" +
                    ",(n08)-[:LINK {weight:2}]->(n00)\n" +
                    ",(n09)-[:LINK {weight:1}]->(n00)\n" +
                    ",(n11)-[:LINK {weight:1}]->(n10)\n" +
                    ",(n11)-[:LINK {weight:3}]->(n02)\n" +
                    ",(n11)-[:LINK {weight:3}]->(n03)\n" +
                    ",(n11)-[:LINK {weight:5}]->(n00)\n" +
                    ",(n12)-[:LINK {weight:1}]->(n11)\n" +
                    ",(n13)-[:LINK {weight:1}]->(n11)\n" +
                    ",(n14)-[:LINK {weight:1}]->(n11)\n" +
                    ",(n15)-[:LINK {weight:1}]->(n11)\n" +
                    ",(n17)-[:LINK {weight:4}]->(n16)\n" +
                    ",(n18)-[:LINK {weight:4}]->(n16)\n" +
                    ",(n18)-[:LINK {weight:4}]->(n17)\n" +
                    ",(n19)-[:LINK {weight:4}]->(n16)\n" +
                    ",(n19)-[:LINK {weight:4}]->(n17)\n" +
                    ",(n19)-[:LINK {weight:4}]->(n18)\n" +
                    ",(n20)-[:LINK {weight:3}]->(n16)\n" +
                    ",(n20)-[:LINK {weight:3}]->(n17)\n" +
                    ",(n20)-[:LINK {weight:3}]->(n18)\n" +
                    ",(n20)-[:LINK {weight:4}]->(n19)\n" +
                    ",(n21)-[:LINK {weight:3}]->(n16)\n" +
                    ",(n21)-[:LINK {weight:3}]->(n17)\n" +
                    ",(n21)-[:LINK {weight:3}]->(n18)\n" +
                    ",(n21)-[:LINK {weight:3}]->(n19)\n" +
                    ",(n21)-[:LINK {weight:5}]->(n20)\n" +
                    ",(n22)-[:LINK {weight:3}]->(n16)\n" +
                    ",(n22)-[:LINK {weight:3}]->(n17)\n" +
                    ",(n22)-[:LINK {weight:3}]->(n18)\n" +
                    ",(n22)-[:LINK {weight:3}]->(n19)\n" +
                    ",(n22)-[:LINK {weight:4}]->(n20)\n" +
                    ",(n22)-[:LINK {weight:4}]->(n21)\n" +
                    ",(n23)-[:LINK {weight:2}]->(n12)\n" +
                    ",(n23)-[:LINK {weight:3}]->(n16)\n" +
                    ",(n23)-[:LINK {weight:3}]->(n17)\n" +
                    ",(n23)-[:LINK {weight:3}]->(n18)\n" +
                    ",(n23)-[:LINK {weight:3}]->(n19)\n" +
                    ",(n23)-[:LINK {weight:4}]->(n20)\n" +
                    ",(n23)-[:LINK {weight:4}]->(n21)\n" +
                    ",(n23)-[:LINK {weight:4}]->(n22)\n" +
                    ",(n23)-[:LINK {weight:9}]->(n11)\n" +
                    ",(n24)-[:LINK {weight:2}]->(n23)\n" +
                    ",(n24)-[:LINK {weight:7}]->(n11)\n" +
                    ",(n25)-[:LINK {weight:1}]->(n23)\n" +
                    ",(n25)-[:LINK {weight:2}]->(n11)\n" +
                    ",(n25)-[:LINK {weight:3}]->(n24)\n" +
                    ",(n26)-[:LINK {weight:1}]->(n11)\n" +
                    ",(n26)-[:LINK {weight:1}]->(n16)\n" +
                    ",(n26)-[:LINK {weight:1}]->(n25)\n" +
                    ",(n26)-[:LINK {weight:4}]->(n24)\n" +
                    ",(n27)-[:LINK {weight:1}]->(n24)\n" +
                    ",(n27)-[:LINK {weight:1}]->(n26)\n" +
                    ",(n27)-[:LINK {weight:5}]->(n23)\n" +
                    ",(n27)-[:LINK {weight:5}]->(n25)\n" +
                    ",(n27)-[:LINK {weight:7}]->(n11)\n" +
                    ",(n28)-[:LINK {weight:1}]->(n27)\n" +
                    ",(n28)-[:LINK {weight:8}]->(n11)\n" +
                    ",(n29)-[:LINK {weight:1}]->(n23)\n" +
                    ",(n29)-[:LINK {weight:1}]->(n27)\n" +
                    ",(n29)-[:LINK {weight:2}]->(n11)\n" +
                    ",(n30)-[:LINK {weight:1}]->(n23)\n" +
                    ",(n31)-[:LINK {weight:1}]->(n27)\n" +
                    ",(n31)-[:LINK {weight:2}]->(n23)\n" +
                    ",(n31)-[:LINK {weight:2}]->(n30)\n" +
                    ",(n31)-[:LINK {weight:3}]->(n11)\n" +
                    ",(n32)-[:LINK {weight:1}]->(n11)\n" +
                    ",(n33)-[:LINK {weight:1}]->(n27)\n" +
                    ",(n33)-[:LINK {weight:2}]->(n11)\n" +
                    ",(n34)-[:LINK {weight:2}]->(n29)\n" +
                    ",(n34)-[:LINK {weight:3}]->(n11)\n" +
                    ",(n35)-[:LINK {weight:2}]->(n29)\n" +
                    ",(n35)-[:LINK {weight:3}]->(n11)\n" +
                    ",(n35)-[:LINK {weight:3}]->(n34)\n" +
                    ",(n36)-[:LINK {weight:1}]->(n29)\n" +
                    ",(n36)-[:LINK {weight:2}]->(n11)\n" +
                    ",(n36)-[:LINK {weight:2}]->(n34)\n" +
                    ",(n36)-[:LINK {weight:2}]->(n35)\n" +
                    ",(n37)-[:LINK {weight:1}]->(n29)\n" +
                    ",(n37)-[:LINK {weight:2}]->(n11)\n" +
                    ",(n37)-[:LINK {weight:2}]->(n34)\n" +
                    ",(n37)-[:LINK {weight:2}]->(n35)\n" +
                    ",(n37)-[:LINK {weight:2}]->(n36)\n" +
                    ",(n38)-[:LINK {weight:1}]->(n29)\n" +
                    ",(n38)-[:LINK {weight:2}]->(n11)\n" +
                    ",(n38)-[:LINK {weight:2}]->(n34)\n" +
                    ",(n38)-[:LINK {weight:2}]->(n35)\n" +
                    ",(n38)-[:LINK {weight:2}]->(n36)\n" +
                    ",(n38)-[:LINK {weight:2}]->(n37)\n" +
                    ",(n39)-[:LINK {weight:1}]->(n25)\n" +
                    ",(n40)-[:LINK {weight:1}]->(n25)\n" +
                    ",(n41)-[:LINK {weight:2}]->(n24)\n" +
                    ",(n41)-[:LINK {weight:3}]->(n25)\n" +
                    ",(n42)-[:LINK {weight:1}]->(n24)\n" +
                    ",(n42)-[:LINK {weight:2}]->(n25)\n" +
                    ",(n42)-[:LINK {weight:2}]->(n41)\n" +
                    ",(n43)-[:LINK {weight:1}]->(n26)\n" +
                    ",(n43)-[:LINK {weight:1}]->(n27)\n" +
                    ",(n43)-[:LINK {weight:3}]->(n11)\n" +
                    ",(n44)-[:LINK {weight:1}]->(n11)\n" +
                    ",(n44)-[:LINK {weight:3}]->(n28)\n" +
                    ",(n45)-[:LINK {weight:2}]->(n28)\n" +
                    ",(n47)-[:LINK {weight:1}]->(n46)\n" +
                    ",(n48)-[:LINK {weight:1}]->(n11)\n" +
                    ",(n48)-[:LINK {weight:1}]->(n25)\n" +
                    ",(n48)-[:LINK {weight:1}]->(n27)\n" +
                    ",(n48)-[:LINK {weight:2}]->(n47)\n" +
                    ",(n49)-[:LINK {weight:2}]->(n11)\n" +
                    ",(n49)-[:LINK {weight:3}]->(n26)\n" +
                    ",(n50)-[:LINK {weight:1}]->(n24)\n" +
                    ",(n50)-[:LINK {weight:1}]->(n49)\n" +
                    ",(n51)-[:LINK {weight:2}]->(n11)\n" +
                    ",(n51)-[:LINK {weight:2}]->(n26)\n" +
                    ",(n51)-[:LINK {weight:9}]->(n49)\n" +
                    ",(n52)-[:LINK {weight:1}]->(n39)\n" +
                    ",(n52)-[:LINK {weight:1}]->(n51)\n" +
                    ",(n53)-[:LINK {weight:1}]->(n51)\n" +
                    ",(n54)-[:LINK {weight:1}]->(n26)\n" +
                    ",(n54)-[:LINK {weight:1}]->(n49)\n" +
                    ",(n54)-[:LINK {weight:2}]->(n51)\n" +
                    ",(n55)-[:LINK {weight:1}]->(n16)\n" +
                    ",(n55)-[:LINK {weight:1}]->(n26)\n" +
                    ",(n55)-[:LINK {weight:1}]->(n39)\n" +
                    ",(n55)-[:LINK {weight:1}]->(n54)\n" +
                    ",(n55)-[:LINK {weight:2}]->(n25)\n" +
                    ",(n55)-[:LINK {weight:2}]->(n49)\n" +
                    ",(n55)-[:LINK {weight:4}]->(n48)\n" +
                    ",(n55)-[:LINK {weight:5}]->(n41)\n" +
                    ",(n55)-[:LINK {weight:6}]->(n51)\n" +
                    ",(n55)-[:LINK {weight:9}]->(n11)\n" +
                    ",(n56)-[:LINK {weight:1}]->(n49)\n" +
                    ",(n56)-[:LINK {weight:1}]->(n55)\n" +
                    ",(n57)-[:LINK {weight:1}]->(n41)\n" +
                    ",(n57)-[:LINK {weight:1}]->(n48)\n" +
                    ",(n57)-[:LINK {weight:1}]->(n55)\n" +
                    ",(n58)-[:LINK {weight:1}]->(n57)\n" +
                    ",(n58)-[:LINK {weight:4}]->(n11)\n" +
                    ",(n58)-[:LINK {weight:6}]->(n27)\n" +
                    ",(n58)-[:LINK {weight:7}]->(n48)\n" +
                    ",(n58)-[:LINK {weight:7}]->(n55)\n" +
                    ",(n59)-[:LINK {weight:2}]->(n57)\n" +
                    ",(n59)-[:LINK {weight:5}]->(n55)\n" +
                    ",(n59)-[:LINK {weight:5}]->(n58)\n" +
                    ",(n59)-[:LINK {weight:6}]->(n48)\n" +
                    ",(n60)-[:LINK {weight:1}]->(n48)\n" +
                    ",(n60)-[:LINK {weight:2}]->(n59)\n" +
                    ",(n60)-[:LINK {weight:4}]->(n58)\n" +
                    ",(n61)-[:LINK {weight:1}]->(n55)\n" +
                    ",(n61)-[:LINK {weight:1}]->(n57)\n" +
                    ",(n61)-[:LINK {weight:2}]->(n48)\n" +
                    ",(n61)-[:LINK {weight:2}]->(n60)\n" +
                    ",(n61)-[:LINK {weight:5}]->(n59)\n" +
                    ",(n61)-[:LINK {weight:6}]->(n58)\n" +
                    ",(n62)-[:LINK {weight:1}]->(n41)\n" +
                    ",(n62)-[:LINK {weight:2}]->(n57)\n" +
                    ",(n62)-[:LINK {weight:3}]->(n59)\n" +
                    ",(n62)-[:LINK {weight:3}]->(n60)\n" +
                    ",(n62)-[:LINK {weight:6}]->(n61)\n" +
                    ",(n62)-[:LINK {weight:7}]->(n48)\n" +
                    ",(n62)-[:LINK {weight:7}]->(n58)\n" +
                    ",(n62)-[:LINK {weight:9}]->(n55)\n" +
                    ",(n63)-[:LINK {weight:1}]->(n55)\n" +
                    ",(n63)-[:LINK {weight:2}]->(n57)\n" +
                    ",(n63)-[:LINK {weight:2}]->(n60)\n" +
                    ",(n63)-[:LINK {weight:3}]->(n61)\n" +
                    ",(n63)-[:LINK {weight:4}]->(n58)\n" +
                    ",(n63)-[:LINK {weight:5}]->(n48)\n" +
                    ",(n63)-[:LINK {weight:5}]->(n59)\n" +
                    ",(n63)-[:LINK {weight:6}]->(n62)\n" +
                    ",(n64)-[:LINK {weight:0}]->(n58)\n" +
                    ",(n64)-[:LINK {weight:1}]->(n11)\n" +
                    ",(n64)-[:LINK {weight:1}]->(n57)\n" +
                    ",(n64)-[:LINK {weight:2}]->(n60)\n" +
                    ",(n64)-[:LINK {weight:2}]->(n62)\n" +
                    ",(n64)-[:LINK {weight:4}]->(n63)\n" +
                    ",(n64)-[:LINK {weight:5}]->(n48)\n" +
                    ",(n64)-[:LINK {weight:5}]->(n55)\n" +
                    ",(n64)-[:LINK {weight:6}]->(n61)\n" +
                    ",(n64)-[:LINK {weight:9}]->(n59)\n" +
                    ",(n65)-[:LINK {weight:1}]->(n57)\n" +
                    ",(n65)-[:LINK {weight:2}]->(n55)\n" +
                    ",(n65)-[:LINK {weight:2}]->(n60)\n" +
                    ",(n65)-[:LINK {weight:3}]->(n48)\n" +
                    ",(n65)-[:LINK {weight:5}]->(n58)\n" +
                    ",(n65)-[:LINK {weight:5}]->(n59)\n" +
                    ",(n65)-[:LINK {weight:5}]->(n61)\n" +
                    ",(n65)-[:LINK {weight:5}]->(n62)\n" +
                    ",(n65)-[:LINK {weight:5}]->(n63)\n" +
                    ",(n65)-[:LINK {weight:7}]->(n64)\n" +
                    ",(n66)-[:LINK {weight:1}]->(n48)\n" +
                    ",(n66)-[:LINK {weight:1}]->(n59)\n" +
                    ",(n66)-[:LINK {weight:1}]->(n60)\n" +
                    ",(n66)-[:LINK {weight:1}]->(n61)\n" +
                    ",(n66)-[:LINK {weight:1}]->(n63)\n" +
                    ",(n66)-[:LINK {weight:2}]->(n62)\n" +
                    ",(n66)-[:LINK {weight:2}]->(n65)\n" +
                    ",(n66)-[:LINK {weight:3}]->(n58)\n" +
                    ",(n66)-[:LINK {weight:3}]->(n64)\n" +
                    ",(n67)-[:LINK {weight:3}]->(n57)\n" +
                    ",(n68)-[:LINK {weight:1}]->(n11)\n" +
                    ",(n68)-[:LINK {weight:1}]->(n24)\n" +
                    ",(n68)-[:LINK {weight:1}]->(n27)\n" +
                    ",(n68)-[:LINK {weight:1}]->(n41)\n" +
                    ",(n68)-[:LINK {weight:1}]->(n48)\n" +
                    ",(n68)-[:LINK {weight:5}]->(n25)\n" +
                    ",(n69)-[:LINK {weight:1}]->(n11)\n" +
                    ",(n69)-[:LINK {weight:1}]->(n24)\n" +
                    ",(n69)-[:LINK {weight:1}]->(n41)\n" +
                    ",(n69)-[:LINK {weight:1}]->(n48)\n" +
                    ",(n69)-[:LINK {weight:2}]->(n27)\n" +
                    ",(n69)-[:LINK {weight:6}]->(n25)\n" +
                    ",(n69)-[:LINK {weight:6}]->(n68)\n" +
                    ",(n70)-[:LINK {weight:1}]->(n11)\n" +
                    ",(n70)-[:LINK {weight:1}]->(n24)\n" +
                    ",(n70)-[:LINK {weight:1}]->(n27)\n" +
                    ",(n70)-[:LINK {weight:1}]->(n41)\n" +
                    ",(n70)-[:LINK {weight:1}]->(n58)\n" +
                    ",(n70)-[:LINK {weight:4}]->(n25)\n" +
                    ",(n70)-[:LINK {weight:4}]->(n68)\n" +
                    ",(n70)-[:LINK {weight:4}]->(n69)\n" +
                    ",(n71)-[:LINK {weight:1}]->(n11)\n" +
                    ",(n71)-[:LINK {weight:1}]->(n25)\n" +
                    ",(n71)-[:LINK {weight:1}]->(n27)\n" +
                    ",(n71)-[:LINK {weight:1}]->(n41)\n" +
                    ",(n71)-[:LINK {weight:1}]->(n48)\n" +
                    ",(n71)-[:LINK {weight:2}]->(n68)\n" +
                    ",(n71)-[:LINK {weight:2}]->(n69)\n" +
                    ",(n71)-[:LINK {weight:2}]->(n70)\n" +
                    ",(n72)-[:LINK {weight:1}]->(n11)\n" +
                    ",(n72)-[:LINK {weight:1}]->(n27)\n" +
                    ",(n72)-[:LINK {weight:2}]->(n26)\n" +
                    ",(n73)-[:LINK {weight:2}]->(n48)\n" +
                    ",(n74)-[:LINK {weight:2}]->(n48)\n" +
                    ",(n74)-[:LINK {weight:3}]->(n73)\n" +
                    ",(n75)-[:LINK {weight:1}]->(n41)\n" +
                    ",(n75)-[:LINK {weight:1}]->(n48)\n" +
                    ",(n75)-[:LINK {weight:1}]->(n70)\n" +
                    ",(n75)-[:LINK {weight:1}]->(n71)\n" +
                    ",(n75)-[:LINK {weight:3}]->(n25)\n" +
                    ",(n75)-[:LINK {weight:3}]->(n68)\n" +
                    ",(n75)-[:LINK {weight:3}]->(n69)\n" +
                    ",(n76)-[:LINK {weight:1}]->(n48)\n" +
                    ",(n76)-[:LINK {weight:1}]->(n58)\n" +
                    ",(n76)-[:LINK {weight:1}]->(n62)\n" +
                    ",(n76)-[:LINK {weight:1}]->(n63)\n" +
                    ",(n76)-[:LINK {weight:1}]->(n64)\n" +
                    ",(n76)-[:LINK {weight:1}]->(n65)\n" +
                    ",(n76)-[:LINK {weight:1}]->(n66)\n";

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{HeavyGraphFactory.class, "Heavy"},
                new Object[]{HugeGraphFactory.class, "Huge"},
                new Object[]{GraphViewFactory.class, "Kernel"}
        );
    }

    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setupGraph() throws KernelException {
        DB.execute(GRAPH).close();
    }

    @Rule
    public ErrorCollector collector = new ErrorCollector();

    private final Graph graph;
    private final List<Result> expected;
    private final List<Result> expectedNonWeighted;

    public WeightedAllShortestPaths427Test(
            Class<? extends GraphFactory> graphImpl,
            String ignoreParamOnlyForTestNaming) {
        graph = new GraphLoader(DB, Pools.DEFAULT)
                .withLabel("Node")
                .withRelationshipType("LINK")
                .withRelationshipWeightsFromProperty("weight", 1.0)
                .withDirection(Direction.OUTGOING)
                .withDefaultConcurrency()
                .load(graphImpl);
        expected = calculateExpected(true);
        expectedNonWeighted = calculateExpected(false);
    }

    @Test
    public void testWeighted() throws Exception {
        compare(new WeightedAllShortestPaths(
                graph,
                Pools.DEFAULT,
                Pools.DEFAULT_CONCURRENCY, Direction.OUTGOING), this.expected);
    }

    @Test
    public void testMsbfs() throws Exception {
        compare(new MSBFSAllShortestPaths(
                graph,
                AllocationTracker.EMPTY,
                Pools.DEFAULT_CONCURRENCY,
                Pools.DEFAULT, Direction.OUTGOING), this.expectedNonWeighted);
    }

    private List<Result> calculateExpected(boolean withWeights) {
        List<Result> expected = new ArrayList<>();
        ShortestPathDijkstra spd = new ShortestPathDijkstra(graph);
        DB.executeAndCommit((db) -> {
            graph.forEachNode(start -> {
                long s = graph.toOriginalNodeId(start);
                TestDijkstra dijkstra = new TestDijkstra(db.getNodeById(s), withWeights);
                graph.forEachNode(end -> {
                    if (start == end) {
                        return true;
                    }

                    Result neoResult = null;
                    Result algoResult = null;

                    dijkstra.reset();
                    long t = graph.toOriginalNodeId(end);
                    Node targetNode = db.getNodeById(t);
                    List<Node> path = dijkstra.getPathAsNodes(targetNode);

                    if (path != null) {
                        Double cost = dijkstra.getCalculatedCost(targetNode);
                        long[] pathIds = path.stream()
                                .mapToLong(Node::getId)
                                .toArray();
                        neoResult = new Result(s, t, cost, pathIds);
                        expected.add(neoResult);
                    }

                    if (withWeights) {
                        spd.compute(s, t, Direction.OUTGOING);
                        double totalCost = spd.getTotalCost();
                        if (totalCost != ShortestPathDijkstra.NO_PATH_FOUND) {
                            long[] pathIds = Arrays
                                    .stream(spd.getFinalPath().toArray())
                                    .mapToLong(graph::toOriginalNodeId)
                                    .toArray();
                            algoResult = new Result(s, t, totalCost, pathIds);
                        }

                        collector.checkThat(String.format(
                                "Neo vs Algo (%d)-[..]*->(%d)",
                                s,
                                t), algoResult, is(neoResult));
                    }


                    return true;
                });

                return true;
            });
        });

        expected.sort(Comparator.naturalOrder());
        return expected;
    }

    private void compare(MSBFSASPAlgorithm<?> asp, List<Result> expected) {
        List<Result> results = asp
                .resultStream()
                .filter(r -> r.sourceNodeId != r.targetNodeId)
                .map(r -> new Result(
                        r.sourceNodeId,
                        r.targetNodeId,
                        r.distance,
                        null))
                .sorted()
                .collect(toList());

        for (int i = 0; i < expected.size(); i++) {
            Result expect = expected.get(i);
            Result actual = results.get(i);

            collector.checkThat(String.format(
                    "Neo vs wASP (%d)-[..]*->(%d)",
                    expect.source,
                    expect.target), actual, is(expect));
        }
    }

    private static final class TestDijkstra extends SingleSourceShortestPathDijkstra<Double> {

        private static final CostEvaluator<Double> WEIGHT = new DoubleEvaluator("weight");

        TestDijkstra(Node startNode, final boolean withWeights ) {
            super(
                    0.0D,
                    startNode,
                    withWeights ? WEIGHT : (relationship, direction) -> 1.0d,
                    new DoubleAdder(),
                    Comparator.comparingDouble(Double::doubleValue),
                    Direction.OUTGOING,
                    RelationshipType.withName("LINK"));
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
