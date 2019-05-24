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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class GetNodeFuncTest {
    public static GraphDatabaseService DB;

    @BeforeClass
    public static void setUp() throws Exception {
        DB = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig(GraphDatabaseSettings.procedure_unrestricted,"algo.*")
                .newGraphDatabase();

        Procedures proceduresService = ((GraphDatabaseAPI) DB).getDependencyResolver().resolveDependency(Procedures.class);

        proceduresService.registerProcedure(Procedures.class, true);
        proceduresService.registerFunction(GetNodeFunc.class, true);
    }

    @AfterClass
    public static void tearDown() {
        DB.shutdown();
    }

    @Test
    public void lookupNode() throws Exception {
        String createNodeQuery = "CREATE (p:Person {name: 'Mark'}) RETURN p AS node";
        Node savedNode = (Node) DB.execute(createNodeQuery).next().get("node");

        Map<String, Object> params = MapUtil.map("nodeId", savedNode.getId());
        Map<String, Object> row = DB.execute("RETURN algo.asNode($nodeId) AS node", params).next();

        Node node = (Node) row.get("node");
        assertEquals(savedNode, node);
    }

    @Test
    public void lookupNonExistentNode() throws Exception {
        Map<String, Object> row = DB.execute(
                "RETURN algo.asNode(3) AS node").next();

        assertNull(row.get("node"));
    }

    @Test
    public void lookupNodes() throws Exception {
        String createNodeQuery = "CREATE (p1:Person {name: 'Mark'}) CREATE (p2:Person {name: 'Arya'}) RETURN p1, p2";
        Map<String, Object> savedRow = DB.execute(createNodeQuery).next();
        Node savedNode1 = (Node) savedRow.get("p1");
        Node savedNode2 = (Node) savedRow.get("p2");

        Map<String, Object> params = MapUtil.map("nodeIds", Arrays.asList(savedNode1.getId(), savedNode2.getId()));
        Map<String, Object> row = DB.execute("RETURN algo.asNodes($nodeIds) AS nodes", params).next();

        List<Node> nodes = (List<Node>) row.get("nodes");
        assertEquals(Arrays.asList(savedNode1, savedNode2), nodes);
    }

    @Test
    public void lookupNonExistentNodes() throws Exception {
        Map<String, Object> row = DB.execute(
                "RETURN algo.getNodesById([3,4,5]) AS nodes").next();

        List<Node> nodes = (List<Node>) row.get("nodes");
        assertEquals(0, nodes.size());
    }

}
