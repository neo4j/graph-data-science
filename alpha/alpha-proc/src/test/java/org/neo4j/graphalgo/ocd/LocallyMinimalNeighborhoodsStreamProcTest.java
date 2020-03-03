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
package org.neo4j.graphalgo.ocd;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GetNodeFunc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LocallyMinimalNeighborhoodsStreamProcTest extends BaseProcTest {
    /**
     *      (a)-- (b)--(d)--(e)
     *        \T1/       \T2/
     *        (c)   (g)  (f)
     *          \  /T3\
     *          (h)--(i)
     */
    String dbCypher() {
        return
            "CREATE " +
            "  (a:Node {name: 'a'})" +
            ", (b:Node {name: 'b'})" +
            ", (c:Node {name: 'c'})" +
            ", (d:Node {name: 'd'})" +
            ", (e:Node {name: 'e'})" +
            ", (f:Node {name: 'f'})" +
            ", (g:Node {name: 'g'})" +
            ", (h:Node {name: 'h'})" +
            ", (i:Node {name: 'i'})" +
            ", (a)-[:TYPE]->(b)" +
            ", (b)-[:TYPE]->(c)" +
            ", (c)-[:TYPE]->(a)" +
            ", (c)-[:TYPE]->(h)" +
            ", (d)-[:TYPE]->(e)" +
            ", (e)-[:TYPE]->(f)" +
            ", (f)-[:TYPE]->(d)" +
            ", (b)-[:TYPE]->(d)" +
            ", (g)-[:TYPE]->(h)" +
            ", (h)-[:TYPE]->(i)" +
            ", (i)-[:TYPE]->(g)";
    }

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase((builder) ->
            builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.util.*")
        );
        registerProcedures(LocallyMinimalNeighborhoodsStreamProc.class);
        registerFunctions(GetNodeFunc.class);
        runQuery(dbCypher());
    }

    @AfterEach
    void shutdownGraph() {
        db.shutdown();
    }

    @Test
    void streamLMNWithMembers() {
        String q = "CALL gds.alpha.lmn.stream({nodeProjection: \"*\", relationshipProjection: {`*`: {type: \"*\", orientation: \"UNDIRECTED\"}}})" +
                   " YIELD nodeId, communityId, conductance" +
                   " RETURN gds.util.asNode(nodeId).name as node, gds.util.asNode(communityId).name as community, conductance";

        Set<String> actualRowDescriptions = new HashSet<>();

        runQueryWithRowConsumer(q, row -> {
            double conductance = row.getNumber("conductance").doubleValue();
            String actual = String.format(
                "node %s community %s conductance %s",
                row.getString("node"),
                row.getString("community"),
                conductance
            );
            actualRowDescriptions.add(actual);
            System.out.println(actual);
        });
        Set<String> expectedRowDescriptions = new HashSet<>(Arrays.asList(("node b community a conductance 0.75\n" +
                                                             "node c community a conductance 0.75\n" +
                                                             "node a community a conductance 0.666666\n" +
                                                             "node d community e conductance 0.5\n" +
                                                             "node f community e conductance 0.333333\n" +
                                                             "node e community e conductance 0.333333\n" +
                                                             "node e community f conductance 0.333333\n" +
                                                             "node f community f conductance 0.333333\n" +
                                                             "node d community f conductance 0.5\n" +
                                                             "node i community g conductance 0.333333\n" +
                                                             "node h community g conductance 0.5\n" +
                                                             "node g community g conductance 0.333333\n" +
                                                             "node h community i conductance 0.5\n" +
                                                             "node g community i conductance 0.333333\n" +
                                                             "node i community i conductance 0.333333").split("\n")));
        assertEquals(expectedRowDescriptions, actualRowDescriptions);
    }

    @Test
    void streamLMNOnlyCenters() {
        String q = "CALL gds.alpha.lmn.stream({nodeProjection: \"*\", relationshipProjection: {`*`: {type: \"*\", orientation: \"UNDIRECTED\"}}, includeMembers: false})" +
                   " YIELD nodeId, communityId, conductance" +
                   " RETURN gds.util.asNode(nodeId).name as node, gds.util.asNode(communityId).name as community, conductance";

        Set<String> actualRowDescriptions = new HashSet<>();

        runQueryWithRowConsumer(q, row -> {
            double conductance = row.getNumber("conductance").doubleValue();
            String actual = String.format(
                "node %s community %s conductance %s",
                row.getString("node"),
                row.getString("community"),
                conductance
            );
            actualRowDescriptions.add(actual);
        });
        Set<String> expectedRowDescriptions = new HashSet<>(Arrays.asList(("node a community a conductance 0.666666\n" +
                                                                           "node e community e conductance 0.333333\n" +
                                                                           "node f community f conductance 0.333333\n" +
                                                                           "node g community g conductance 0.333333\n" +
                                                                           "node i community i conductance 0.333333").split("\n")));
        assertEquals(expectedRowDescriptions, actualRowDescriptions);
    }
}
