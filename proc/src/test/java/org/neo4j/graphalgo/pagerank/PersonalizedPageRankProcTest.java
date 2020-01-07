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

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.GraphLoadProc;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.newapi.GraphCreateProc;
import org.neo4j.graphalgo.newapi.ImmutableGraphCreateFromStoreConfig;
import org.neo4j.graphalgo.newapi.ImmutableGraphCreateFromStoreConfig;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.neo4j.graphalgo.QueryRunner.runInTransaction;
import static org.neo4j.helpers.collection.MapUtil.map;

class PersonalizedPageRankProcTest extends BaseProcTest {
    @Language("Cypher")
    private static final String DB_CYPHER =
        "CREATE" +
        "  (iphone:Product {name: 'iPhone5'})" +
        ", (kindle:Product {name: 'Kindle Fire'})" +
        ", (fitbit:Product {name: 'Fitbit Flex Wireless'})" +
        ", (potter:Product {name: 'Harry Potter'})" +
        ", (hobbit:Product {name: 'Hobbit'})" +

        ", (todd:Person {name: 'Todd'})" +
        ", (mary:Person {name: 'Mary'})" +
        ", (jill:Person {name: 'Jill'})" +
        ", (john:Person {name: 'John'})" +

        ",  (john)-[:PURCHASED]->(iphone)" +
        ",  (john)-[:PURCHASED]->(kindle)" +
        ",  (mary)-[:PURCHASED]->(iphone)" +
        ",  (mary)-[:PURCHASED]->(kindle)" +
        ",  (mary)-[:PURCHASED]->(fitbit)" +
        ",  (jill)-[:PURCHASED]->(iphone)" +
        ",  (jill)-[:PURCHASED]->(kindle)" +
        ",  (jill)-[:PURCHASED]->(fitbit)" +
        ",  (todd)-[:PURCHASED]->(fitbit)" +
        ",  (todd)-[:PURCHASED]->(potter)" +
        ",  (todd)-[:PURCHASED]->(hobbit)";

    private Map<Long, Double> expected;
    private static final Label PERSON_LABEL = Label.label("Person");
    private static final Label PRODUCT_LABEL = Label.label("Product");

    @BeforeEach
    void setupGraph() throws KernelException {

        db = TestDatabaseCreator.createTestDatabase();

        registerProcedures(
            PageRankStreamProc.class,
            PageRankWriteProc.class,
            GraphLoadProc.class,
            GraphCreateProc.class
        );
        runQuery(DB_CYPHER);


        expected = new HashMap<>();

        runInTransaction(db, () -> {
            expected.put(db.findNode(PERSON_LABEL, "name", "John").getId(), 0.24851499999999993);
            expected.put(db.findNode(PERSON_LABEL, "name", "Jill").getId(), 0.12135449999999998);
            expected.put(db.findNode(PERSON_LABEL, "name", "Mary").getId(), 0.12135449999999998);
            expected.put(db.findNode(PERSON_LABEL, "name", "Todd").getId(), 0.043511499999999995);

            expected.put(db.findNode(PRODUCT_LABEL, "name", "Kindle Fire").getId(), 0.17415649999999996);
            expected.put(db.findNode(PRODUCT_LABEL, "name", "iPhone5").getId(), 0.17415649999999996);
            expected.put(db.findNode(PRODUCT_LABEL, "name", "Fitbit Flex Wireless").getId(), 0.08085200000000001);
            expected.put(db.findNode(PRODUCT_LABEL, "name", "Harry Potter").getId(), 0.01224);
            expected.put(db.findNode(PRODUCT_LABEL, "name", "Hobbit").getId(), 0.01224);
        });
    }

    @Test
    void personalizedPageRankOnImplicitGraph() {
        List<Node> startNodes = new ArrayList<>();
        runInTransaction(db, () -> {
            startNodes.add(db.findNode(PERSON_LABEL, "name", "John"));
        });

        @Language("Cypher")
        String query = GdsCypher.call().implicitCreation(ImmutableGraphCreateFromStoreConfig
            .builder()
            .graphName("personalisedGraph")
            .nodeProjection(NodeProjections.fromObject(map("Person", "Person | Product")))
            .relationshipProjection(RelationshipProjections.builder()
                .putProjection(
                    ElementIdentifier.of("Product"),
                    RelationshipProjection.builder()
                        .type("PURCHASED")
                        .projection(Projection.UNDIRECTED)
                        .build()
                )
                .build()
            )
            .build()
        )
            .algo("pageRank").streamMode()
            .addPlaceholder("sourceNodes", "startNodes")
            .yields("nodeId", "score");

        Map<Long, Double> actual = new HashMap<>();

        runQuery(
            query,
            map("startNodes", startNodes),
            row -> actual.put(
                (Long) row.get("nodeId"),
                (Double) row.get("score")
            )
        );

        assertMapEquals(expected, actual);
    }

    @Test
    void personalizedPageRankWithTwoSourceNodesOnImplicitGraph() {
        List<Node> startNodes = new ArrayList<>();
        runInTransaction(db, () -> {
            startNodes.add(db.findNode(PERSON_LABEL, "name", "John"));
            startNodes.add(db.findNode(PERSON_LABEL, "name", "Mary"));
        });

        @Language("Cypher")
        String query = GdsCypher.call().implicitCreation(ImmutableGraphCreateFromStoreConfig
            .builder()
            .graphName("personalisedGraph")
            .nodeProjection(NodeProjections.fromObject(map("Person", "Person | Product")))
            .relationshipProjection(RelationshipProjections.builder()
                .putProjection(
                    ElementIdentifier.of("Product"),
                    RelationshipProjection.builder()
                        .type("PURCHASED")
                        .projection(Projection.UNDIRECTED)
                        .build()
                )
                .build()
            )
            .build()
        )
            .algo("pageRank").streamMode()
            .addPlaceholder("sourceNodes", "startNodes")
            .yields("nodeId", "score");

        Map<Long, Double> actual = new HashMap<>();

        runQuery(
            query,
            map("startNodes", startNodes),
            row -> actual.put(
                (Long) row.get("nodeId"),
                (Double) row.get("score")
            )
        );

        Map<Long, Double> expectedResult = new HashMap<>();
        runInTransaction(db, () -> {
            expectedResult.put(db.findNode(PERSON_LABEL, "name", "John").getId(), 0.3260027844575233);
            expectedResult.put(db.findNode(PERSON_LABEL, "name", "Jill").getId(), 0.23342811424518006);
            expectedResult.put(db.findNode(PERSON_LABEL, "name", "Mary").getId(), 0.3834281142451801);
            expectedResult.put(db.findNode(PERSON_LABEL, "name", "Todd").getId(), 0.10794770773500204);

            expectedResult.put(db.findNode(PRODUCT_LABEL, "name", "Kindle Fire").getId(), 0.3105931498808786);
            expectedResult.put(db.findNode(PRODUCT_LABEL, "name", "iPhone5").getId(), 0.3105931498808786);
            expectedResult.put(db.findNode(PRODUCT_LABEL, "name", "Fitbit Flex Wireless").getId(), 0.2026776185026392);
            expectedResult.put(db.findNode(PRODUCT_LABEL, "name", "Harry Potter").getId(), 0.029719049402046945);
            expectedResult.put(db.findNode(PRODUCT_LABEL, "name", "Hobbit").getId(), 0.029719049402046945);
        });

        assertMapEquals(expectedResult, actual);
    }

    @Test
    void personalizedPageRankOnExplicitGraph() {
        List<Node> startNodes = new ArrayList<>();
        runInTransaction(db, () -> {
            startNodes.add(db.findNode(PERSON_LABEL, "name", "John"));
        });

        runQuery("CALL  gds.graph.create('personalisedGraph', " +
                 "'Person | Product'," +
                 "  {" +
                 "      Product:{" +
                 "        type:'PURCHASED'," +
                 "        projection:'UNDIRECTED'," +
                 "        aggregation: 'DEFAULT'" +
                 "      }" +
                 "  }" +
                 ")");

        String query = GdsCypher.call().explicitCreation("personalisedGraph")
            .algo("pageRank").streamMode()
            .addPlaceholder("sourceNodes", "startNodes")
            .yields("nodeId", "score");

        Map<Long, Double> actual = new HashMap<>();

        runQuery(
            query,
            map("startNodes", startNodes),
            row -> actual.put(
                (Long) row.get("nodeId"),
                (Double) row.get("score")
            )
        );

        assertMapEquals(expected, actual);
    }
}
