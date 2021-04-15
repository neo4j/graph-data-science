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
package org.neo4j.graphalgo.pagerank;

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.QueryRunner;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.config.ImmutableGraphCreateFromStoreConfig;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.runInTransaction;
import static org.neo4j.graphalgo.compat.MapUtil.map;

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
    void setupGraph() throws Exception {
        registerProcedures(PageRankStreamProc.class, PageRankWriteProc.class, GraphCreateProc.class);
        runQuery(DB_CYPHER);
        expected = createExpectedResults(db);
    }

    @AfterEach
    void cleanup() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    Map<Long, Double> createExpectedResults(final GraphDatabaseService db) {
        Map<Long, Double> expected = new HashMap<>();

        runInTransaction(db, tx -> {
            expected.put(tx.findNode(PERSON_LABEL, "name", "John").getId(), 0.24851499999999993);
            expected.put(tx.findNode(PERSON_LABEL, "name", "Jill").getId(), 0.12135449999999998);
            expected.put(tx.findNode(PERSON_LABEL, "name", "Mary").getId(), 0.12135449999999998);
            expected.put(tx.findNode(PERSON_LABEL, "name", "Todd").getId(), 0.043511499999999995);

            expected.put(tx.findNode(PRODUCT_LABEL, "name", "Kindle Fire").getId(), 0.17415649999999996);
            expected.put(tx.findNode(PRODUCT_LABEL, "name", "iPhone5").getId(), 0.17415649999999996);
            expected.put(tx.findNode(PRODUCT_LABEL, "name", "Fitbit Flex Wireless").getId(), 0.08085200000000001);
            expected.put(tx.findNode(PRODUCT_LABEL, "name", "Harry Potter").getId(), 0.01224);
            expected.put(tx.findNode(PRODUCT_LABEL, "name", "Hobbit").getId(), 0.01224);
        });

        return expected;
    }

    private void assertMapEqualsWithTolerance(Map<Long, Double> expected, Map<Long, Double> actual) {
        super.assertMapEqualsWithTolerance(expected, actual, 0.1);
    }

    @Test
    void personalizedPageRankOnImplicitGraph() {
        List<Node> startNodes = new ArrayList<>();
        runInTransaction(db, tx -> startNodes.add(tx.findNode(PERSON_LABEL, "name", "John")));

        @Language("Cypher")
        String query = GdsCypher.call().implicitCreation(ImmutableGraphCreateFromStoreConfig
            .builder()
            .graphName("personalisedGraph")
            .nodeProjections(NodeProjections.fromObject(map("Person", "Person", "Product", "Product")))
            .relationshipProjections(RelationshipProjections.builder()
                .putProjection(
                    RelationshipType.of("Product"),
                    RelationshipProjection.builder()
                        .type("PURCHASED")
                        .orientation(Orientation.UNDIRECTED)
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

        runQueryWithRowConsumer(
            query,
            map("startNodes", startNodes),
            row -> actual.put(
                (Long) row.get("nodeId"),
                (Double) row.get("score")
            )
        );

        assertMapEqualsWithTolerance(expected, actual);
    }

    @Test
    void personalizedPageRankWithTwoSourceNodesOnImplicitGraph() {
        List<Node> startNodes = new ArrayList<>();
        runInTransaction(db, tx -> {
            startNodes.add(tx.findNode(PERSON_LABEL, "name", "John"));
            startNodes.add(tx.findNode(PERSON_LABEL, "name", "Mary"));
        });

        @Language("Cypher")
        String query = GdsCypher.call().implicitCreation(ImmutableGraphCreateFromStoreConfig
            .builder()
            .graphName("personalisedGraph")
            .nodeProjections(NodeProjections.fromObject(map("Person", "Person", "Product", "Product")))
            .relationshipProjections(RelationshipProjections.builder()
                .putProjection(
                    RelationshipType.of("Product"),
                    RelationshipProjection.builder()
                        .type("PURCHASED")
                        .orientation(Orientation.UNDIRECTED)
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

        runQueryWithRowConsumer(
            query,
            map("startNodes", startNodes),
            row -> actual.put(
                (Long) row.get("nodeId"),
                (Double) row.get("score")
            )
        );

        Map<Long, Double> expectedResult = new HashMap<>();
        runInTransaction(db, tx -> {
            expectedResult.put(tx.findNode(PERSON_LABEL, "name", "John").getId(), 0.3238460881011776);
            expectedResult.put(tx.findNode(PERSON_LABEL, "name", "Jill").getId(), 0.23022101428940242);
            expectedResult.put(tx.findNode(PERSON_LABEL, "name", "Mary").getId(), 0.38022101428940247);
            expectedResult.put(tx.findNode(PERSON_LABEL, "name", "Todd").getId(), 0.10489076863405605);

            expectedResult.put(tx.findNode(PRODUCT_LABEL, "name", "Kindle Fire").getId(), 0.3105931622069952);
            expectedResult.put(tx.findNode(PRODUCT_LABEL, "name", "iPhone5").getId(), 0.3105931622069952);
            expectedResult.put(tx.findNode(PRODUCT_LABEL, "name", "Fitbit Flex Wireless").getId(), 0.20267762587697727);
            expectedResult.put(tx.findNode(PRODUCT_LABEL, "name", "Harry Potter").getId(), 0.029719051112982554);
            expectedResult.put(tx.findNode(PRODUCT_LABEL, "name", "Hobbit").getId(), 0.029719051112982554);
        });

        assertThat(actual).containsExactlyInAnyOrderEntriesOf(expectedResult);
    }

    @Test
    void personalizedPageRankOnExplicitGraph() {
        List<Node> startNodes = new ArrayList<>();
        runInTransaction(db, tx -> startNodes.add(tx.findNode(PERSON_LABEL, "name", "John")));

        runQuery("CALL  gds.graph.create('personalisedGraph', " +
                 "  ['Person', 'Product']," +
                 "  {" +
                 "      Product:{" +
                 "        type:'PURCHASED'," +
                 "        orientation:'UNDIRECTED'," +
                 "        aggregation: 'DEFAULT'" +
                 "      }" +
                 "  }" +
                 ")");

        String query = GdsCypher.call().explicitCreation("personalisedGraph")
            .algo("pageRank").streamMode()
            .addPlaceholder("sourceNodes", "startNodes")
            .yields("nodeId", "score");

        Map<Long, Double> actual = new HashMap<>();

        runQueryWithRowConsumer(
            query,
            map("startNodes", startNodes),
            row -> actual.put(
                (Long) row.get("nodeId"),
                (Double) row.get("score")
            )
        );

        assertMapEqualsWithTolerance(expected, actual);
    }

    @Test
    void testStreamRunsOnLoadedGraphWithNodeLabelFilter() {
        clearDb();
        String queryWithIgnore = "CREATE (nXX:IgnoreAlso {nodeId: 1337}), (nX:Ignore {nodeId: 42}) " + DB_CYPHER + " CREATE (nX)-[:X]->(a), (a)-[:X]->(nX), (nX)-[:X]->(e), (e)-[:X]->(nX)";
        QueryRunner.runQuery(db, queryWithIgnore);

        Map<Long, Double> expected = createExpectedResults(db);

        List<Node> startNodes = new ArrayList<>();
        runInTransaction(db, tx -> startNodes.add(tx.findNode(PERSON_LABEL, "name", "John")));

        QueryRunner.runQuery(
            db,
            "CALL  gds.graph.create('personalisedGraph', " +
            "  ['Ignore', 'Person', 'Product']," +
            "  {" +
            "      Product:{" +
            "        type:'PURCHASED'," +
            "        orientation:'UNDIRECTED'," +
            "        aggregation: 'DEFAULT'" +
            "      }" +
            "  }" +
            ")"
        );

        String query = GdsCypher.call().explicitCreation("personalisedGraph")
            .algo("pageRank").streamMode()
            .addPlaceholder("sourceNodes", "startNodes")
            .addParameter("nodeLabels", Arrays.asList("Person", "Product"))
            .yields("nodeId", "score");

        Map<Long, Double> actual = new HashMap<>();

        runQueryWithRowConsumer(
            db,
            query,
            map("startNodes", startNodes),
            row -> actual.put(
                (Long) row.get("nodeId"),
                (Double) row.get("score")
            )
        );

        assertMapEqualsWithTolerance(expected, actual);
    }
}
