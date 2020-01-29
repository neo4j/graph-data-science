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
package org.neo4j.graphalgo.similarity;

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GetNodeFunc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class ApproxNearestNeighborsDocTest extends BaseProcTest {
    @Language("Cypher")
    public final String DB_CYPHER =
        " CREATE " +
        "  (french:Cuisine {name:'French'})," +
        "  (italian:Cuisine {name:'Italian'})," +
        "  (indian:Cuisine {name:'Indian'})," +
        "  (lebanese:Cuisine {name:'Lebanese'})," +
        "  (portuguese:Cuisine {name:'Portuguese'})," +
        "" +
        "  (zhen:Person {name: 'Zhen'})," +
        "  (praveena:Person {name: 'Praveena'})," +
        "  (michael:Person {name: 'Michael'})," +
        "  (arya:Person {name: 'Arya'})," +
        "  (karin:Person {name: 'Karin'})," +
        "" +
        "  (praveena)-[:LIKES]->(indian)," +
        "  (praveena)-[:LIKES]->(portuguese)," +
        "" +
        "  (zhen)-[:LIKES]->(french)," +
        "  (zhen)-[:LIKES]->(indian)," +
        "" +
        "  (michael)-[:LIKES]->(french)," +
        "  (michael)-[:LIKES]->(italian)," +
        "  (michael)-[:LIKES]->(indian)," +
        "" +
        "  (arya)-[:LIKES]->(lebanese)," +
        "  (arya)-[:LIKES]->(italian)," +
        "  (arya)-[:LIKES]->(portuguese)," +
        "" +
        "  (karin)-[:LIKES]->(lebanese)," +
        "  (karin)-[:LIKES]->(italian)";

    String NL = System.lineSeparator();

    @BeforeEach
    void setupGraph() throws Exception {
        db = TestDatabaseCreator.createTestDatabase(builder ->
            builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.*")
        );

        registerProcedures(ApproxNearestNeighborsProc.class);
        registerFunctions(GetNodeFunc.class);
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void clearCommunities() {
        db.shutdown();
        GraphCatalog.removeAllLoadedGraphs();
    }

    @Test
    void stream() {
        String query = " MATCH (p:Person)-[:LIKES]->(cuisine) " +
                       " WITH {item:id(p), categories: collect(id(cuisine))} AS userData " +
                       " WITH collect(userData) AS data " +
                       " CALL gds.alpha.ml.ann.stream({ " +
                       "   data: data," +
                       "   algorithm: 'jaccard'," +
                       "   similarityCutoff: 0.1," +
                       "   concurrency: 1" +
                       " }) " +
                       " YIELD item1, item2, similarity " +
                       " RETURN gds.util.asNode(item1).name AS from, gds.util.asNode(item2).name AS to, similarity " +
                       " ORDER BY from ASCENDING";
        
        String expectedResult = "+----------------------------------------------+" + NL +
                                "| from       | to         | similarity         |" + NL +
                                "+----------------------------------------------+" + NL +
                                "| \"Arya\"     | \"Karin\"    | 0.6666666666666666 |" + NL +
                                "| \"Arya\"     | \"Praveena\" | 0.25               |" + NL +
                                "| \"Arya\"     | \"Michael\"  | 0.2                |" + NL +
                                "| \"Karin\"    | \"Arya\"     | 0.6666666666666666 |" + NL +
                                "| \"Karin\"    | \"Michael\"  | 0.25               |" + NL +
                                "| \"Michael\"  | \"Karin\"    | 0.25               |" + NL +
                                "| \"Michael\"  | \"Praveena\" | 0.25               |" + NL +
                                "| \"Michael\"  | \"Arya\"     | 0.2                |" + NL +
                                "| \"Praveena\" | \"Arya\"     | 0.25               |" + NL +
                                "| \"Praveena\" | \"Michael\"  | 0.25               |" + NL +
                                "| \"Zhen\"     | \"Michael\"  | 0.6666666666666666 |" + NL +
                                "+----------------------------------------------+" + NL +
                                "11 rows" + NL ;

        runQueryWithResultConsumer(query, result -> assertEquals(expectedResult, result.resultAsString()));
    }

    @Test
    void write() {
        String query =
            "MATCH (p:Person)-[:LIKES]->(cuisine) " +
            " WITH {item:id(p), categories: collect(id(cuisine))} AS userData " +
            " WITH collect(userData) AS data " +
            " CALL gds.alpha.ml.ann.write({ " +
            "  algorithm: 'jaccard', " +
            "  data: data, " +
            "  similarityCutoff: 0.1, " +
            "  showComputations: true," +
            "  concurrency: 1, " +
            "  write: true " +
            " }) " +
            " YIELD nodes, similarityPairs, writeRelationshipType, writeProperty, min, max, mean, p95 " +
            " RETURN nodes, similarityPairs, writeRelationshipType, writeProperty, min, max, mean, p95 ";

        String expectedResult = "+-------------------------------------------------------------------------------------------------------------------------------------------------------+" + NL +
                                "| nodes | similarityPairs | writeRelationshipType | writeProperty | min                 | max                | mean                | p95                |" + NL +
                                "+-------------------------------------------------------------------------------------------------------------------------------------------------------+" + NL +
                                "| 5     | 13              | \"SIMILAR\"             | \"score\"       | 0.19999980926513672 | 0.6666669845581055 | 0.35277803738911945 | 0.6666669845581055 |" + NL +
                                "+-------------------------------------------------------------------------------------------------------------------------------------------------------+" + NL +
                                "1 row" + NL ;

        runQueryWithResultConsumer(query, result -> assertNotEquals(expectedResult, result.resultAsString()));

        String controlQuery = " MATCH (p:Person {name: 'Praveena'})-[:SIMILAR]->(other), " +
                              "       (other)-[:LIKES]->(cuisine) " +
                              " WHERE NOT((p)-[:LIKES]->(cuisine)) " +
                              " RETURN cuisine.name AS cuisine, COUNT(*) AS count " +
                              " ORDER BY count DESC ";

        String expectedResult2 = "+--------------------+" + NL +
                                 "| cuisine    | count |" + NL +
                                 "+--------------------+" + NL +
                                 "| \"Italian\"  | 2     |" + NL +
                                 "| \"French\"   | 1     |" + NL +
                                 "| \"Lebanese\" | 1     |" + NL +
                                 "+--------------------+" + NL +
                                 "3 rows" + NL;

        runQueryWithResultConsumer(controlQuery, result -> assertEquals(expectedResult2, result.resultAsString()));
    }
}
