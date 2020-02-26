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
import org.neo4j.graphalgo.IsFiniteFunc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphdb.Result;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CosineDocTest extends BaseProcTest {

    @BeforeEach
    void setup() throws Exception {
        String createGraph = "CREATE (french:Cuisine {name:'French'})" +
                             "CREATE (italian:Cuisine {name:'Italian'})" +
                             "CREATE (indian:Cuisine {name:'Indian'})" +
                             "CREATE (lebanese:Cuisine {name:'Lebanese'})" +
                             "CREATE (portuguese:Cuisine {name:'Portuguese'})" +
                             "CREATE (british:Cuisine {name:'British'})" +
                             "CREATE (mauritian:Cuisine {name:'Mauritian'})" +

                             "CREATE (zhen:Person {name: \"Zhen\"})" +
                             "CREATE (praveena:Person {name: \"Praveena\"})" +
                             "CREATE (michael:Person {name: \"Michael\"})" +
                             "CREATE (arya:Person {name: \"Arya\"})" +
                             "CREATE (karin:Person {name: \"Karin\"})" +
                             "CREATE (praveena)-[:LIKES {score: 9}]->(indian)" +
                             "CREATE (praveena)-[:LIKES {score: 7}]->(portuguese)" +
                             "CREATE (praveena)-[:LIKES {score: 8}]->(british)" +
                             "CREATE (praveena)-[:LIKES {score: 1}]->(mauritian)" +
                             "CREATE (zhen)-[:LIKES {score: 10}]->(french)" +
                             "CREATE (zhen)-[:LIKES {score: 6}]->(indian)" +
                             "CREATE (zhen)-[:LIKES {score: 2}]->(british)" +

                             "CREATE (michael)-[:LIKES {score: 8}]->(french)" +
                             "CREATE (michael)-[:LIKES {score: 7}]->(italian)" +
                             "CREATE (michael)-[:LIKES {score: 9}]->(indian)" +
                             "CREATE (michael)-[:LIKES {score: 3}]->(portuguese)" +

                             "CREATE (arya)-[:LIKES {score: 10}]->(lebanese)" +
                             "CREATE (arya)-[:LIKES {score: 10}]->(italian)" +
                             "CREATE (arya)-[:LIKES {score: 7}]->(portuguese)" +
                             "CREATE (arya)-[:LIKES {score: 9}]->(mauritian)" +

                             "CREATE (karin)-[:LIKES {score: 9}]->(lebanese)" +
                             "CREATE (karin)-[:LIKES {score: 7}]->(italian)" +
                             "CREATE (karin)-[:LIKES {score: 10}]->(portuguese)";

        db = TestDatabaseCreator.createTestDatabase();
        runQuery(createGraph);
        registerProcedures(CosineProc.class, GraphCreateProc.class);
        registerFunctions(GetNodeFunc.class, SimilaritiesFunc.class, IsFiniteFunc.class);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void functionOnLists() {
        String expectedString = "+--------------------+\n" +
                                "| similarity         |\n" +
                                "+--------------------+\n" +
                                "| 0.8638935626791597 |\n" +
                                "+--------------------+\n" +
                                "1 row\n";

        assertEquals(expectedString, runQuery("RETURN gds.alpha.similarity.cosine([3,8,7,5,2,9], [10,8,6,6,4,5]) AS similarity", Result::resultAsString));
    }

    @Test
    void functionOnGraph() {
        @Language("Cypher")
        String query = " MATCH (p1:Person {name: 'Michael'})-[likes1:LIKES]->(cuisine)" +
                       " MATCH (p2:Person {name: \"Arya\"})-[likes2:LIKES]->(cuisine)" +
                       " RETURN p1.name AS from," +
                       "        p2.name AS to," +
                       "        gds.alpha.similarity.cosine(collect(likes1.score), collect(likes2.score)) AS similarity";

        String expectedString = "+-----------------------------------------+\n" +
                                "| from      | to     | similarity         |\n" +
                                "+-----------------------------------------+\n" +
                                "| \"Michael\" | \"Arya\" | 0.9788908326303921 |\n" +
                                "+-----------------------------------------+\n" +
                                "1 row\n";

        assertEquals(expectedString, runQuery(query, Result::resultAsString));
    }

    @Test
    void functionOnAllGraph() {
        @Language("Cypher")
        String query = " MATCH (p1:Person {name: 'Michael'})-[likes1:LIKES]->(cuisine)" +
                       " MATCH (p2:Person)-[likes2:LIKES]->(cuisine) WHERE p2 <> p1" +
                       " RETURN p1.name AS from," +
                       "        p2.name AS to," +
                       "        gds.alpha.similarity.cosine(collect(likes1.score), collect(likes2.score)) AS similarity" +
                       "    ORDER BY similarity DESC";

        String expectedString = "+---------------------------------------------+\n" +
                                "| from      | to         | similarity         |\n" +
                                "+---------------------------------------------+\n" +
                                "| \"Michael\" | \"Arya\"     | 0.9788908326303921 |\n" +
                                "| \"Michael\" | \"Zhen\"     | 0.9542262139256075 |\n" +
                                "| \"Michael\" | \"Praveena\" | 0.9429903335828894 |\n" +
                                "| \"Michael\" | \"Karin\"    | 0.8498063272285821 |\n" +
                                "+---------------------------------------------+\n" +
                                "4 rows\n";

        assertEquals(expectedString, runQuery(query, Result::resultAsString));
    }

    @Test
    void stream() {
        @Language("Cypher")
        String query = " MATCH (p:Person), (c:Cuisine)" +
                       " OPTIONAL MATCH (p)-[likes:LIKES]->(c)" +
                       " WITH {item:id(p), weights: collect(coalesce(likes.score, gds.util.NaN()))} as userData" +
                       " WITH collect(userData) AS data" +
                       " CALL gds.alpha.similarity.cosine.stream({data: data, topK: 0})" +
                       " YIELD item1, item2, count1, count2, similarity" +
                       " RETURN gds.util.asNode(item1).name AS from, gds.util.asNode(item2).name AS to, similarity" +
                       "    ORDER BY similarity DESC";

        String expectedString = "+----------------------------------------------+\n" +
                                "| from       | to         | similarity         |\n" +
                                "+----------------------------------------------+\n" +
                                "| \"Praveena\" | \"Karin\"    | 1.0                |\n" +
                                "| \"Michael\"  | \"Arya\"     | 0.9788908326303921 |\n" +
                                "| \"Arya\"     | \"Karin\"    | 0.9610904115204073 |\n" +
                                "| \"Zhen\"     | \"Michael\"  | 0.9542262139256075 |\n" +
                                "| \"Praveena\" | \"Michael\"  | 0.9429903335828895 |\n" +
                                "| \"Zhen\"     | \"Praveena\" | 0.9191450300180579 |\n" +
                                "| \"Michael\"  | \"Karin\"    | 0.8498063272285821 |\n" +
                                "| \"Praveena\" | \"Arya\"     | 0.7194014606174091 |\n" +
                                "| \"Zhen\"     | \"Arya\"     | 0.0                |\n" +
                                "| \"Zhen\"     | \"Karin\"    | 0.0                |\n" +
                                "+----------------------------------------------+\n" +
                                "10 rows\n";

        assertEquals(expectedString, runQuery(query, Result::resultAsString));
    }
    
    @Test
    void streamWithSymCutoff() {
        // TODO: Solve regression
        @Language("Cypher")
        String query = " MATCH (p:Person), (c:Cuisine)" +
                       " OPTIONAL MATCH (p)-[likes:LIKES]->(c)" +
                       " WITH {item:id(p), weights: collect(coalesce(likes.score, gds.util.NaN()))} as userData" +
                       " WITH collect(userData) AS data" +
                       " CALL gds.alpha.similarity.cosine.stream({" +
                       "    data: data," +
                       "    similarityCutoff: 0.0," +
                       "    topK: 0" +
                       " })" +
                       " YIELD item1, item2, count1, count2, similarity" +
                       " RETURN gds.util.asNode(item1).name AS from, gds.util.asNode(item2).name AS to, similarity" +
                       "    ORDER BY similarity DESC";

        String expectedString = "+----------------------------------------------+\n" +
                                "| from       | to         | similarity         |\n" +
                                "+----------------------------------------------+\n" +
                                "| \"Praveena\" | \"Karin\"    | 1.0                |\n" +
                                "| \"Michael\"  | \"Arya\"     | 0.9788908326303921 |\n" +
                                "| \"Arya\"     | \"Karin\"    | 0.9610904115204073 |\n" +
                                "| \"Zhen\"     | \"Michael\"  | 0.9542262139256075 |\n" +
                                "| \"Praveena\" | \"Michael\"  | 0.9429903335828895 |\n" +
                                "| \"Zhen\"     | \"Praveena\" | 0.9191450300180579 |\n" +
                                "| \"Michael\"  | \"Karin\"    | 0.8498063272285821 |\n" +
                                "| \"Praveena\" | \"Arya\"     | 0.7194014606174091 |\n" +
                                "+----------------------------------------------+\n" +
                                "8 rows\n";

        assertEquals(expectedString, runQuery(query, Result::resultAsString));
    }

    @Test
    void streamWithTopK() {
        @Language("Cypher")
        String query = " MATCH (p:Person), (c:Cuisine)" +
                       " OPTIONAL MATCH (p)-[likes:LIKES]->(c)" +
                       " WITH {item:id(p), weights: collect(coalesce(likes.score, gds.util.NaN()))} as userData" +
                       " WITH collect(userData) AS data" +
                       " CALL gds.alpha.similarity.cosine.stream({" +
                       "    data: data, " +
                       "    similarityCutoff: 0.0," +
                       "    topK: 1" +
                       " })" +
                       " YIELD item1, item2, count1, count2, similarity" +
                       " RETURN gds.util.asNode(item1).name AS from, gds.util.asNode(item2).name AS to, similarity" +
                       "    ORDER BY from";

        String expectedString = "+----------------------------------------------+\n" +
                                "| from       | to         | similarity         |\n" +
                                "+----------------------------------------------+\n" +
                                "| \"Arya\"     | \"Michael\"  | 0.9788908326303921 |\n" +
                                "| \"Karin\"    | \"Praveena\" | 1.0                |\n" +
                                "| \"Michael\"  | \"Arya\"     | 0.9788908326303921 |\n" +
                                "| \"Praveena\" | \"Karin\"    | 1.0                |\n" +
                                "| \"Zhen\"     | \"Michael\"  | 0.9542262139256075 |\n" +
                                "+----------------------------------------------+\n" +
                                "5 rows\n";

        assertEquals(expectedString, runQuery(query, Result::resultAsString));
    }

    @Test
    void write() {
        @Language("Cypher")
        String query = " MATCH (p:Person), (c:Cuisine)" +
                       " OPTIONAL MATCH (p)-[likes:LIKES]->(c)" +
                       " WITH {item:id(p), weights: collect(coalesce(likes.score, gds.util.NaN()))} as userData" +
                       " WITH collect(userData) as data" +
                       " CALL gds.alpha.similarity.cosine.write({" +
                       "    data: data," +
                       "    topK: 1, " +
                       "    similarityCutoff: 0.1," +
                       "    write: True" +
                       "    })" +
                       " YIELD nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, stdDev, p25, p50, p75, p90, p95, p99, p999, p100" +
                       " RETURN nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, p95";

        String expectedString = "+----------------------------------------------------------------------------------------------------------------------------------------------------------+\n" +
                                "| nodes | similarityPairs | write | writeRelationshipType | writeProperty | min             | max                | mean               | p95                |\n" +
                                "+----------------------------------------------------------------------------------------------------------------------------------------------------------+\n" +
                                "| 5     | 5               | true  | \"SIMILAR\"             | \"score\"       | 0.9542236328125 | 1.0000038146972656 | 0.9824020385742187 | 1.0000038146972656 |\n" +
                                "+----------------------------------------------------------------------------------------------------------------------------------------------------------+\n" +
                                "1 row\n";

        assertEquals(expectedString, runQuery(query, Result::resultAsString));
        
        String controlQuery = "MATCH (p:Person {name: \"Praveena\"})-[:SIMILAR]->(other)," +
                              "       (other)-[:LIKES]->(cuisine)" +
                              " WHERE not((p)-[:LIKES]->(cuisine))" +
                              " RETURN cuisine.name AS cuisine";

        expectedString = "+------------+\n" +
                         "| cuisine    |\n" +
                         "+------------+\n" +
                         "| \"Italian\"  |\n" +
                         "| \"Lebanese\" |\n" +
                         "+------------+\n" +
                         "2 rows\n";

        assertEquals(expectedString, runQuery(controlQuery, Result::resultAsString));
    }
    
    @Test
    void sourceTargetIds() {
        String query = " MATCH (p:Person), (c:Cuisine)" +
                       " OPTIONAL MATCH (p)-[likes:LIKES]->(c)" +
                       " WITH {item:id(p), name: p.name, weights: collect(coalesce(likes.score, gds.util.NaN()))} as userData" +
                       " WITH collect(userData) as personCuisines" +
                       " WITH personCuisines," +
                       "      [value in personCuisines WHERE value.name IN [\"Praveena\", \"Arya\"] | value.item ] AS sourceIds" +
                       " CALL gds.alpha.similarity.cosine.stream({" +
                       "  data: personCuisines, " +
                       "  sourceIds: sourceIds, " +
                       "  topK: 1" +
                       " })" +
                       " YIELD item1, item2, similarity" +
                       " WITH gds.util.asNode(item1) AS from, gds.util.asNode(item2) AS to, similarity" +
                       " RETURN from.name AS from, to.name AS to, similarity" +
                       "  ORDER BY similarity DESC";

        String expectedString = "+---------------------------------------------+\n" +
                                "| from       | to        | similarity         |\n" +
                                "+---------------------------------------------+\n" +
                                "| \"Praveena\" | \"Karin\"   | 1.0                |\n" +
                                "| \"Arya\"     | \"Michael\" | 0.9788908326303921 |\n" +
                                "+---------------------------------------------+\n" +
                                "2 rows\n";

        assertEquals(expectedString, runQuery(query, Result::resultAsString));
        
    }

    @Test
    void embeddings() {
        String setEmbeddingsQuery = "CREATE (french:Cuisine {name:'French'})          SET french.embedding = [0.71, 0.33, 0.81, 0.52, 0.41]\n" +
                                    "CREATE (italian:Cuisine {name:'Italian'})        SET italian.embedding = [0.31, 0.72, 0.58, 0.67, 0.31]\n" +
                                    "CREATE (indian:Cuisine {name:'Indian'})          SET indian.embedding = [0.43, 0.26, 0.98, 0.51, 0.76]\n" +
                                    "CREATE (lebanese:Cuisine {name:'Lebanese'})      SET lebanese.embedding = [0.12, 0.23, 0.35, 0.31, 0.39]\n" +
                                    "CREATE (portuguese:Cuisine {name:'Portuguese'})  SET portuguese.embedding = [0.47, 0.98, 0.81, 0.72, 0.89]\n" +
                                    "CREATE (british:Cuisine {name:'British'})        SET british.embedding = [0.94, 0.12, 0.23, 0.4, 0.71]\n" +
                                    "CREATE (mauritian:Cuisine {name:'Mauritian'})    SET mauritian.embedding = [0.31, 0.56, 0.98, 0.21, 0.62]";

        runQuery(setEmbeddingsQuery);

        String query = " MATCH (c:Cuisine)" +
                       " WITH {item:id(c), weights: c.embedding} AS userData" +
                       " WITH collect(userData) AS data" +
                       " CALL gds.alpha.similarity.cosine.stream({" +
                       "  data: data," +
                       "  skipValue: null" +
                       " })" +
                       " YIELD item1, item2, count1, count2, similarity" +
                       " RETURN gds.util.asNode(item1).name AS from, gds.util.asNode(item2).name AS to, similarity" +
                       " ORDER BY similarity DESC";

        String expectedString = "+--------------------------------------------------+\n" +
                                "| from         | to           | similarity         |\n" +
                                "+--------------------------------------------------+\n" +
                                "| \"Lebanese\"   | \"Portuguese\" | 0.9671144333535775 |\n" +
                                "| \"Portuguese\" | \"Lebanese\"   | 0.9671144333535775 |\n" +
                                "| \"Indian\"     | \"Lebanese\"   | 0.9590440861639105 |\n" +
                                "| \"Lebanese\"   | \"Indian\"     | 0.9590440861639105 |\n" +
                                "| \"Italian\"    | \"Portuguese\" | 0.9582444106965535 |\n" +
                                "| \"Portuguese\" | \"Italian\"    | 0.9582444106965535 |\n" +
                                "| \"Indian\"     | \"Mauritian\"  | 0.9464344561993275 |\n" +
                                "| \"Mauritian\"  | \"Indian\"     | 0.9464344561993275 |\n" +
                                "| \"French\"     | \"Indian\"     | 0.9414524820541921 |\n" +
                                "| \"Indian\"     | \"French\"     | 0.9414524820541921 |\n" +
                                "| \"Portuguese\" | \"Mauritian\"  | 0.92092461331529   |\n" +
                                "| \"Mauritian\"  | \"Portuguese\" | 0.92092461331529   |\n" +
                                "| \"Lebanese\"   | \"Mauritian\"  | 0.9192477665074964 |\n" +
                                "| \"Mauritian\"  | \"Lebanese\"   | 0.9192477665074964 |\n" +
                                "| \"Italian\"    | \"Lebanese\"   | 0.9072862556290799 |\n" +
                                "| \"French\"     | \"Mauritian\"  | 0.8913504022120791 |\n" +
                                "| \"French\"     | \"Lebanese\"   | 0.8853775767967607 |\n" +
                                "| \"Italian\"    | \"French\"     | 0.8778358577197801 |\n" +
                                "| \"British\"    | \"French\"     | 0.8384644973081824 |\n" +
                                "| \"British\"    | \"Indian\"     | 0.7717276859027897 |\n" +
                                "| \"British\"    | \"Lebanese\"   | 0.7393113934601527 |\n" +
                                "+--------------------------------------------------+\n" +
                                "21 rows\n";

        assertEquals(expectedString, runQuery(query, Result::resultAsString));
    }

    @Test
    void cypherProjection() {
        String query = " WITH 'MATCH (person:Person)-[likes:LIKES]->(c)" +
                       "       RETURN id(person) AS item, id(c) AS category, likes.score AS weight' AS query" +
                       " CALL gds.alpha.similarity.cosine.write({" +
                       "  data: query," +
                       "  graph: 'cypher'," +
                       "  topK: 1," +
                       "  similarityCutoff: 0.1," +
                       "  write:true" +
                       " })" +
                       " YIELD nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, stdDev, p95" +
                       " RETURN nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, p95";

        String expectedString = "+----------------------------------------------------------------------------------------------------------------------------------------------------------+\n" +
                                "| nodes | similarityPairs | write | writeRelationshipType | writeProperty | min             | max                | mean               | p95                |\n" +
                                "+----------------------------------------------------------------------------------------------------------------------------------------------------------+\n" +
                                "| 5     | 5               | true  | \"SIMILAR\"             | \"score\"       | 0.9542236328125 | 1.0000038146972656 | 0.9824020385742187 | 1.0000038146972656 |\n" +
                                "+----------------------------------------------------------------------------------------------------------------------------------------------------------+\n" +
                                "1 row\n";

        assertEquals(expectedString, runQuery(query, Result::resultAsString));

    }

}

