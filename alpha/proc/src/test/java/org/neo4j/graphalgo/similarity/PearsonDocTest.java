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
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PearsonDocTest extends BaseProcTest {
    @Language("Cypher")
    private static final String DB_CYPHER =
        " MERGE (home_alone:Movie {name:'Home Alone'})" +
        " MERGE (matrix:Movie {name:'The Matrix'})" +
        " MERGE (good_men:Movie {name:'A Few Good Men'})" +
        " MERGE (top_gun:Movie {name:'Top Gun'})" +
        " MERGE (jerry:Movie {name:'Jerry Maguire'})" +
        " MERGE (gruffalo:Movie {name:'The Gruffalo'})" +

        " MERGE (zhen:Person {name: 'Zhen'})" +
        " MERGE (praveena:Person {name: 'Praveena'})" +
        " MERGE (michael:Person {name: 'Michael'})" +
        " MERGE (arya:Person {name: 'Arya'})" +
        " MERGE (karin:Person {name: 'Karin'})" +

        " MERGE (zhen)-[:RATED {score: 2}]->(home_alone)" +
        " MERGE (zhen)-[:RATED {score: 2}]->(good_men)" +
        " MERGE (zhen)-[:RATED {score: 3}]->(matrix)" +
        " MERGE (zhen)-[:RATED {score: 6}]->(jerry)" +

        " MERGE (praveena)-[:RATED {score: 6}]->(home_alone)" +
        " MERGE (praveena)-[:RATED {score: 7}]->(good_men)" +
        " MERGE (praveena)-[:RATED {score: 8}]->(matrix)" +
        " MERGE (praveena)-[:RATED {score: 9}]->(jerry)" +

        " MERGE (michael)-[:RATED {score: 7}]->(home_alone)" +
        " MERGE (michael)-[:RATED {score: 9}]->(good_men)" +
        " MERGE (michael)-[:RATED {score: 3}]->(jerry)" +
        " MERGE (michael)-[:RATED {score: 4}]->(top_gun)" +

        " MERGE (arya)-[:RATED {score: 8}]->(top_gun)" +
        " MERGE (arya)-[:RATED {score: 1}]->(matrix)" +
        " MERGE (arya)-[:RATED {score: 10}]->(jerry)" +
        " MERGE (arya)-[:RATED {score: 10}]->(gruffalo)" +

        " MERGE (karin)-[:RATED {score: 9}]->(top_gun)" +
        " MERGE (karin)-[:RATED {score: 7}]->(matrix)" +
        " MERGE (karin)-[:RATED {score: 7}]->(home_alone)" +
        " MERGE (karin)-[:RATED {score: 9}]->(gruffalo)";

    private static final String NL = System.lineSeparator();

    @BeforeEach
    void setUp() throws Exception {
        db = TestDatabaseCreator.createTestDatabase(builder -> {
                builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.*");
            }
        );
        registerProcedures(PearsonProc.class);
        registerFunctions(GetNodeFunc.class, SimilaritiesFunc.class, IsFiniteFunc.class);
        registerAggregationFunctions(SimilaritiesFunc.class);
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void functionCall() {
        runQueryWithRowConsumer(
            "RETURN gds.alpha.similarity.pearson([5,8,7,5,4,9], [7,8,6,6,4,5]) AS similarity",
            row -> assertEquals(0.28767798089123053, row.get("similarity"))
        );
    }
    
    @Test
    void functionCallOnGraph() {
        String query = " MATCH (p1:Person {name: 'Arya'})-[rated:RATED]->(movie)" +
                       " WITH p1, gds.alpha.similarity.asVector(movie, rated.score) AS p1Vector" +
                       " MATCH (p2:Person {name: 'Karin'})-[rated:RATED]->(movie)" +
                       " WITH p1, p2, p1Vector, gds.alpha.similarity.asVector(movie, rated.score) AS p2Vector" +
                       " RETURN p1.name AS from," +
                       "        p2.name AS to," +
                       "        gds.alpha.similarity.pearson(p1Vector, p2Vector, {vectorType: 'maps'}) AS similarity";

        String expectedString = "+---------------------------------------+" + NL +
                                "| from   | to      | similarity         |" + NL +
                                "+---------------------------------------+" + NL +
                                "| \"Arya\" | \"Karin\" | 0.8194651785206903 |" + NL +
                                "+---------------------------------------+" + NL +
                                "1 row" + NL;

        runQueryWithResultConsumer(query, result -> assertEquals(expectedString, result.resultAsString()));
    }

    @Test
    void functionCallOnWholeGraph() {
        String query = " MATCH (p1:Person {name: 'Arya'})-[rated:RATED]->(movie)" +
                       " WITH p1, gds.alpha.similarity.asVector(movie, rated.score) AS p1Vector" +
                       " MATCH (p2:Person)-[rated:RATED]->(movie) WHERE p2 <> p1" +
                       " WITH p1, p2, p1Vector, gds.alpha.similarity.asVector(movie, rated.score) AS p2Vector" +
                       " RETURN p1.name AS from," +
                       "        p2.name AS to," +
                       "        gds.alpha.similarity.pearson(p1Vector, p2Vector, {vectorType: \"maps\"}) AS similarity" +
                       " ORDER BY similarity DESC";

        String expectedString = "+-------------------------------------------+" + NL +
                                "| from   | to         | similarity          |" + NL +
                                "+-------------------------------------------+" + NL +
                                "| \"Arya\" | \"Karin\"    | 0.8194651785206903  |" + NL +
                                "| \"Arya\" | \"Zhen\"     | 0.4839533792540704  |" + NL +
                                "| \"Arya\" | \"Praveena\" | 0.09262336892949784 |" + NL +
                                "| \"Arya\" | \"Michael\"  | -0.9551953674747637 |" + NL +
                                "+-------------------------------------------+" + NL +
                                "4 rows" + NL;

        runQueryWithResultConsumer(query, result -> assertEquals(expectedString, result.resultAsString()));
    }

    @Test
    void stream() {
        String query = " MATCH (p:Person), (m:Movie)" +
                       " OPTIONAL MATCH (p)-[rated:RATED]->(m)" +
                       " WITH {item:id(p), weights: collect(coalesce(rated.score, gds.util.NaN()))} as userData" +
                       " WITH collect(userData) as data" +
                       " CALL gds.alpha.similarity.pearson.stream({" +
                       "    data: data," +
                       "    topK: 0" +
                       "})" +
                       " YIELD item1, item2, count1, count2, similarity" +
                       " RETURN gds.util.asNode(item1).name AS from, gds.util.asNode(item2).name AS to, similarity" +
                       " ORDER BY similarity DESC";

        String expectedString = "+-----------------------------------------------+" + NL +
                                "| from       | to         | similarity          |" + NL +
                                "+-----------------------------------------------+" + NL +
                                "| \"Zhen\"     | \"Praveena\" | 0.8865926413116155  |" + NL +
                                "| \"Zhen\"     | \"Karin\"    | 0.8320502943378437  |" + NL +
                                "| \"Arya\"     | \"Karin\"    | 0.8194651785206903  |" + NL +
                                "| \"Zhen\"     | \"Arya\"     | 0.4839533792540704  |" + NL +
                                "| \"Praveena\" | \"Karin\"    | 0.4472135954999579  |" + NL +
                                "| \"Praveena\" | \"Arya\"     | 0.09262336892949784 |" + NL +
                                "| \"Praveena\" | \"Michael\"  | -0.788492846568306  |" + NL +
                                "| \"Zhen\"     | \"Michael\"  | -0.9091365607973364 |" + NL +
                                "| \"Michael\"  | \"Arya\"     | -0.9551953674747637 |" + NL +
                                "| \"Michael\"  | \"Karin\"    | -0.9863939238321437 |" + NL +
                                "+-----------------------------------------------+" + NL +
                                "10 rows" + NL;

        runQueryWithResultConsumer(query, result -> assertEquals(expectedString, result.resultAsString()));
    }

    @Test
    void streamCutOff() {
        String query = " MATCH (p:Person), (m:Movie)" +
                       " OPTIONAL MATCH (p)-[rated:RATED]->(m)" +
                       " WITH {item:id(p), weights: collect(coalesce(rated.score, gds.util.NaN()))} as userData" +
                       " WITH collect(userData) as data" +
                       " CALL gds.alpha.similarity.pearson.stream({" +
                       "  data: data," +
                       "  similarityCutoff: 0.1," +
                       "  topK: 0" +
                       " })" +
                       " YIELD item1, item2, count1, count2, similarity" +
                       " RETURN gds.util.asNode(item1).name AS from, gds.util.asNode(item2).name AS to, similarity" +
                       " ORDER BY similarity DESC";

        String expectedString = "+----------------------------------------------+" + NL +
                                "| from       | to         | similarity         |" + NL +
                                "+----------------------------------------------+" + NL +
                                "| \"Zhen\"     | \"Praveena\" | 0.8865926413116155 |" + NL +
                                "| \"Zhen\"     | \"Karin\"    | 0.8320502943378437 |" + NL +
                                "| \"Arya\"     | \"Karin\"    | 0.8194651785206903 |" + NL +
                                "| \"Zhen\"     | \"Arya\"     | 0.4839533792540704 |" + NL +
                                "| \"Praveena\" | \"Karin\"    | 0.4472135954999579 |" + NL +
                                "+----------------------------------------------+" + NL +
                                "5 rows" + NL;

        runQueryWithResultConsumer(query, result -> assertEquals(expectedString, result.resultAsString()));
    }

    @Test
    void streamTopK() {
        String query = "  MATCH (p:Person), (m:Movie)" +
                       " OPTIONAL MATCH (p)-[rated:RATED]->(m)" +
                       " WITH {item:id(p), weights: collect(coalesce(rated.score, gds.util.NaN()))} as userData" +
                       " WITH collect(userData) as data" +
                       " CALL gds.alpha.similarity.pearson.stream({" +
                       "  data: data, " +
                       "  topK: 1, " +
                       "  similarityCutoff: 0.0" +
                       " })" +
                       " YIELD item1, item2, count1, count2, similarity" +
                       " RETURN gds.util.asNode(item1).name AS from, gds.util.asNode(item2).name AS to, similarity" +
                       " ORDER BY similarity DESC";

        String expectedString = "+----------------------------------------------+" + NL +
                                "| from       | to         | similarity         |" + NL +
                                "+----------------------------------------------+" + NL +
                                "| \"Zhen\"     | \"Praveena\" | 0.8865926413116155 |" + NL +
                                "| \"Praveena\" | \"Zhen\"     | 0.8865926413116155 |" + NL +
                                "| \"Karin\"    | \"Zhen\"     | 0.8320502943378437 |" + NL +
                                "| \"Arya\"     | \"Karin\"    | 0.8194651785206903 |" + NL +
                                "+----------------------------------------------+" + NL +
                                "4 rows" + NL;

        runQueryWithResultConsumer(query, result -> assertEquals(expectedString, result.resultAsString()));
    }
    
    @Test
    void write() {
        String query = " MATCH (p:Person), (m:Movie)" +
                       " OPTIONAL MATCH (p)-[rated:RATED]->(m)" +
                       " WITH {item:id(p), weights: collect(coalesce(rated.score, gds.util.NaN()))} as userData" +
                       " WITH collect(userData) as data" +
                       " CALL gds.alpha.similarity.pearson.write({" +
                       "  data: data," +
                       "  topK: 1, " +
                       "  similarityCutoff: 0.1, " +
                       "  write: true" +
                       " })" +
                       " YIELD nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, stdDev, p25, p50, p75, p90, p95, p99, p999, p100" +
                       " RETURN nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, p95";

        String expectedString = "+-------------------------------------------------------------------------------------------------------------------------------------------------------------+" + NL +
                                "| nodes | similarityPairs | write | writeRelationshipType | writeProperty | min                | max                | mean               | p95                |" + NL +
                                "+-------------------------------------------------------------------------------------------------------------------------------------------------------------+" + NL +
                                "| 5     | 4               | true  | \"SIMILAR\"             | \"score\"       | 0.8194618225097656 | 0.8865890502929688 | 0.8561716079711914 | 0.8865890502929688 |" + NL +
                                "+-------------------------------------------------------------------------------------------------------------------------------------------------------------+" + NL +
                                "1 row" + NL;

        runQueryWithResultConsumer(query, result -> assertEquals(expectedString, result.resultAsString()));

        String controlQuery = " MATCH (p:Person {name: 'Karin'})-[:SIMILAR]->(other)," +
                              "       (other)-[r:RATED]->(movie)" +
                              " WHERE not((p)-[:RATED]->(movie)) and r.score >= 5" +
                              " RETURN movie.name AS movie";

        String expectedString2 = "+-----------------+" + NL +
                                 "| movie           |" + NL +
                                 "+-----------------+" + NL +
                                 "| \"Jerry Maguire\" |" + NL +
                                 "+-----------------+" + NL +
                                 "1 row" + NL;

        runQueryWithResultConsumer(controlQuery, result -> assertEquals(expectedString2, result.resultAsString()));
    }

    @Test
    void sourceIds() {
        String query = " MATCH (p:Person), (m:Movie)" +
                       " OPTIONAL MATCH (p)-[rated:RATED]->(m)" +
                       " WITH {item:id(p), name: p.name, weights: collect(coalesce(rated.score, gds.util.NaN()))} as userData" +
                       " WITH collect(userData) as personCuisines" +
                       " WITH personCuisines," +
                       "      [value in personCuisines WHERE value.name IN [\"Praveena\", \"Arya\"] | value.item ] AS sourceIds" +
                       " CALL gds.alpha.similarity.pearson.stream({" +
                       "    data: personCuisines, " +
                       "    sourceIds: sourceIds, " +
                       "    topK: 1" +
                       " })" +
                       " YIELD item1, item2, similarity" +
                       " WITH gds.util.asNode(item1) AS from, gds.util.asNode(item2) AS to, similarity" +
                       " RETURN from.name AS from, to.name AS to, similarity" +
                       " ORDER BY similarity DESC";

        String expectedString = "+-------------------------------------------+" + NL +
                                "| from       | to      | similarity         |" + NL +
                                "+-------------------------------------------+" + NL +
                                "| \"Praveena\" | \"Zhen\"  | 0.8865926413116155 |" + NL +
                                "| \"Arya\"     | \"Karin\" | 0.8194651785206903 |" + NL +
                                "+-------------------------------------------+" + NL +
                                "2 rows" + NL;

        runQueryWithResultConsumer(query, result -> assertEquals(expectedString, result.resultAsString()));
    }

    @Test
    void embeddingGraph() {
        @Language("Cypher")
        String setEmbedding =
            "MERGE (home_alone:Movie {name:'Home Alone'})    SET home_alone.embedding = [0.71, 0.33, 0.81, 0.52, 0.41]" +
            "MERGE (matrix:Movie {name:'The Matrix'})        SET matrix.embedding = [0.31, 0.72, 0.58, 0.67, 0.31]" +
            "MERGE (good_men:Movie {name:'A Few Good Men'})  SET good_men.embedding = [0.43, 0.26, 0.98, 0.51, 0.76]" +
            "MERGE (top_gun:Movie {name:'Top Gun'})          SET top_gun.embedding = [0.12, 0.23, 0.35, 0.31, 0.3]" +
            "MERGE (jerry:Movie {name:'Jerry Maguire'})      SET jerry.embedding = [0.47, 0.98, 0.81, 0.72, 0]";

        runQuery(setEmbedding);

        String query = " MATCH (m:Movie)" +
                       " WITH {item:id(m), weights: m.embedding} as userData" +
                       " WITH collect(userData) as data" +
                       " CALL gds.alpha.similarity.pearson.stream({" +
                       "  data: data," +
                       "  skipValue: null," +
                       "  topK: 0" +
                       " })" +
                       " YIELD item1, item2, count1, count2, similarity" +
                       " RETURN gds.util.asNode(item1).name AS from, gds.util.asNode(item2).name AS to, similarity" +
                       " ORDER BY similarity DESC";

        String expectedString = "+------------------------------------------------------------+" + NL +
                                "| from             | to               | similarity           |" + NL +
                                "+------------------------------------------------------------+" + NL +
                                "| \"The Matrix\"     | \"Jerry Maguire\"  | 0.8689113641953199   |" + NL +
                                "| \"A Few Good Men\" | \"Top Gun\"        | 0.6846566091701214   |" + NL +
                                "| \"Home Alone\"     | \"A Few Good Men\" | 0.556559508845268    |" + NL +
                                "| \"The Matrix\"     | \"Top Gun\"        | 0.39320549183813097  |" + NL +
                                "| \"Home Alone\"     | \"Jerry Maguire\"  | 0.10026787755714502  |" + NL +
                                "| \"Top Gun\"        | \"Jerry Maguire\"  | 0.056232940630734043 |" + NL +
                                "| \"Home Alone\"     | \"Top Gun\"        | 0.006048691083898151 |" + NL +
                                "| \"Home Alone\"     | \"The Matrix\"     | -0.23435051666541426 |" + NL +
                                "| \"The Matrix\"     | \"A Few Good Men\" | -0.2545273235448378  |" + NL +
                                "| \"A Few Good Men\" | \"Jerry Maguire\"  | -0.31099199179883635 |" + NL +
                                "+------------------------------------------------------------+" + NL +
                                "10 rows" + NL;

        runQueryWithResultConsumer(query, result -> assertEquals(expectedString, result.resultAsString()));
    }
    
    @Test
    void cypherProjection() {
        String query = " WITH \"MATCH (person:Person)-[rated:RATED]->(c)" +
                       "       RETURN id(person) AS item, id(c) AS category, rated.score AS weight\" AS query" +
                       " CALL gds.alpha.similarity.pearson.write({" +
                       "  data: query," +
                       "  graph: 'cypher'," +
                       "  topK: 1," +
                       "  similarityCutoff: 0.1," +
                       "  write:true" +
                       " })" +
                       " YIELD nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, stdDev, p95" +
                       " RETURN nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, p95";

        String expectedString = "+-------------------------------------------------------------------------------------------------------------------------------------------------------------+" + NL +
                                "| nodes | similarityPairs | write | writeRelationshipType | writeProperty | min                | max                | mean               | p95                |" + NL +
                                "+-------------------------------------------------------------------------------------------------------------------------------------------------------------+" + NL +
                                "| 5     | 4               | true  | \"SIMILAR\"             | \"score\"       | 0.8194618225097656 | 0.8865890502929688 | 0.8561716079711914 | 0.8865890502929688 |" + NL +
                                "+-------------------------------------------------------------------------------------------------------------------------------------------------------------+" + NL +
                                "1 row" + NL;

        runQueryWithResultConsumer(query, result -> assertEquals(expectedString, result.resultAsString()));
    }
}
