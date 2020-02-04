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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GetNodeFunc;
import org.neo4j.graphalgo.IsFiniteFunc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class EuclideanDocTest extends BaseProcTest {

    public final String DB_CYPHER = " MERGE (french:Cuisine {name:'French'})" +
                                    " MERGE (italian:Cuisine {name:'Italian'})" +
                                    " MERGE (indian:Cuisine {name:'Indian'})" +
                                    " MERGE (lebanese:Cuisine {name:'Lebanese'})" +
                                    " MERGE (portuguese:Cuisine {name:'Portuguese'})" +
                                    " MERGE (british:Cuisine {name:'British'})" +
                                    " MERGE (mauritian:Cuisine {name:'Mauritian'})" +
                                    "" +
                                    " MERGE (zhen:Person {name: \"Zhen\"})" +
                                    " MERGE (praveena:Person {name: \"Praveena\"})" +
                                    " MERGE (michael:Person {name: \"Michael\"})" +
                                    " MERGE (arya:Person {name: \"Arya\"})" +
                                    " MERGE (karin:Person {name: \"Karin\"})" +
                                    
                                    " MERGE (praveena)-[:LIKES {score: 9}]->(indian)" +
                                    " MERGE (praveena)-[:LIKES {score: 7}]->(portuguese)" +
                                    " MERGE (praveena)-[:LIKES {score: 8}]->(british)" +
                                    " MERGE (praveena)-[:LIKES {score: 1}]->(mauritian)" +
                                    
                                    " MERGE (zhen)-[:LIKES {score: 10}]->(french)" +
                                    " MERGE (zhen)-[:LIKES {score: 6}]->(indian)" +
                                    " MERGE (zhen)-[:LIKES {score: 2}]->(british)" +
                                    
                                    " MERGE (michael)-[:LIKES {score: 8}]->(french)" +
                                    " MERGE (michael)-[:LIKES {score: 7}]->(italian)" +
                                    " MERGE (michael)-[:LIKES {score: 9}]->(indian)" +
                                    " MERGE (michael)-[:LIKES {score: 3}]->(portuguese)" +
                                    
                                    " MERGE (arya)-[:LIKES {score: 10}]->(lebanese)" +
                                    " MERGE (arya)-[:LIKES {score: 10}]->(italian)" +
                                    " MERGE (arya)-[:LIKES {score: 7}]->(portuguese)" +
                                    " MERGE (arya)-[:LIKES {score: 9}]->(mauritian)" +
                                    
                                    " MERGE (karin)-[:LIKES {score: 9}]->(lebanese)" +
                                    " MERGE (karin)-[:LIKES {score: 7}]->(italian)" +
                                    " MERGE (karin)-[:LIKES {score: 10}]->(portuguese)";

    @BeforeEach
    void setupGraph() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase(builder ->
            builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.*")
        );

        registerProcedures(EuclideanProc.class);
        registerFunctions(GetNodeFunc.class, SimilaritiesFunc.class, IsFiniteFunc.class);
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void clearCommunities() {
        db.shutdown();
        GraphCatalog.removeAllLoadedGraphs();
    }

    @Test
    void functionCall() {
        String query = "RETURN gds.alpha.similarity.euclideanDistance([3,8,7,5,2,9], [10,8,6,6,4,5]) AS similarity";

        runQueryWithRowConsumer(query, row -> assertEquals(8.426149773176359 ,row.get("similarity")));
    }

    @Test
    void functionCallOnGraph() {
        String query =
            " MATCH (p1:Person {name: 'Zhen'})-[likes1:LIKES]->(cuisine)" +
            " MATCH (p2:Person {name: 'Praveena'})-[likes2:LIKES]->(cuisine)" +
            " RETURN p1.name AS from," +
            "        p2.name AS to," +
            "        gds.alpha.similarity.euclideanDistance(collect(likes1.score), collect(likes2.score)) AS similarity";

        String expectedResult = "+-----------------------------------------+\n" +
                                "| from   | to         | similarity        |\n" +
                                "+-----------------------------------------+\n" +
                                "| \"Zhen\" | \"Praveena\" | 6.708203932499369 |\n" +
                                "+-----------------------------------------+\n" +
                                "1 row\n";

        runQueryWithResultConsumer(query, result-> assertEquals(expectedResult, result.resultAsString()));
    }

    @Test
    void functionCallOnWholeGraph() {
        String query =
            " MATCH (p1:Person {name: 'Zhen'})-[likes1:LIKES]->(cuisine)" +
            " MATCH (p2:Person)-[likes2:LIKES]->(cuisine) WHERE p2 <> p1" +
            " RETURN p1.name AS from," +
            "        p2.name AS to," +
            "        gds.alpha.similarity.euclideanDistance(collect(likes1.score), collect(likes2.score)) AS similarity" +
            " ORDER BY similarity DESC";

        String expectedResult = "+-----------------------------------------+\n" +
                                "| from   | to         | similarity        |\n" +
                                "+-----------------------------------------+\n" +
                                "| \"Zhen\" | \"Praveena\" | 6.708203932499369 |\n" +
                                "| \"Zhen\" | \"Michael\"  | 3.605551275463989 |\n" +
                                "+-----------------------------------------+\n" +
                                "2 rows\n";

        runQueryWithResultConsumer(query, result-> assertEquals(expectedResult, result.resultAsString()));
    }

    @Test
    void stream() {
        String query =
            " MATCH (p:Person), (c:Cuisine)" +
            " OPTIONAL MATCH (p)-[likes:LIKES]->(c)" +
            " WITH {item:id(p), weights: collect(coalesce(likes.score, gds.util.NaN()))} AS userData" +
            " WITH collect(userData) AS data" +
            " CALL gds.alpha.similarity.euclidean.stream({" +
            "   data: data," +
            "   topK: 0" +
            " })" +
            " YIELD item1, item2, count1, count2, similarity" +
            " RETURN gds.util.asNode(item1).name AS from, gds.util.asNode(item2).name AS to, similarity" +
            " ORDER BY similarity";

        String expectedResult = "+---------------------------------------------+\n" +
                                "| from       | to         | similarity        |\n" +
                                "+---------------------------------------------+\n" +
                                "| \"Praveena\" | \"Karin\"    | 3.0               |\n" +
                                "| \"Zhen\"     | \"Michael\"  | 3.605551275463989 |\n" +
                                "| \"Praveena\" | \"Michael\"  | 4.0               |\n" +
                                "| \"Arya\"     | \"Karin\"    | 4.358898943540674 |\n" +
                                "| \"Michael\"  | \"Arya\"     | 5.0               |\n" +
                                "| \"Zhen\"     | \"Praveena\" | 6.708203932499369 |\n" +
                                "| \"Michael\"  | \"Karin\"    | 7.0               |\n" +
                                "| \"Praveena\" | \"Arya\"     | 8.0               |\n" +
                                "| \"Zhen\"     | \"Arya\"     | NaN               |\n" +
                                "| \"Zhen\"     | \"Karin\"    | NaN               |\n" +
                                "+---------------------------------------------+\n" +
                                "10 rows\n";

        runQueryWithResultConsumer(query, result-> assertEquals(expectedResult, result.resultAsString()));
    }

    @Test
    void streamFinite() {
        String query =
            "  MATCH (p:Person), (c:Cuisine)" +
            " OPTIONAL MATCH (p)-[likes:LIKES]->(c)" +
            " WITH {item:id(p), weights: collect(coalesce(likes.score, gds.util.NaN()))} as userData" +
            " WITH collect(userData) as data" +
            " CALL gds.alpha.similarity.euclidean.stream({" +
            "   data: data," +
            "   topK: 0" +
            " })" +
            " YIELD item1, item2, count1, count2, similarity" +
            " WHERE gds.util.isFinite(similarity)" +
            " RETURN gds.util.asNode(item1).name AS from, gds.util.asNode(item2).name AS to, similarity" +
            " ORDER BY similarity";

        String expectedResult = "+---------------------------------------------+\n" +
                                "| from       | to         | similarity        |\n" +
                                "+---------------------------------------------+\n" +
                                "| \"Praveena\" | \"Karin\"    | 3.0               |\n" +
                                "| \"Zhen\"     | \"Michael\"  | 3.605551275463989 |\n" +
                                "| \"Praveena\" | \"Michael\"  | 4.0               |\n" +
                                "| \"Arya\"     | \"Karin\"    | 4.358898943540674 |\n" +
                                "| \"Michael\"  | \"Arya\"     | 5.0               |\n" +
                                "| \"Zhen\"     | \"Praveena\" | 6.708203932499369 |\n" +
                                "| \"Michael\"  | \"Karin\"    | 7.0               |\n" +
                                "| \"Praveena\" | \"Arya\"     | 8.0               |\n" +
                                "+---------------------------------------------+\n" +
                                "8 rows\n";

        runQueryWithResultConsumer(query, result-> assertEquals(expectedResult, result.resultAsString()));
    }

    @Test
    void streamSimilarityCutoff()
    {
        String query = 
            " MATCH (p:Person), (c:Cuisine)" +
            " OPTIONAL MATCH (p)-[likes:LIKES]->(c)" +
            " WITH {item:id(p), weights: collect(coalesce(likes.score, gds.util.NaN()))} as userData" +
            " WITH collect(userData) as data" +
            " CALL gds.alpha.similarity.euclidean.stream({" +
            "  data: data," +
            "  similarityCutoff: 4.0," +
            "  topK: 0" +
            " })" +
            " YIELD item1, item2, count1, count2, similarity" +
            " WHERE gds.util.isFinite(similarity)" +
            " RETURN gds.util.asNode(item1).name AS from, gds.util.asNode(item2).name AS to, similarity" +
            " ORDER BY similarity";

        String expectedResult = "+--------------------------------------------+\n" +
                                "| from       | to        | similarity        |\n" +
                                "+--------------------------------------------+\n" +
                                "| \"Praveena\" | \"Karin\"   | 3.0               |\n" +
                                "| \"Zhen\"     | \"Michael\" | 3.605551275463989 |\n" +
                                "| \"Praveena\" | \"Michael\" | 4.0               |\n" +
                                "+--------------------------------------------+\n" +
                                "3 rows\n";

        runQueryWithResultConsumer(query, result-> assertEquals(expectedResult, result.resultAsString()));
    }

    @Test
    void streamTopK()
    {
        String query =
            " MATCH (p:Person), (c:Cuisine)" +
            " OPTIONAL MATCH (p)-[likes:LIKES]->(c)" +
            " WITH {item:id(p), weights: collect(coalesce(likes.score, gds.util.NaN()))} AS userData" +
            " WITH collect(userData) AS data" +
            " CALL gds.alpha.similarity.euclidean.stream({" +
            "  data: data," +
            "  topK: 1" +
            " })" +
            " YIELD item1, item2, count1, count2, similarity" +
            " RETURN gds.util.asNode(item1).name AS from, gds.util.asNode(item2).name AS to, similarity" +
            " ORDER BY from";

        String expectedResult = "+---------------------------------------------+\n" +
                                "| from       | to         | similarity        |\n" +
                                "+---------------------------------------------+\n" +
                                "| \"Arya\"     | \"Karin\"    | 4.358898943540674 |\n" +
                                "| \"Karin\"    | \"Praveena\" | 3.0               |\n" +
                                "| \"Michael\"  | \"Zhen\"     | 3.605551275463989 |\n" +
                                "| \"Praveena\" | \"Karin\"    | 3.0               |\n" +
                                "| \"Zhen\"     | \"Michael\"  | 3.605551275463989 |\n" +
                                "+---------------------------------------------+\n" +
                                "5 rows\n";

        runQueryWithResultConsumer(query, result-> assertEquals(expectedResult, result.resultAsString()));
    }

    @Test
    void write()
    {
        String query =
            " MATCH (p:Person), (c:Cuisine)" +
            " OPTIONAL MATCH (p)-[likes:LIKES]->(c)" +
            " WITH {item:id(p), weights: collect(coalesce(likes.score, gds.util.NaN()))} AS userData" +
            " WITH collect(userData) AS data" +
            " CALL gds.alpha.similarity.euclidean.write({" +
            "  data: data," +
            "  topK: 1," +
            "  write:true" +
            " })" +
            " YIELD nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, stdDev, p25, p50, p75, p90, p95, p99, p999, p100" +
            " RETURN nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, p95";

        String expectedResult = "+----------------------------------------------------------------------------------------------------------------------------------------------+\n" +
                                "| nodes | similarityPairs | write | writeRelationshipType | writeProperty | min | max                | mean               | p95                |\n" +
                                "+----------------------------------------------------------------------------------------------------------------------------------------------+\n" +
                                "| 5     | 5               | true  | \"SIMILAR\"             | \"score\"       | 3.0 | 4.3589019775390625 | 3.5139984130859374 | 4.3589019775390625 |\n" +
                                "+----------------------------------------------------------------------------------------------------------------------------------------------+\n" +
                                "1 row\n";

        runQueryWithResultConsumer(query, result-> assertEquals(expectedResult, result.resultAsString()));

        String controlQuery = " MATCH (p:Person {name: \"Praveena\"})-[:SIMILAR]->(other)," +
                              "       (other)-[:LIKES]->(cuisine)" +
                              " WHERE not((p)-[:LIKES]->(cuisine))" +
                              " RETURN cuisine.name AS cuisine";

        String expectedResult2 = "+------------+\n" +
                                 "| cuisine    |\n" +
                                 "+------------+\n" +
                                 "| \"Italian\"  |\n" +
                                 "| \"Lebanese\" |\n" +
                                 "+------------+\n" +
                                 "2 rows\n";

        runQueryWithResultConsumer(controlQuery, result -> assertEquals(expectedResult2, result.resultAsString()));
    }

    @Test
    void sourceIds()
    {
        String query =
            " MATCH (p:Person), (c:Cuisine)" +
            " OPTIONAL MATCH (p)-[likes:LIKES]->(c)" +
            " WITH {item:id(p), name: p.name, weights: collect(coalesce(likes.score, gds.util.NaN()))} AS userData" +
            " WITH collect(userData) AS personCuisines" +
            " WITH personCuisines," +
            "      [value IN personCuisines WHERE value.name IN ['Praveena', 'Arya'] | value.item ] AS sourceIds" +
            " CALL gds.alpha.similarity.euclidean.stream({" +
            "   data: personCuisines," +
            "   sourceIds: sourceIds," +
            "   topK: 1" +
            " })" +
            " YIELD item1, item2, similarity" +
            " WITH gds.util.asNode(item1) AS from, gds.util.asNode(item2) AS to, similarity" +
            " RETURN from.name AS from, to.name AS to, similarity" +
            " ORDER BY similarity DESC";

        String expectedResult = "+------------------------------------------+\n" +
                                "| from       | to      | similarity        |\n" +
                                "+------------------------------------------+\n" +
                                "| \"Arya\"     | \"Karin\" | 4.358898943540674 |\n" +
                                "| \"Praveena\" | \"Karin\" | 3.0               |\n" +
                                "+------------------------------------------+\n" +
                                "2 rows\n";

        runQueryWithResultConsumer(query, result-> assertEquals(expectedResult, result.resultAsString()));
    }

    @Test
    void embeddings()
    {
        String embeddingsQuery =
            " MERGE (french:Cuisine {name:'French'})          SET french.embedding = [0.71, 0.33, 0.81, 0.52, 0.41]" +
            " MERGE (italian:Cuisine {name:'Italian'})        SET italian.embedding = [0.31, 0.72, 0.58, 0.67, 0.31]" +
            " MERGE (indian:Cuisine {name:'Indian'})          SET indian.embedding = [0.43, 0.26, 0.98, 0.51, 0.76]" +
            " MERGE (lebanese:Cuisine {name:'Lebanese'})      SET lebanese.embedding = [0.12, 0.23, 0.35, 0.31, 0.39]" +
            " MERGE (portuguese:Cuisine {name:'Portuguese'})  SET portuguese.embedding = [0.47, 0.98, 0.81, 0.72, 0.89]" +
            " MERGE (british:Cuisine {name:'British'})        SET british.embedding = [0.94, 0.12, 0.23, 0.4, 0.71]" +
            " MERGE (mauritian:Cuisine {name:'Mauritian'})    SET mauritian.embedding = [0.31, 0.56, 0.98, 0.21, 0.62]";
        
        runQuery(embeddingsQuery);

        String query =
            " MATCH (c:Cuisine)" +
            " WITH {item:id(c), weights: c.embedding} AS userData" +
            " WITH collect(userData) AS data" +
            " CALL gds.alpha.similarity.euclidean.stream({" +
            "  data: data," +
            "  skipValue: null" +
            " })" +
            " YIELD item1, item2, count1, count2, similarity" +
            " RETURN gds.util.asNode(item1).name AS from, gds.util.asNode(item2).name AS to, similarity" +
            " ORDER BY similarity DESC";

        String expectedResult = "+---------------------------------------------------+\n" +
                                "| from         | to           | similarity          |\n" +
                                "+---------------------------------------------------+\n" +
                                "| \"British\"    | \"Indian\"     | 0.9256349172324908  |\n" +
                                "| \"British\"    | \"Lebanese\"   | 0.8996666049153985  |\n" +
                                "| \"Lebanese\"   | \"French\"     | 0.783709129715866   |\n" +
                                "| \"Indian\"     | \"Portuguese\" | 0.7809609465267774  |\n" +
                                "| \"Portuguese\" | \"Indian\"     | 0.7809609465267774  |\n" +
                                "| \"Lebanese\"   | \"Mauritian\"  | 0.7776888838089432  |\n" +
                                "| \"Portuguese\" | \"Mauritian\"  | 0.7509327533141699  |\n" +
                                "| \"British\"    | \"French\"     | 0.7333484846919642  |\n" +
                                "| \"Mauritian\"  | \"Italian\"    | 0.7023531875061151  |\n" +
                                "| \"Italian\"    | \"Portuguese\" | 0.696419413859206   |\n" +
                                "| \"Portuguese\" | \"Italian\"    | 0.696419413859206   |\n" +
                                "| \"Italian\"    | \"Lebanese\"   | 0.6819824044651006  |\n" +
                                "| \"Lebanese\"   | \"Italian\"    | 0.6819824044651006  |\n" +
                                "| \"French\"     | \"Italian\"    | 0.6304760106459246  |\n" +
                                "| \"Italian\"    | \"French\"     | 0.6304760106459246  |\n" +
                                "| \"French\"     | \"Mauritian\"  | 0.6180614856144977  |\n" +
                                "| \"Mauritian\"  | \"French\"     | 0.6180614856144977  |\n" +
                                "| \"French\"     | \"Indian\"     | 0.48456165758342873 |\n" +
                                "| \"Indian\"     | \"French\"     | 0.48456165758342873 |\n" +
                                "| \"Indian\"     | \"Mauritian\"  | 0.46260134024881516 |\n" +
                                "| \"Mauritian\"  | \"Indian\"     | 0.46260134024881516 |\n" +
                                "+---------------------------------------------------+\n" +
                                "21 rows\n";

        runQueryWithResultConsumer(query, result-> assertEquals(expectedResult, result.resultAsString()));
    }

    @Test
    void cypherProjection() {
        String query =
            " WITH \"MATCH (person:Person)-[likes:LIKES]->(c)\n" +
            "      RETURN id(person) AS item, id(c) AS category, likes.score AS weight\" AS query" +
            " CALL gds.alpha.similarity.euclidean.write({" +
            "  data: query," +
            "  graph: 'cypher'," +
            "  topK: 1," +
            "  similarityCutoff: 4.0," +
            "  write:true" +
            " })" +
            " YIELD nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, stdDev, p95" +
            " RETURN nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, p95";

        String expectedResult = "+---------------------------------------------------------------------------------------------------------------------------------------------+\n" +
                                "| nodes | similarityPairs | write | writeRelationshipType | writeProperty | min | max                | mean              | p95                |\n" +
                                "+---------------------------------------------------------------------------------------------------------------------------------------------+\n" +
                                "| 5     | 5               | true  | \"SIMILAR\"             | \"score\"       | 0.0 | 3.6055450439453125 | 2.642218017578125 | 3.6055450439453125 |\n" +
                                "+---------------------------------------------------------------------------------------------------------------------------------------------+\n" +
                                "1 row\n";
        
        runQueryWithResultConsumer(query, result-> assertEquals(expectedResult, result.resultAsString()));
    }
}
