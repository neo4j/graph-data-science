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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Result;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SimilaritiesFuncTest extends BaseProcTest {

    private static final String DB_CYPHER =
            "CREATE (java:Skill{name:'Java'})\n" +
            "CREATE (neo4j:Skill{name:'Neo4j'})\n" +
            "CREATE (nodejs:Skill{name:'NodeJS'})\n" +
            "CREATE (scala:Skill{name:'Scala'})\n" +
            "CREATE (jim:Employee{name:'Jim'})\n" +
            "CREATE (bob:Employee{name:'Bob'})\n" +
            "CREATE (role:Role {name:'Role 1-Analytics Manager'})\n" +
            "\n" +
            "CREATE (role)-[:REQUIRES_SKILL{proficiency:8.54}]->(java)\n" +
            "CREATE (role)-[:REQUIRES_SKILL{proficiency:4.3}]->(scala)\n" +
            "CREATE (role)-[:REQUIRES_SKILL{proficiency:9.75}]->(neo4j)\n" +
            "\n" +
            "CREATE (bob)-[:HAS_SKILL{proficiency:10}]->(java)\n" +
            "CREATE (bob)-[:HAS_SKILL{proficiency:7.5}]->(neo4j)\n" +
            "CREATE (bob)-[:HAS_SKILL]->(scala)\n" +
            "CREATE (jim)-[:HAS_SKILL{proficiency:8.25}]->(java)\n" +
            "CREATE (jim)-[:HAS_SKILL{proficiency:7.1}]->(scala)";

    // cosine similarity taken from here: https://neo4j.com/graphgist/a7c915c8-a3d6-43b9-8127-1836fecc6e2f
    // euclid distance taken from here: https://neo4j.com/blog/real-time-recommendation-engine-data-science/
    // euclid similarity taken from here: http://stats.stackexchange.com/a/158285
    // pearson similarity taken from here: http://guides.neo4j.com/sandbox/recommendations

    @BeforeEach
    void setUp() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerFunctions(SimilaritiesFunc.class);
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    public void testCosineSimilarityWithSomeWeightPropertiesNull() {
        String controlQuery =
                "MATCH (p1:Employee)-[x:HAS_SKILL]->(sk:Skill)<-[y:REQUIRES_SKILL] -(p2:Role {name:'Role 1-Analytics Manager'})\n" +
                        "WITH SUM(x.proficiency * y.proficiency) AS xyDotProduct,\n" +
                        "SQRT(REDUCE(xDot = 0.0, a IN COLLECT(x.proficiency) | xDot + a^2)) AS xLength,\n" +
                        "SQRT(REDUCE(yDot = 0.0, b IN COLLECT(y.proficiency) | yDot + b^2)) AS yLength,\n" +
                        "p1, p2\n" +
                        "WITH  p1.name as name, xyDotProduct / (xLength * yLength) as cosineSim\n" +
                        "ORDER BY name ASC\n" +
                        "WITH name, toFloat(cosineSim*10000.0) AS cosineSim\n" +
                        "RETURN name, toString(toInteger(cosineSim)/10000.0) as cosineSim";
        Result result1 = runQuery(controlQuery);
        String bobSimilarity = (String) result1.next().get("cosineSim");
        String jimSimilarity = (String) result1.next().get("cosineSim");

        Result result2 = runQuery(
                "MATCH (p1:Employee)-[x:HAS_SKILL]->(sk:Skill)<-[y:REQUIRES_SKILL]-(p2:Role {name:'Role 1-Analytics Manager'})\n" +
                        "WITH p1, COLLECT(coalesce(x.proficiency, 0.0d)) as v1, COLLECT(coalesce(y.proficiency, 0.0d)) as v2\n" +
                        "WITH p1.name as name, algo.similarity.cosine(v1, v2) as cosineSim ORDER BY name ASC\n" +
                        "RETURN name, toString(toInteger(cosineSim*10000)/10000.0) as cosineSim");
        assertEquals(bobSimilarity, result2.next().get("cosineSim"));
        assertEquals(jimSimilarity, result2.next().get("cosineSim"));
    }

    @Test
    public void testCosineSimilarityWithSomeRelationshipsNull() {
        String controlQuery =
                "MATCH (p1:Employee)\n" +
                        "MATCH (sk:Skill)<-[y:REQUIRES_SKILL] -(p2:Role {name:'Role 1-Analytics Manager'})\n" +
                        "OPTIONAL MATCH (p1)-[x:HAS_SKILL]->(sk)\n" +
                        "WITH SUM(x.proficiency * y.proficiency) AS xyDotProduct,\n" +
                        "SQRT(REDUCE(xDot = 0.0, a IN COLLECT(x.proficiency) | xDot + a^2)) AS xLength,\n" +
                        "SQRT(REDUCE(yDot = 0.0, b IN COLLECT(y.proficiency) | yDot + b^2)) AS yLength,\n" +
                        "p1, p2\n" +
                        "WITH  p1.name as name, xyDotProduct / (xLength * yLength) as cosineSim\n" +
                        "ORDER BY name ASC\n" +
                        "WITH name, toFloat(cosineSim*10000.0) AS cosineSim\n" +
                        "RETURN name, toString(toInteger(cosineSim)/10000.0) as cosineSim";
        Result result1 = runQuery(controlQuery);
        String bobSimilarity = (String) result1.next().get("cosineSim");
        String jimSimilarity = (String) result1.next().get("cosineSim");

        Result result2 = runQuery(
                "MATCH (sk:Skill)<-[y:REQUIRES_SKILL]-(p2:Role {name:'Role 1-Analytics Manager'})\n" +
                        "MATCH (p1:Employee)\n" +
                        "OPTIONAL MATCH (p1)-[x:HAS_SKILL]->(sk)\n" +
                        "WITH p1, COLLECT(coalesce(x.proficiency, 0.0d)) as v1, COLLECT(coalesce(y.proficiency, 0.0d)) as v2\n" +
                        "WITH p1.name as name, algo.similarity.cosine(v1, v2) as cosineSim ORDER BY name ASC\n" +
                        "RETURN name, toString(toInteger(cosineSim*10000)/10000.0) as cosineSim");

        assertEquals(bobSimilarity, result2.next().get("cosineSim"));
        assertEquals(jimSimilarity, result2.next().get("cosineSim"));
    }

    @Test
    public void testPearsonSimilarityWithSomeRelationshipsNull() {
        String controlQuery =
                "MATCH (p2:Role {name:'Role 1-Analytics Manager'})-[s:REQUIRES_SKILL]->(:Skill) WITH p2, avg(s.proficiency) AS p2Mean " +
                "MATCH (p1:Employee)\n" +
                "MATCH (sk:Skill)<-[y:REQUIRES_SKILL] -(p2)\n" +
                "OPTIONAL MATCH (p1)-[x:HAS_SKILL]->(sk)\n" +
                "WITH p2, p2Mean, p1, avg(coalesce(x.proficiency,0)) AS p1Mean, collect({r1: coalesce(x.proficiency, 0), r2: y.proficiency}) AS ratings " +
                "UNWIND ratings AS r\n" +
                        "WITH sum( (r.r1-p1Mean) * (r.r2-p2Mean) ) AS nom,\n" +
                        "     sqrt( sum( (r.r1 - p1Mean)^2) * sum( (r.r2 - p2Mean) ^2)) AS denom,\n" +
                        "     p1, p2 \n" +
                        "WHERE denom > 0  " +
                        "WITH p1.name AS name, nom/denom AS pearson ORDER BY name ASC "+
                "RETURN name, toString(toInteger(pearson*10000)/10000.0) as pearsonSim";

        Result result1 = runQuery(controlQuery);
        String bobSimilarity = (String) result1.next().get("pearsonSim");
        String jimSimilarity = (String) result1.next().get("pearsonSim");

        Result result2 = runQuery(
                "MATCH (sk:Skill)<-[y:REQUIRES_SKILL]-(p2:Role {name:'Role 1-Analytics Manager'})\n" +
                        "MATCH (p1:Employee)\n" +
                        "OPTIONAL MATCH (p1)-[x:HAS_SKILL]->(sk)\n" +
                        "WITH p1, COLLECT(coalesce(x.proficiency, 0.0d)) as v1, COLLECT(coalesce(y.proficiency, 0.0d)) as v2\n" +
                        "WITH p1.name as name, algo.similarity.pearson(v1, v2) as pearsonSim ORDER BY name ASC\n" +
                        "RETURN name, toString(toInteger(pearsonSim*10000)/10000.0) as pearsonSim");

        assertEquals(bobSimilarity, result2.next().get("pearsonSim"));
        assertEquals(jimSimilarity, result2.next().get("pearsonSim"));

    }

    @Test
    public void testEuclideanDistance() {
        String controlQuery =
                "MATCH (p1:Employee)\n" +
                        "MATCH (sk:Skill)<-[y:REQUIRES_SKILL] -(p2:Role {name:'Role 1-Analytics Manager'})\n" +
                        "OPTIONAL MATCH (p1)-[x:HAS_SKILL]->(sk)\n" +
                        "WITH SQRT(SUM((coalesce(x.proficiency,0) - coalesce(y.proficiency, 0))^2)) AS euclidDist, p1, p2\n" +
                        "ORDER BY p1.name ASC\n" +
                        "RETURN p1.name, toString(toInteger(euclidDist*10000)/10000.0) as euclidDist";
        Result result1 = runQuery(controlQuery);
        String bobDist = (String) result1.next().get("euclidDist");
        String jimDist = (String) result1.next().get("euclidDist");

        Result result2 = runQuery(
                "MATCH (sk:Skill)<-[y:REQUIRES_SKILL]-(p2:Role {name:'Role 1-Analytics Manager'})\n" +
                        "MATCH (p1:Employee)\n" +
                        "OPTIONAL MATCH (p1)-[x:HAS_SKILL]->(sk)\n" +
                        "WITH p1, COLLECT(coalesce(x.proficiency, 0.0d)) as v1, COLLECT(coalesce(y.proficiency, 0.0d)) as v2\n" +
                        "WITH p1.name as name, algo.similarity.euclideanDistance(v1, v2) as euclidDist ORDER BY name ASC\n" +
                        "RETURN name, toString(toInteger(euclidDist*10000)/10000.0) as euclidDist");

        assertEquals(bobDist, result2.next().get("euclidDist"));
        assertEquals(jimDist, result2.next().get("euclidDist"));
    }

    @Test
    public void testJaccardSimilarity() {
        String controlQuery =
                "MATCH (p1:Employee)-[:HAS_SKILL]->(sk)<-[:HAS_SKILL]-(p2)\n" +
                "WITH p1,p2,size((p1)-[:HAS_SKILL]->()) as d1, size((p2)-[:HAS_SKILL]->()) as d2, count(distinct sk) as intersection\n" +
                "WITH p1.name as name1, p2.name as name2, toFloat(intersection) / (d1+d2-intersection) as jaccardSim\n" +
                "ORDER BY name1,name2\n" +
                "RETURN name1,name2, toString(toInteger(jaccardSim*10000)/10000.0) as jaccardSim";
        Result result1 = runQuery(controlQuery);
        String bobSim = (String) result1.next().get("jaccardSim");
        String jimSim = (String) result1.next().get("jaccardSim");

        Result result2 = runQuery(
                        "MATCH (p1:Employee),(p2:Employee) WHERE p1 <> p2\n" +
                        "WITH p1, [(p1)-[:HAS_SKILL]->(sk) | id(sk)] as v1, p2, [(p2)-[:HAS_SKILL]->(sk) | id(sk)] as v2\n" +
                        "WITH p1.name as name1, p2.name as name2, algo.similarity.jaccard(v1, v2) as jaccardSim ORDER BY name1,name2\n" +
                        "RETURN name1, name2, toString(toInteger(jaccardSim*10000)/10000.0) as jaccardSim");

        assertEquals(bobSim, result2.next().get("jaccardSim"));
        assertEquals(jimSim, result2.next().get("jaccardSim"));
    }
    
    @Test
    public void testOverlapSimilarity() {
        String controlQuery =
                "MATCH (p1:Employee)-[:HAS_SKILL]->(sk)<-[:HAS_SKILL]-(p2)\n" +
                        "WITH p1,p2,size((p1)-[:HAS_SKILL]->()) as d1, size((p2)-[:HAS_SKILL]->()) as d2, count(distinct sk) as intersection\n" +
                        "WITH p1.name as name1, p2.name as name2, toFloat(intersection) / CASE WHEN d1 > d2 THEN d2 ELSE d1 END as overlapSim\n" +
                        "ORDER BY name1,name2\n" +
                        "RETURN name1,name2, toString(toInteger(overlapSim*10000)/10000.0) as overlapSim";
        Result results = runQuery(controlQuery);
        String bobSim = (String) results.next().get("overlapSim");
        String jimSim = (String) results.next().get("overlapSim");

        Result result2 = runQuery(
                "MATCH (p1:Employee),(p2:Employee) WHERE p1 <> p2\n" +
                        "WITH p1, [(p1)-[:HAS_SKILL]->(sk) | id(sk)] as v1, p2, [(p2)-[:HAS_SKILL]->(sk) | id(sk)] as v2\n" +
                        "WITH p1.name as name1, p2.name as name2, algo.similarity.overlap(v1, v2) as overlapSim ORDER BY name1,name2\n" +
                        "RETURN name1, name2, toString(toInteger(overlapSim*10000)/10000.0) as overlapSim");

        assertEquals(bobSim, result2.next().get("overlapSim"));
        assertEquals(jimSim, result2.next().get("overlapSim"));
    }

    @Test
    public void testEuclideanSimilarity() {
        String controlQuery =
                "MATCH (p1:Employee)\n" +
                        "MATCH (sk:Skill)<-[y:REQUIRES_SKILL] -(p2:Role {name:'Role 1-Analytics Manager'})\n" +
                        "OPTIONAL MATCH (p1)-[x:HAS_SKILL]->(sk)\n" +
                        "WITH SQRT(SUM((coalesce(x.proficiency,0) - coalesce(y.proficiency, 0))^2)) AS euclidDist, p1\n" +
                        "WITH p1.name as name, 1 / (1 + euclidDist) as euclidSim\n" +
                        "ORDER BY name ASC\n" +
                        "RETURN name, toString(toInteger(euclidSim*10000)/10000.0) as euclidSim";
        Result result1 = runQuery(controlQuery);
        String bobSim = (String) result1.next().get("euclidSim");
        String jimSim = (String) result1.next().get("euclidSim");

        Result result2 = runQuery(
                "MATCH (sk:Skill)<-[y:REQUIRES_SKILL]-(p2:Role {name:'Role 1-Analytics Manager'})\n" +
                        "MATCH (p1:Employee)\n" +
                        "OPTIONAL MATCH (p1)-[x:HAS_SKILL]->(sk)\n" +
                        "WITH p1, COLLECT(coalesce(x.proficiency, 0.0d)) as v1, COLLECT(coalesce(y.proficiency, 0.0d)) as v2\n" +
                        "WITH p1.name as name, algo.similarity.euclidean(v1, v2) as euclidSim ORDER BY name ASC\n" +
                        "RETURN name, toString(toInteger(euclidSim*10000)/10000.0) as euclidSim");

        assertEquals(bobSim, result2.next().get("euclidSim"));
        assertEquals(jimSim, result2.next().get("euclidSim"));
    }
}
