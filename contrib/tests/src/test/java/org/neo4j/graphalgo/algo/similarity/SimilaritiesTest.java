/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.algo.similarity;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.similarity.Similarities;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.Assert.assertEquals;

public class SimilaritiesTest {
    private static final String SETUP = "create (java:Skill{name:'Java'})\n" +
            "create (neo4j:Skill{name:'Neo4j'})\n" +
            "create (nodejs:Skill{name:'NodeJS'})\n" +
            "create (scala:Skill{name:'Scala'})\n" +
            "create (jim:Employee{name:'Jim'})\n" +
            "create (bob:Employee{name:'Bob'})\n" +
            "create (role:Role {name:'Role 1-Analytics Manager'})\n" +
            "\n" +
            "create (role)-[:REQUIRES_SKILL{proficiency:8.54}]->(java)\n" +
            "create (role)-[:REQUIRES_SKILL{proficiency:4.3}]->(scala)\n" +
            "create (role)-[:REQUIRES_SKILL{proficiency:9.75}]->(neo4j)\n" +
            "\n" +
            "create (bob)-[:HAS_SKILL{proficiency:10}]->(java)\n" +
            "create (bob)-[:HAS_SKILL{proficiency:7.5}]->(neo4j)\n" +
            "create (bob)-[:HAS_SKILL]->(scala)\n" +
            "create (jim)-[:HAS_SKILL{proficiency:8.25}]->(java)\n" +
            "create (jim)-[:HAS_SKILL{proficiency:7.1}]->(scala)";

    // cosine similarity taken from here: https://neo4j.com/graphgist/a7c915c8-a3d6-43b9-8127-1836fecc6e2f
    // euclid distance taken from here: https://neo4j.com/blog/real-time-recommendation-engine-data-science/
    // euclid similarity taken from here: http://stats.stackexchange.com/a/158285
    // pearson similarity taken from here: http://guides.neo4j.com/sandbox/recommendations

    private static GraphDatabaseAPI db;

    @BeforeClass
    public static void setUp() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerFunction(Similarities.class);

        db.execute(SETUP).close();
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testCosineSimilarityWithSomeWeightPropertiesNull() throws Exception {
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
        String bobSimilarity;
        String jimSimilarity;
        try (Transaction tx = db.beginTx()) {
            Result result = db.execute(controlQuery);
            bobSimilarity = (String) result.next().get("cosineSim");
            jimSimilarity = (String) result.next().get("cosineSim");
        }

        Result result = db.execute(
                "MATCH (p1:Employee)-[x:HAS_SKILL]->(sk:Skill)<-[y:REQUIRES_SKILL]-(p2:Role {name:'Role 1-Analytics Manager'})\n" +
                        "WITH p1, COLLECT(coalesce(x.proficiency, 0.0d)) as v1, COLLECT(coalesce(y.proficiency, 0.0d)) as v2\n" +
                        "WITH p1.name as name, algo.similarity.cosine(v1, v2) as cosineSim ORDER BY name ASC\n" +
                        "RETURN name, toString(toInteger(cosineSim*10000)/10000.0) as cosineSim");
        assertEquals(bobSimilarity, result.next().get("cosineSim"));
        assertEquals(jimSimilarity, result.next().get("cosineSim"));
    }

    @Test
    public void testCosineSimilarityWithSomeRelationshipsNull() throws Exception {
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
        String bobSimilarity;
        String jimSimilarity;
        try (Transaction tx = db.beginTx()) {
            Result result = db.execute(controlQuery);
            bobSimilarity = (String) result.next().get("cosineSim");
            jimSimilarity = (String) result.next().get("cosineSim");
        }

        Result result = db.execute(
                "MATCH (sk:Skill)<-[y:REQUIRES_SKILL]-(p2:Role {name:'Role 1-Analytics Manager'})\n" +
                        "MATCH (p1:Employee)\n" +
                        "OPTIONAL MATCH (p1)-[x:HAS_SKILL]->(sk)\n" +
                        "WITH p1, COLLECT(coalesce(x.proficiency, 0.0d)) as v1, COLLECT(coalesce(y.proficiency, 0.0d)) as v2\n" +
                        "WITH p1.name as name, algo.similarity.cosine(v1, v2) as cosineSim ORDER BY name ASC\n" +
                        "RETURN name, toString(toInteger(cosineSim*10000)/10000.0) as cosineSim");

        assertEquals(bobSimilarity, result.next().get("cosineSim"));
        assertEquals(jimSimilarity, result.next().get("cosineSim"));
    }

    @Test
    public void testPearsonSimilarityWithSomeRelationshipsNull() throws Exception {
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

        String bobSimilarity;
        String jimSimilarity;
        try (Transaction tx = db.beginTx()) {
            Result result = db.execute(controlQuery);
            bobSimilarity = (String) result.next().get("pearsonSim");
            jimSimilarity = (String) result.next().get("pearsonSim");
        }

        Result result = db.execute(
                "MATCH (sk:Skill)<-[y:REQUIRES_SKILL]-(p2:Role {name:'Role 1-Analytics Manager'})\n" +
                        "MATCH (p1:Employee)\n" +
                        "OPTIONAL MATCH (p1)-[x:HAS_SKILL]->(sk)\n" +
                        "WITH p1, COLLECT(coalesce(x.proficiency, 0.0d)) as v1, COLLECT(coalesce(y.proficiency, 0.0d)) as v2\n" +
                        "WITH p1.name as name, algo.similarity.pearson(v1, v2) as pearsonSim ORDER BY name ASC\n" +
                        "RETURN name, toString(toInteger(pearsonSim*10000)/10000.0) as pearsonSim");

        assertEquals(bobSimilarity, result.next().get("pearsonSim"));
        assertEquals(jimSimilarity, result.next().get("pearsonSim"));

    }

    @Test
    public void testEuclideanDistance() throws Exception {
        String controlQuery =
                "MATCH (p1:Employee)\n" +
                        "MATCH (sk:Skill)<-[y:REQUIRES_SKILL] -(p2:Role {name:'Role 1-Analytics Manager'})\n" +
                        "OPTIONAL MATCH (p1)-[x:HAS_SKILL]->(sk)\n" +
                        "WITH SQRT(SUM((coalesce(x.proficiency,0) - coalesce(y.proficiency, 0))^2)) AS euclidDist, p1, p2\n" +
                        "ORDER BY p1.name ASC\n" +
                        "RETURN p1.name, toString(toInteger(euclidDist*10000)/10000.0) as euclidDist";
        String bobDist;
        String jimDist;
        try (Transaction tx = db.beginTx()) {
            Result result = db.execute(controlQuery);
            bobDist = (String) result.next().get("euclidDist");
            jimDist = (String) result.next().get("euclidDist");
        }

        Result result = db.execute(
                "MATCH (sk:Skill)<-[y:REQUIRES_SKILL]-(p2:Role {name:'Role 1-Analytics Manager'})\n" +
                        "MATCH (p1:Employee)\n" +
                        "OPTIONAL MATCH (p1)-[x:HAS_SKILL]->(sk)\n" +
                        "WITH p1, COLLECT(coalesce(x.proficiency, 0.0d)) as v1, COLLECT(coalesce(y.proficiency, 0.0d)) as v2\n" +
                        "WITH p1.name as name, algo.similarity.euclideanDistance(v1, v2) as euclidDist ORDER BY name ASC\n" +
                        "RETURN name, toString(toInteger(euclidDist*10000)/10000.0) as euclidDist");

        assertEquals(bobDist, result.next().get("euclidDist"));
        assertEquals(jimDist, result.next().get("euclidDist"));

    }

    @Test
    public void testJaccardSimilarity() throws Exception {
        String controlQuery =
                "MATCH (p1:Employee)-[:HAS_SKILL]->(sk)<-[:HAS_SKILL]-(p2)\n" +
                "WITH p1,p2,size((p1)-[:HAS_SKILL]->()) as d1, size((p2)-[:HAS_SKILL]->()) as d2, count(distinct sk) as intersection\n" +
                "WITH p1.name as name1, p2.name as name2, toFloat(intersection) / (d1+d2-intersection) as jaccardSim\n" +
                "ORDER BY name1,name2\n" +
                "RETURN name1,name2, toString(toInteger(jaccardSim*10000)/10000.0) as jaccardSim";
        String bobSim;
        String jimSim;
        try (Transaction tx = db.beginTx()) {
            Result result = db.execute(controlQuery);
            bobSim = (String) result.next().get("jaccardSim");
            jimSim = (String) result.next().get("jaccardSim");
        }

        Result result = db.execute(
                        "MATCH (p1:Employee),(p2:Employee) WHERE p1 <> p2\n" +
                        "WITH p1, [(p1)-[:HAS_SKILL]->(sk) | id(sk)] as v1, p2, [(p2)-[:HAS_SKILL]->(sk) | id(sk)] as v2\n" +
                        "WITH p1.name as name1, p2.name as name2, algo.similarity.jaccard(v1, v2) as jaccardSim ORDER BY name1,name2\n" +
                        "RETURN name1, name2, toString(toInteger(jaccardSim*10000)/10000.0) as jaccardSim");

        assertEquals(bobSim, result.next().get("jaccardSim"));
        assertEquals(jimSim, result.next().get("jaccardSim"));
    }
    
    @Test
    public void testOverlapSimilarity() throws Exception {
        String controlQuery =
                "MATCH (p1:Employee)-[:HAS_SKILL]->(sk)<-[:HAS_SKILL]-(p2)\n" +
                        "WITH p1,p2,size((p1)-[:HAS_SKILL]->()) as d1, size((p2)-[:HAS_SKILL]->()) as d2, count(distinct sk) as intersection\n" +
                        "WITH p1.name as name1, p2.name as name2, toFloat(intersection) / CASE WHEN d1 > d2 THEN d2 ELSE d1 END as overlapSim\n" +
                        "ORDER BY name1,name2\n" +
                        "RETURN name1,name2, toString(toInteger(overlapSim*10000)/10000.0) as overlapSim";
        String bobSim;
        String jimSim;
        try (Transaction tx = db.beginTx()) {
            Result result = db.execute(controlQuery);
            bobSim = (String) result.next().get("overlapSim");
            jimSim = (String) result.next().get("overlapSim");
        }

        Result result = db.execute(
                "MATCH (p1:Employee),(p2:Employee) WHERE p1 <> p2\n" +
                        "WITH p1, [(p1)-[:HAS_SKILL]->(sk) | id(sk)] as v1, p2, [(p2)-[:HAS_SKILL]->(sk) | id(sk)] as v2\n" +
                        "WITH p1.name as name1, p2.name as name2, algo.similarity.overlap(v1, v2) as overlapSim ORDER BY name1,name2\n" +
                        "RETURN name1, name2, toString(toInteger(overlapSim*10000)/10000.0) as overlapSim");

        assertEquals(bobSim, result.next().get("overlapSim"));
        assertEquals(jimSim, result.next().get("overlapSim"));
    }

    @Test
    public void testEuclideanSimilarity() throws Exception {
        String controlQuery =
                "MATCH (p1:Employee)\n" +
                        "MATCH (sk:Skill)<-[y:REQUIRES_SKILL] -(p2:Role {name:'Role 1-Analytics Manager'})\n" +
                        "OPTIONAL MATCH (p1)-[x:HAS_SKILL]->(sk)\n" +
                        "WITH SQRT(SUM((coalesce(x.proficiency,0) - coalesce(y.proficiency, 0))^2)) AS euclidDist, p1\n" +
                        "WITH p1.name as name, 1 / (1 + euclidDist) as euclidSim\n" +
                        "ORDER BY name ASC\n" +
                        "RETURN name, toString(toInteger(euclidSim*10000)/10000.0) as euclidSim";
        String bobSim;
        String jimSim;
        try (Transaction tx = db.beginTx()) {
            Result result = db.execute(controlQuery);
            bobSim = (String) result.next().get("euclidSim");
            jimSim = (String) result.next().get("euclidSim");
        }

        Result result = db.execute(
                "MATCH (sk:Skill)<-[y:REQUIRES_SKILL]-(p2:Role {name:'Role 1-Analytics Manager'})\n" +
                        "MATCH (p1:Employee)\n" +
                        "OPTIONAL MATCH (p1)-[x:HAS_SKILL]->(sk)\n" +
                        "WITH p1, COLLECT(coalesce(x.proficiency, 0.0d)) as v1, COLLECT(coalesce(y.proficiency, 0.0d)) as v2\n" +
                        "WITH p1.name as name, algo.similarity.euclidean(v1, v2) as euclidSim ORDER BY name ASC\n" +
                        "RETURN name, toString(toInteger(euclidSim*10000)/10000.0) as euclidSim");

        assertEquals(bobSim, result.next().get("euclidSim"));
        assertEquals(jimSim, result.next().get("euclidSim"));
    }
}
