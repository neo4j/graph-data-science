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
package org.neo4j.gds.similarity;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.BaseProcTest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SimilaritiesFuncTest extends BaseProcTest {

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
        registerFunctions(SimilaritiesFunc.class);
        runQuery(DB_CYPHER);
    }

    @Test
    public void testCosineSimilarityWithSomeWeightPropertiesNull() {
        String controlQuery =
            "MATCH (p1:Employee)-[x:HAS_SKILL]->(sk:Skill)<-[y:REQUIRES_SKILL] -(p2:Role {name:'Role 1-Analytics Manager'})\n" +
            "WITH sum(x.proficiency * y.proficiency) AS xyDotProduct,\n" +
            "sqrt(reduce(xDot = 0.0, a IN collect(x.proficiency) | xDot + a^2)) AS xLength,\n" +
            "sqrt(reduce(yDot = 0.0, b IN collect(y.proficiency) | yDot + b^2)) AS yLength,\n" +
            "p1, p2\n" +
            "WITH  p1.name AS name, xyDotProduct / (xLength * yLength) AS cosineSim\n" +
            "ORDER BY name ASC\n" +
            "WITH name, toFloat(cosineSim*10000.0) AS cosineSim\n" +
            "RETURN name, toString(toInteger(cosineSim)/10000.0) AS cosineSim";

        AtomicReference<String> bobSimilarity = new AtomicReference<>();
        AtomicReference<String> jimSimilarity = new AtomicReference<>();
        runQueryWithResultConsumer(controlQuery, result -> {
            bobSimilarity.set((String) result.next().get("cosineSim"));
            jimSimilarity.set((String) result.next().get("cosineSim"));
        });

        runQueryWithResultConsumer(
            "MATCH (p1:Employee)-[x:HAS_SKILL]->(sk:Skill)<-[y:REQUIRES_SKILL]-(p2:Role {name:'Role 1-Analytics Manager'})\n" +
            "WITH p1, COLLECT(coalesce(x.proficiency, 0.0)) AS v1, COLLECT(coalesce(y.proficiency, 0.0)) AS v2\n" +
            "WITH p1.name AS name, gds.alpha.similarity.cosine(v1, v2) AS cosineSim ORDER BY name ASC\n" +
            "RETURN name, toString(toInteger(cosineSim*10000)/10000.0) AS cosineSim",
            result -> {
                assertEquals(bobSimilarity.get(), result.next().get("cosineSim"));
                assertEquals(jimSimilarity.get(), result.next().get("cosineSim"));
            }
        );
    }

    @Test
    public void testCosineSimilarityWithSomeRelationshipsNull() {
        String controlQuery =
            "MATCH (p1:Employee)\n" +
            "MATCH (sk:Skill)<-[y:REQUIRES_SKILL] -(p2:Role {name:'Role 1-Analytics Manager'})\n" +
            "OPTIONAL MATCH (p1)-[x:HAS_SKILL]->(sk)\n" +
            "WITH sum(x.proficiency * y.proficiency) AS xyDotProduct,\n" +
            "sqrt(reduce(xDot = 0.0, a IN collect(x.proficiency) | xDot + a^2)) AS xLength,\n" +
            "sqrt(reduce(yDot = 0.0, b IN collect(y.proficiency) | yDot + b^2)) AS yLength,\n" +
            "p1, p2\n" +
            "WITH  p1.name AS name, xyDotProduct / (xLength * yLength) AS cosineSim\n" +
            "ORDER BY name ASC\n" +
            "WITH name, toFloat(cosineSim*10000.0) AS cosineSim\n" +
            "RETURN name, toString(toInteger(cosineSim)/10000.0) AS cosineSim";
        AtomicReference<String> bobSimilarity = new AtomicReference<>();
        AtomicReference<String> jimSimilarity = new AtomicReference<>();
        runQueryWithResultConsumer(controlQuery, result -> {
            bobSimilarity.set((String) result.next().get("cosineSim"));
            jimSimilarity.set((String) result.next().get("cosineSim"));
        });

        runQueryWithResultConsumer(
            "MATCH (sk:Skill)<-[y:REQUIRES_SKILL]-(p2:Role {name:'Role 1-Analytics Manager'})\n" +
            "MATCH (p1:Employee)\n" +
            "OPTIONAL MATCH (p1)-[x:HAS_SKILL]->(sk)\n" +
            "WITH p1, COLLECT(coalesce(x.proficiency, 0.0)) AS v1, COLLECT(coalesce(y.proficiency, 0.0)) AS v2\n" +
            "WITH p1.name AS name, gds.alpha.similarity.cosine(v1, v2) AS cosineSim ORDER BY name ASC\n" +
            "RETURN name, toString(toInteger(cosineSim*10000)/10000.0) AS cosineSim", result -> {

                assertEquals(bobSimilarity.get(), result.next().get("cosineSim"));
                assertEquals(jimSimilarity.get(), result.next().get("cosineSim"));
            });
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
            "WITH p1.name AS name, nom/denom AS pearson ORDER BY name ASC " +
            "RETURN name, toString(toInteger(pearson*10000)/10000.0) AS pearsonSim";

        AtomicReference<String> bobSimilarity = new AtomicReference<>();
        AtomicReference<String> jimSimilarity = new AtomicReference<>();
        runQueryWithResultConsumer(controlQuery, result -> {
            bobSimilarity.set((String) result.next().get("pearsonSim"));
            jimSimilarity.set((String) result.next().get("pearsonSim"));
        });

        runQueryWithResultConsumer(
            "MATCH (sk:Skill)<-[y:REQUIRES_SKILL]-(p2:Role {name:'Role 1-Analytics Manager'})\n" +
            "MATCH (p1:Employee)\n" +
            "OPTIONAL MATCH (p1)-[x:HAS_SKILL]->(sk)\n" +
            "WITH p1, COLLECT(coalesce(x.proficiency, 0.0)) AS v1, COLLECT(coalesce(y.proficiency, 0.0)) AS v2\n" +
            "WITH p1.name AS name, gds.alpha.similarity.pearson(v1, v2) AS pearsonSim ORDER BY name ASC\n" +
            "RETURN name, toString(toInteger(pearsonSim*10000)/10000.0) AS pearsonSim", result -> {

                assertEquals(bobSimilarity.get(), result.next().get("pearsonSim"));
                assertEquals(jimSimilarity.get(), result.next().get("pearsonSim"));
            });
    }

    @Test
    public void testEuclideanDistance() {
        String controlQuery =
            "MATCH (p1:Employee)\n" +
            "MATCH (sk:Skill)<-[y:REQUIRES_SKILL] -(p2:Role {name:'Role 1-Analytics Manager'})\n" +
            "OPTIONAL MATCH (p1)-[x:HAS_SKILL]->(sk)\n" +
            "WITH sqrt(sum((coalesce(x.proficiency,0) - coalesce(y.proficiency, 0))^2)) AS euclidDist, p1, p2\n" +
            "ORDER BY p1.name ASC\n" +
            "RETURN p1.name, toString(toInteger(euclidDist*10000)/10000.0) AS euclidDist";
        AtomicReference<String> bobDist = new AtomicReference<>();
        AtomicReference<String> jimDist = new AtomicReference<>();
        runQueryWithResultConsumer(controlQuery, result -> {
            bobDist.set((String) result.next().get("euclidDist"));
            jimDist.set((String) result.next().get("euclidDist"));
        });

        runQueryWithResultConsumer(
            "MATCH (sk:Skill)<-[y:REQUIRES_SKILL]-(p2:Role {name:'Role 1-Analytics Manager'})\n" +
            "MATCH (p1:Employee)\n" +
            "OPTIONAL MATCH (p1)-[x:HAS_SKILL]->(sk)\n" +
            "WITH p1, COLLECT(coalesce(x.proficiency, 0.0)) AS v1, COLLECT(coalesce(y.proficiency, 0.0)) AS v2\n" +
            "WITH p1.name AS name, gds.alpha.similarity.euclideanDistance(v1, v2) AS euclidDist ORDER BY name ASC\n" +
            "RETURN name, toString(toInteger(euclidDist*10000)/10000.0) AS euclidDist", result -> {

                assertEquals(bobDist.get(), result.next().get("euclidDist"));
                assertEquals(jimDist.get(), result.next().get("euclidDist"));
            });
    }

    @Test
    public void testJaccardSimilarity() {
        String controlQuery =
            "MATCH (p1:Employee)-[:HAS_SKILL]->(sk)<-[:HAS_SKILL]-(p2)\n" +
            "WITH p1,p2,size((p1)-[:HAS_SKILL]->()) AS d1, size((p2)-[:HAS_SKILL]->()) AS d2, count(DISTINCT sk) AS intersection\n" +
            "WITH p1.name AS name1, p2.name AS name2, toFloat(intersection) / (d1+d2-intersection) AS jaccardSim\n" +
            "ORDER BY name1,name2\n" +
            "RETURN name1,name2, toString(toInteger(jaccardSim*10000)/10000.0) AS jaccardSim";

        AtomicReference<String> bobSim = new AtomicReference<>();
        AtomicReference<String> jimSim = new AtomicReference<>();
        runQueryWithResultConsumer(controlQuery, result -> {
            bobSim.set((String) result.next().get("jaccardSim"));
            jimSim.set((String) result.next().get("jaccardSim"));
        });

        runQueryWithResultConsumer(
            "MATCH (p1:Employee),(p2:Employee) WHERE p1 <> p2\n" +
            "WITH p1, [(p1)-[:HAS_SKILL]->(sk) | id(sk)] AS v1, p2, [(p2)-[:HAS_SKILL]->(sk) | id(sk)] AS v2\n" +
            "WITH p1.name AS name1, p2.name AS name2, gds.alpha.similarity.jaccard(v1, v2) AS jaccardSim ORDER BY name1,name2\n" +
            "RETURN name1, name2, toString(toInteger(jaccardSim*10000)/10000.0) AS jaccardSim", result -> {

                assertEquals(jimSim.get(), result.next().get("jaccardSim"));
                assertEquals(bobSim.get(), result.next().get("jaccardSim"));
            });
    }

    @Test
    void testJaccardWithDuplicatesViaCypher() {
        runQueryWithResultConsumer(
            "RETURN gds.alpha.similarity.jaccard([1, 1, 2], [1, 3, 3]) AS jaccardSim", result -> {
                assertEquals(1/5D, result.next().get("jaccardSim"));
            });
    }

    @ParameterizedTest
    @MethodSource("listsWithDuplicates")
    void jaccardSimilarityShouldUseListsNotSets(List<Number> left, List<Number> right, double expected) {
        double actual = new SimilaritiesFunc().jaccardSimilarity(left, right);
        assertEquals(expected, actual);
    }

    static Stream<Arguments> listsWithDuplicates() {
        return Stream.of(
            Arguments.of(
                new ArrayList<Number>(Arrays.asList(1, 1)),
                new ArrayList<Number>(Arrays.asList(1, 2)),
                1/3D
            ),
            Arguments.of(
                new ArrayList<Number>(Arrays.asList(1, 2)),
                new ArrayList<Number>(Arrays.asList(2, 1)),
                2/2D
            ),
            Arguments.of(
                new ArrayList<Number>(Arrays.asList(Long.MAX_VALUE,              1)),
                new ArrayList<Number>(Arrays.asList((double) Long.MAX_VALUE - 1, 1)),
                1/3D
            ),
            Arguments.of(
                new ArrayList<Number>(Arrays.asList(Long.MAX_VALUE,     1)),
                new ArrayList<Number>(Arrays.asList(Long.MAX_VALUE - 1, 1)),
                1/3D
            ),
            Arguments.of(
                new ArrayList<Number>(Arrays.asList(Integer.MAX_VALUE,     1)),
                new ArrayList<Number>(Arrays.asList(Integer.MAX_VALUE - 1, 1)),
                1/3D
            ),
            Arguments.of(
                new ArrayList<Number>(Arrays.asList(16605, 16605, 16605, 150672)),
                new ArrayList<Number>(Arrays.asList(16605, 16605, 150672, 16605)),
                4/4D
            ), Arguments.of(
                new ArrayList<Number>(Arrays.asList(4159.0, 4159,   4159.0, 4159)),
                new ArrayList<Number>(Arrays.asList(4159,   4159.0, 4159,   1337.0)),
                3/5D
            ), Arguments.of(
                new ArrayList<Number>(Arrays.asList(4159, 1337, 1337, 1337)),
                new ArrayList<Number>(Arrays.asList(1337, 4159, 4159, 4159)),
                2/6D
            ), Arguments.of(
                new ArrayList<Number>(Arrays.asList(1, 2, 2)),
                new ArrayList<Number>(Arrays.asList(2, 2, 3)),
                2/4D
            ), Arguments.of(
                new ArrayList<Number>(Arrays.asList(null, 2, 2)),
                new ArrayList<Number>(Arrays.asList(2, 2, null, null)),
                1D
            ), Arguments.of(
                new ArrayList<Number>(),
                new ArrayList<Number>(),
                1D
            )
        );
    }

    @Test
    public void testOverlapSimilarity() {
        String controlQuery =
            "MATCH (p1:Employee)-[:HAS_SKILL]->(sk)<-[:HAS_SKILL]-(p2)\n" +
            "WITH p1,p2,size((p1)-[:HAS_SKILL]->()) as d1, size((p2)-[:HAS_SKILL]->()) as d2, count(distinct sk) as intersection\n" +
            "WITH p1.name as name1, p2.name as name2, toFloat(intersection) / CASE WHEN d1 > d2 THEN d2 ELSE d1 END as overlapSim\n" +
            "ORDER BY name1,name2\n" +
            "RETURN name1,name2, toString(toInteger(overlapSim*10000)/10000.0) as overlapSim";

        AtomicReference<String> bobSim = new AtomicReference<>();
        AtomicReference<String> jimSim = new AtomicReference<>();
        runQueryWithResultConsumer(controlQuery, result -> {
            bobSim.set((String) result.next().get("overlapSim"));
            jimSim.set((String) result.next().get("overlapSim"));
        });

        runQueryWithResultConsumer(
            "MATCH (p1:Employee),(p2:Employee) WHERE p1 <> p2\n" +
            "WITH p1, [(p1)-[:HAS_SKILL]->(sk) | id(sk)] as v1, p2, [(p2)-[:HAS_SKILL]->(sk) | id(sk)] as v2\n" +
            "WITH p1.name as name1, p2.name as name2, gds.alpha.similarity.overlap(v1, v2) as overlapSim ORDER BY name1,name2\n" +
            "RETURN name1, name2, toString(toInteger(overlapSim*10000)/10000.0) as overlapSim", result -> {
                assertEquals(jimSim.get(), result.next().get("overlapSim"));
                assertEquals(bobSim.get(), result.next().get("overlapSim"));
            });

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

        AtomicReference<String> bobSim = new AtomicReference<>();
        AtomicReference<String> jimSim = new AtomicReference<>();
        runQueryWithResultConsumer(controlQuery, result -> {
            bobSim.set((String) result.next().get("euclidSim"));
            jimSim.set((String) result.next().get("euclidSim"));
        });

        runQueryWithResultConsumer(
            "MATCH (sk:Skill)<-[y:REQUIRES_SKILL]-(p2:Role {name:'Role 1-Analytics Manager'})\n" +
            "MATCH (p1:Employee)\n" +
            "OPTIONAL MATCH (p1)-[x:HAS_SKILL]->(sk)\n" +
            "WITH p1, COLLECT(coalesce(x.proficiency, 0.0)) as v1, COLLECT(coalesce(y.proficiency, 0.0)) as v2\n" +
            "WITH p1.name as name, gds.alpha.similarity.euclidean(v1, v2) as euclidSim ORDER BY name ASC\n" +
            "RETURN name, toString(toInteger(euclidSim*10000)/10000.0) as euclidSim", result -> {
                assertEquals(bobSim.get(), result.next().get("euclidSim"));
                assertEquals(jimSim.get(), result.next().get("euclidSim"));
            });
    }

    @Test
    void testSimilarityFunctionsHandlesNullValues() {
        runQuery("RETURN gds.alpha.similarity.cosine([1, null, 2], [null, 1, 3])");

        runQuery("RETURN gds.alpha.similarity.euclidean([1, null, 2], [null, 1, 3])");

        runQuery("RETURN gds.alpha.similarity.euclideanDistance([1, null, 2], [null, 1, 3])");

        runQuery("RETURN gds.alpha.similarity.pearson([1, null, 2], [null, 1, 3])");

        runQuery("RETURN gds.alpha.similarity.overlap([1, null, 2], [null, 1, 3])");

        runQuery("RETURN gds.alpha.similarity.jaccard([1, null, 2], [null, 1, 3])");
    }
}
