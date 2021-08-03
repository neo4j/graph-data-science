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

import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.impl.similarity.ApproxNearestNeighborsAlgorithm;
import org.neo4j.gds.impl.similarity.SimilarityConfig;
import org.neo4j.gds.impl.similarity.SimilarityInput;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.functions.AsNodeFunc;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphalgo.compat.MapUtil.map;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class ApproxNearestNeighborsProcTest extends AlphaSimilarityProcTest<ApproxNearestNeighborsAlgorithm<SimilarityInput>, SimilarityInput> {

    private static final String DB_CYPHER =
        "CREATE" +
        "  (french:Cuisine {name:'French'}) " +
        ", (italian:Cuisine {name:'Italian'}) " +
        ", (indian:Cuisine {name:'Indian'}) " +
        ", (lebanese:Cuisine {name:'Lebanese'}) " +
        ", (portuguese:Cuisine {name:'Portuguese'}) " +
        ", (zhen:Person {name: 'Zhen'}) " +
        ", (praveena:Person {name: 'Praveena'}) " +
        ", (michael:Person {name: 'Michael'}) " +
        ", (arya:Person {name: 'Arya'}) " +
        ", (karin:Person {name: 'Karin'}) " +
        ", (shrimp:Recipe {title: 'Shrimp Bolognese'}) " +
        ", (saltimbocca:Recipe {title: 'Saltimbocca alla roman'}) " +
        ", (periperi:Recipe {title: 'Peri Peri Naan'}) " +
        ", (praveena)-[:LIKES]->(indian) " +
        ", (praveena)-[:LIKES]->(portuguese) " +
        ", (zhen)-[:LIKES]->(french) " +
        ", (zhen)-[:LIKES]->(indian) " +
        ", (michael)-[:LIKES]->(french) " +
        ", (michael)-[:LIKES]->(italian) " +
        ", (michael)-[:LIKES]->(indian) " +
        ", (arya)-[:LIKES]->(lebanese) " +
        ", (arya)-[:LIKES]->(italian) " +
        ", (arya)-[:LIKES]->(portuguese) " +
        ", (karin)-[:LIKES]->(lebanese) " +
        ", (karin)-[:LIKES]->(italian) " +
        ", (shrimp)-[:TYPE]->(italian) " +
        ", (shrimp)-[:TYPE]->(indian) " +
        ", (saltimbocca)-[:TYPE]->(italian) " +
        ", (saltimbocca)-[:TYPE]->(french) " +
        ", (periperi)-[:TYPE]->(portuguese) " +
        ", (periperi)-[:TYPE]->(indian) ";

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(ApproxNearestNeighborsProc.class);
        registerFunctions(AsNodeFunc.class);
        runQuery(DB_CYPHER);
    }

    @Test
    void shouldStream() {
        Map<Pair<String, String>, Double> expectedScores = MapUtil.genericMap(
            Tuples.pair("Arya", "Karin"), 0.6666666666666666,
            Tuples.pair("Arya", "Praveena"), 0.25,
            Tuples.pair("Arya", "Michael"), 0.2,
            Tuples.pair("Karin", "Arya"), 0.6666666666666666,
            Tuples.pair("Karin", "Michael"), 0.25,
            Tuples.pair("Michael", "Zhen"), 0.6666666666666666,
            Tuples.pair("Michael", "Praveena"), 0.25,
            Tuples.pair("Michael", "Karin"), 0.25,
            Tuples.pair("Praveena", "Zhen"), 0.3333333333333333,
            Tuples.pair("Praveena", "Arya"), 0.25,
            Tuples.pair("Praveena", "Michael"), 0.25,
            Tuples.pair("Zhen", "Michael"), 0.6666666666666666,
            Tuples.pair("Zhen", "Praveena"), 0.3333333333333333
        );

        Map<String, Object> config = map(
            "config", map(
            "algorithm", "jaccard", "similarityCutoff", 0.1, "randomSeed", 42L)
        );

        String query =
            " MATCH (i:Cuisine)" +
            " WITH i ORDER BY id(i)" +
            " MATCH (p:Person) OPTIONAL MATCH (p)-[r:LIKES]->(cuisine)" +
            " WITH {item: id(p), categories: collect(id(cuisine))} as userData" +
            " WITH collect(userData) AS data, $config AS config" +
            " WITH config {.*, data: data} AS input" +
            " CALL gds.alpha.ml.ann.stream(input)" +
            " YIELD item1, item2, count1, count2, intersection, similarity" +
            " RETURN gds.util.asNode(item1).name as from, gds.util.asNode(item2).name AS to, similarity" +
            " ORDER BY from";

        runQueryWithRowConsumer(query, config, row -> {
            String from = row.getString("from");
            String to = row.getString("to");
            Double expectedScore = expectedScores.get(Tuples.pair(from, to));
            if (expectedScore == null) {
                expectedScore = expectedScores.get(Tuples.pair(to, from));
            }
            if (expectedScore == null) {
                fail(formatWithLocale("Unexpected result pair: from = %s to = %s", from, to));
            } else {
                assertEquals(expectedScore, row.getNumber("similarity").doubleValue());
            }
        });
    }

    @Test
    void shouldWrite() {
        Map<Pair<String, String>, Double> expectedScores = MapUtil.genericMap(
            Tuples.pair("Arya", "Karin"), 0.6666666666666666,
            Tuples.pair("Arya", "Praveena"), 0.25,
            Tuples.pair("Arya", "Michael"), 0.2,
            Tuples.pair("Karin", "Arya"), 0.6666666666666666,
            Tuples.pair("Karin", "Michael"), 0.25,
            Tuples.pair("Michael", "Zhen"), 0.6666666666666666,
            Tuples.pair("Michael", "Praveena"), 0.25,
            Tuples.pair("Michael", "Karin"), 0.25,
            Tuples.pair("Praveena", "Zhen"), 0.3333333333333333,
            Tuples.pair("Praveena", "Arya"), 0.25,
            Tuples.pair("Praveena", "Michael"), 0.25,
            Tuples.pair("Zhen", "Michael"), 0.6666666666666666,
            Tuples.pair("Zhen", "Praveena"), 0.3333333333333333
        );

        Map<String, Object> config = map(
            "config",
            map("algorithm", "jaccard", "similarityCutoff", 0.1, "randomSeed", 42L)
        );

        String query =
            " MATCH (i:Cuisine)" +
            " WITH i ORDER BY id(i)" +
            " MATCH (p:Person) OPTIONAL MATCH (p)-[r:LIKES]->(cuisine)" +
            " WITH {item: id(p), categories: collect(id(cuisine))} as userData" +
            " WITH collect(userData) AS data, $config AS config" +
            " WITH config {.*, data: data} AS input" +
            " CALL gds.alpha.ml.ann.write(input)" +
            " YIELD nodes RETURN nodes";
        runQueryWithRowConsumer(query, config, row -> {
            assertEquals(5, row.getNumber("nodes").intValue());
        });

        String resultQuery =
            " MATCH (from:Person)-[r:SIMILAR]->(to:Person)" +
            " RETURN from.name as from, to.name AS to, r.score as similarity" +
            " ORDER BY from";

        runQueryWithRowConsumer(resultQuery, row -> {
            String from = row.getString("from");
            String to = row.getString("to");
            Double expectedScore = expectedScores.get(Tuples.pair(from, to));
            if (expectedScore == null) {
                expectedScore = expectedScores.get(Tuples.pair(to, from));
            }
            if (expectedScore == null) {
                fail(formatWithLocale("Unexpected result pair: from = %s to = %s", from, to));
            } else {
                assertEquals(expectedScore.doubleValue(), row.getNumber("similarity").doubleValue());
            }
        });
    }

    @Override
    Map<String, Object> minimalViableConfig() {
        var config = super.minimalViableConfig();
        config.put("algorithm", "jaccard");
        return config;
    }

    @Override
    Class<? extends AlphaSimilarityProc<ApproxNearestNeighborsAlgorithm<SimilarityInput>, ? extends SimilarityConfig>> getProcedureClazz() {
        return ApproxNearestNeighborsProc.class;
    }
}
