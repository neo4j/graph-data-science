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
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OverlapDocTest extends BaseProcTest {

    public final String DB_CYPHER = "CREATE" +
                                    " (fahrenheit451:Book {title:'Fahrenheit 451'})," +
                                    " (dune:Book {title:'Dune'})," +
                                    " (hungerGames:Book {title:'The Hunger Games'})," +
                                    " (nineteen84:Book {title:'1984'})," +
                                    " (gatsby:Book {title:'The Great Gatsby'})," +

                                    " (scienceFiction:Genre {name: \"Science Fiction\"})," +
                                    " (fantasy:Genre {name: \"Fantasy\"})," +
                                    " (dystopia:Genre {name: \"Dystopia\"})," +
                                    " (classics:Genre {name: \"Classics\"})," +

                                    " (fahrenheit451)-[:HAS_GENRE]->(dystopia)," +
                                    " (fahrenheit451)-[:HAS_GENRE]->(scienceFiction)," +
                                    " (fahrenheit451)-[:HAS_GENRE]->(fantasy)," +
                                    " (fahrenheit451)-[:HAS_GENRE]->(classics)," +

                                    " (hungerGames)-[:HAS_GENRE]->(scienceFiction)," +
                                    " (hungerGames)-[:HAS_GENRE]->(fantasy)," +

                                    " (nineteen84)-[:HAS_GENRE]->(scienceFiction)," +
                                    " (nineteen84)-[:HAS_GENRE]->(dystopia)," +
                                    " (nineteen84)-[:HAS_GENRE]->(classics)," +

                                    " (dune)-[:HAS_GENRE]->(scienceFiction)," +
                                    " (dune)-[:HAS_GENRE]->(fantasy)," +
                                    " (dune)-[:HAS_GENRE]->(classics)," +

                                    " (gatsby)-[:HAS_GENRE]->(classics)";

    @BeforeEach
    void setupGraph() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();

        registerProcedures(OverlapProc.class);
        registerFunctions(GetNodeFunc.class, SimilaritiesFunc.class, IsFiniteFunc.class);
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void clearCommunities() {
        db.shutdown();
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void functionCall() {
        String query = "RETURN gds.alpha.similarity.overlap([1,2,3], [1,2,4,5]) AS similarity";

        runQueryWithRowConsumer(query, row -> assertEquals(0.6666666666666666 ,row.get("similarity")));
    }

    @Test
    void stream() {
        String query =
            " MATCH (book:Book)-[:HAS_GENRE]->(genre) " +
            " WITH {item:id(genre), categories: collect(id(book))} AS userData " +
            " WITH collect(userData) AS data " +
            " CALL gds.alpha.similarity.overlap.stream({nodeProjection: '*', relationshipProjection: '*', data: data}) " +
            " YIELD item1, item2, count1, count2, intersection, similarity " +
            " RETURN gds.util.asNode(item1).name AS from, gds.util.asNode(item2).name AS to, " +
            "        count1, count2, intersection, similarity " +
            " ORDER BY similarity DESC";

        String expectedResult = "+---------------------------------------------------------------------------------------------+\n" +
                                "| from              | to                | count1 | count2 | intersection | similarity         |\n" +
                                "+---------------------------------------------------------------------------------------------+\n" +
                                "| \"Fantasy\"         | \"Science Fiction\" | 3      | 4      | 3            | 1.0                |\n" +
                                "| \"Dystopia\"        | \"Classics\"        | 2      | 4      | 2            | 1.0                |\n" +
                                "| \"Dystopia\"        | \"Science Fiction\" | 2      | 4      | 2            | 1.0                |\n" +
                                "| \"Science Fiction\" | \"Classics\"        | 4      | 4      | 3            | 0.75               |\n" +
                                "| \"Fantasy\"         | \"Classics\"        | 3      | 4      | 2            | 0.6666666666666666 |\n" +
                                "| \"Dystopia\"        | \"Fantasy\"         | 2      | 3      | 1            | 0.5                |\n" +
                                "+---------------------------------------------------------------------------------------------+\n" +
                                "6 rows\n";

        runQueryWithResultConsumer(query, result-> assertEquals(expectedResult, result.resultAsString()));
    }

    @Test
    void streamSimilarityCutoff()
    {
        String query =
            " MATCH (book:Book)-[:HAS_GENRE]->(genre) " +
            " WITH {item:id(genre), categories: collect(id(book))} as userData " +
            " WITH collect(userData) as data " +
            " CALL gds.alpha.similarity.overlap.stream({ " +
            "   nodeProjection: '*', " +
            "   relationshipProjection: '*', " +
            "   data: data, " +
            "   similarityCutoff: 0.75 " +
            " }) " +
            " YIELD item1, item2, count1, count2, intersection, similarity " +
            " RETURN gds.util.asNode(item1).name AS from, gds.util.asNode(item2).name AS to, " +
            "        count1, count2, intersection, similarity " +
            " ORDER BY similarity DESC";

        String expectedResult = "+-------------------------------------------------------------------------------------+\n" +
                                "| from              | to                | count1 | count2 | intersection | similarity |\n" +
                                "+-------------------------------------------------------------------------------------+\n" +
                                "| \"Fantasy\"         | \"Science Fiction\" | 3      | 4      | 3            | 1.0        |\n" +
                                "| \"Dystopia\"        | \"Classics\"        | 2      | 4      | 2            | 1.0        |\n" +
                                "| \"Dystopia\"        | \"Science Fiction\" | 2      | 4      | 2            | 1.0        |\n" +
                                "| \"Science Fiction\" | \"Classics\"        | 4      | 4      | 3            | 0.75       |\n" +
                                "+-------------------------------------------------------------------------------------+\n" +
                                "4 rows\n";

        runQueryWithResultConsumer(query, result-> assertEquals(expectedResult, result.resultAsString()));
    }

    @Test
    void streamTopK()
    {
        String query =
            " MATCH (book:Book)-[:HAS_GENRE]->(genre) " +
            " WITH {item:id(genre), categories: collect(id(book))} as userData " +
            " WITH collect(userData) as data " +
            " CALL gds.alpha.similarity.overlap.stream({ " +
            "  nodeProjection: '*', " +
            "  relationshipProjection: '*', " +
            "  data: data, " +
            "  topK: 2 " +
            " }) " +
            " YIELD item1, item2, count1, count2, intersection, similarity " +
            " RETURN gds.util.asNode(item1).name AS from, gds.util.asNode(item2).name AS to, " +
            "        count1, count2, intersection, similarity " +
            " ORDER BY from";

        String expectedResult = "+---------------------------------------------------------------------------------------------+\n" +
                                "| from              | to                | count1 | count2 | intersection | similarity         |\n" +
                                "+---------------------------------------------------------------------------------------------+\n" +
                                "| \"Dystopia\"        | \"Classics\"        | 2      | 4      | 2            | 1.0                |\n" +
                                "| \"Dystopia\"        | \"Science Fiction\" | 2      | 4      | 2            | 1.0                |\n" +
                                "| \"Fantasy\"         | \"Science Fiction\" | 3      | 4      | 3            | 1.0                |\n" +
                                "| \"Fantasy\"         | \"Classics\"        | 3      | 4      | 2            | 0.6666666666666666 |\n" +
                                "| \"Science Fiction\" | \"Classics\"        | 4      | 4      | 3            | 0.75               |\n" +
                                "+---------------------------------------------------------------------------------------------+\n" +
                                "5 rows\n";

        runQueryWithResultConsumer(query, result-> assertEquals(expectedResult, result.resultAsString()));
    }

    @Test
    void write()
    {
        String query =
            " MATCH (book:Book)-[:HAS_GENRE]->(genre) " +
            " WITH {item:id(genre), categories: collect(id(book))} as userData " +
            " WITH collect(userData) as data " +
            " CALL gds.alpha.similarity.overlap.write({ " +
            "  nodeProjection: '*', " +
            "  relationshipProjection: '*', " +
            "  data: data, " +
            "  topK: 2, " +
            "  similarityCutoff: 0.5 " +
            " }) " +
            " YIELD nodes, similarityPairs, writeRelationshipType, writeProperty, min, max, mean, stdDev, p25, p50, p75, p90, p95, p99, p999, p100 " +
            " RETURN nodes, similarityPairs, writeRelationshipType, writeProperty, min, max, mean, p95";

        String expectedResult = "+-----------------------------------------------------------------------------------------------------------------------------------------------------+\n" +
                                "| nodes | similarityPairs | writeRelationshipType | writeProperty | min                | max                | mean               | p95                |\n" +
                                "+-----------------------------------------------------------------------------------------------------------------------------------------------------+\n" +
                                "| 4     | 5               | \"NARROWER_THAN\"       | \"score\"       | 0.6666641235351562 | 1.0000038146972656 | 0.8833351135253906 | 1.0000038146972656 |\n" +
                                "+-----------------------------------------------------------------------------------------------------------------------------------------------------+\n" +
                                "1 row\n";

        runQueryWithResultConsumer(query, result-> assertEquals(expectedResult, result.resultAsString()));

        String controlQuery = " MATCH path = (fantasy:Genre {name: \"Fantasy\"})-[:NARROWER_THAN*]->(genre) " +
                              " RETURN [node in nodes(path) | node.name] AS hierarchy " +
                              " ORDER BY length(path)";

        String expectedResult2 = "+------------------------------------------+\n" +
                                 "| hierarchy                                |\n" +
                                 "+------------------------------------------+\n" +
                                 "| [\"Fantasy\",\"Science Fiction\"]            |\n" +
                                 "| [\"Fantasy\",\"Classics\"]                   |\n" +
                                 "| [\"Fantasy\",\"Science Fiction\",\"Classics\"] |\n" +
                                 "+------------------------------------------+\n" +
                                 "3 rows\n";

        runQueryWithResultConsumer(controlQuery, result -> assertEquals(expectedResult2, result.resultAsString()));
    }

    @Test
    void sourceIds()
    {
        String query =
            " MATCH (book:Book)-[:HAS_GENRE]->(genre) " +
            " WITH {item:id(genre), name: genre.name, categories: collect(id(book))} as userData " +
            " WITH collect(userData) as data " +
            " WITH data, " +
            "      [value in data WHERE value.name IN [\"Fantasy\", \"Classics\"] | value.item ] AS sourceIds " +
            " CALL gds.alpha.similarity.overlap.stream({ " +
            "  nodeProjection: '*', " +
            "  relationshipProjection: '*', " +
            "  data: data, " +
            "  sourceIds: sourceIds " +
            " }) " +
            " YIELD item1, item2, count1, count2, intersection, similarity " +
            " RETURN gds.util.asNode(item1).name AS from, gds.util.asNode(item2).name AS to, similarity " +
            " ORDER BY similarity DESC";

        String expectedResult = "+-----------------------------------------------------+\n" +
                                "| from       | to                | similarity         |\n" +
                                "+-----------------------------------------------------+\n" +
                                "| \"Fantasy\"  | \"Science Fiction\" | 1.0                |\n" +
                                "| \"Classics\" | \"Science Fiction\" | 0.75               |\n" +
                                "| \"Fantasy\"  | \"Classics\"        | 0.6666666666666666 |\n" +
                                "+-----------------------------------------------------+\n" +
                                "3 rows\n";

        runQueryWithResultConsumer(query, result-> assertEquals(expectedResult, result.resultAsString()));
    }
}
