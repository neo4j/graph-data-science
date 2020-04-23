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
package org.neo4j.graphalgo.triangle;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TriangleCountProcValidationTest extends BaseProcTest {

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphCreateProc.class,
            TriangleCountStreamProc.class,
            TriangleCountStatsProc.class,
            TriangleCountWriteProc.class,
            TriangleCountMutateProc.class
        );

        runQuery("CREATE (a:A)-[:T]->(b:A)");
        runQuery("CALL gds.graph.create('directed', 'A', 'T')");
        runQuery("CALL gds.graph.create('directedMultiRels', 'A', {" +
                 "  R: { type: 'T', orientation: 'REVERSE' }, " +
                 "  U: { type: 'T', orientation: 'UNDIRECTED' }, " +
                 "  N: { type: 'T', orientation: 'NATURAL' } " +
                 "})");
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @MethodSource("unfiltered")
    @ParameterizedTest(name = "Mode: {1}")
    void validateUndirected(String query, String ignoredModeName) {

        QueryExecutionException ex = assertThrows(
            QueryExecutionException.class,
            () -> runQuery(query)
        );

        assertThat(ex.getMessage(), containsString("Procedure requires relationship projections to be UNDIRECTED."));
    }

    @MethodSource("filtered")
    @ParameterizedTest(name = "Orientation(s): {1}")
    void validateUndirectedFiltering(List<String> filter, String ignoredModeName) {

        var query = GdsCypher.call().explicitCreation("directedMultiRels")
            .algo("triangleCount")
            .streamMode()
            .addParameter("relationshipTypes", filter)
            .yields();

        QueryExecutionException ex = assertThrows(
            QueryExecutionException.class,
            () -> runQuery(query)
        );

        assertThat(ex.getMessage(), containsString("Procedure requires relationship projections to be UNDIRECTED."));
    }

    static Stream<Arguments> filtered() {
        return Stream.of(
            Arguments.of(
                List.of("N"),
                "Natural"
            ),
            Arguments.of(
                List.of("R"),
                "Reverse"
            ),
            Arguments.of(
                List.of("U", "R"),
                "Undirected and Reverse"
            ),
            Arguments.of(
                List.of("U", "N"),
                "Undirected and Natural"
            ),
            Arguments.of(
                List.of("R", "N"),
                "Reverse and Natural"
            ),
            Arguments.of(
                List.of("*"),
                "All"
            )
        );
    }

    static Stream<Arguments> unfiltered() {
        return Stream.of(
            Arguments.of(
                GdsCypher.call().explicitCreation("directed")
                    .algo("triangleCount")
                    .streamMode()
                    .yields(),
                "Stream"
            ),
            Arguments.of(
                GdsCypher.call().explicitCreation("directed")
                    .algo("triangleCount")
                    .statsMode()
                    .yields(),
                "Stats"
            ),
            Arguments.of(
                GdsCypher.call().explicitCreation("directed")
                    .algo("triangleCount")
                    .writeMode()
                    .addParameter("writeProperty", "testTriangleCount")
                    .addParameter("clusteringCoefficientProperty", "clusteringCoefficient")
                    .yields(),
                "Write"
            ),
            Arguments.of(
                GdsCypher.call().explicitCreation("directed")
                    .algo("triangleCount")
                    .mutateMode()
                    .addParameter("mutateProperty", "mutatedTriangleCount")
                    .yields(),
                "Mutate"
            )
        );
    }
}
