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
package org.neo4j.graphalgo.similarity.knn;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.catalog.GraphCreateProc;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatNoException;

class KnnProcsOnFilteredGraphTest extends BaseProcTest {

    private static final String DB_QUERY =
        "CREATE " +
        "  (n:Node)" +
        ", (n1:Node:Foo { vector:[1,2,3] })" +
        ", (:Node:Foo { vector:[1,3,3] })";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphCreateProc.class,
            KnnStreamProc.class,
            KnnStatsProc.class,
            KnnWriteProc.class,
            KnnMutateProc.class
        );
        runQuery(DB_QUERY);

        String graphCreateQuery = GdsCypher.call()
            .withNodeLabels("Node", "Foo")
            .withNodeProperty("vector")
            .withAnyRelationshipType()
            .graphCreate("test")
            .yields();

        runQuery(graphCreateQuery);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("procedures")
    void testFilteredNodeProperties(String mode, String query) {
        assertThatNoException().isThrownBy(() -> runQuery(query));
    }

    private static Stream<Arguments> procedures() {
        return Stream.of(
            Arguments.of(
                "Stats",
                "CALL gds.beta.knn.stats(" +
                "  'test', " +
                "  {" +
                "    nodeLabels:['Foo']," +
                "    nodeWeightProperty: 'vector'" +
                "  }" +
                ")"
            ),
            Arguments.of(
                "Stream",
                "CALL gds.beta.knn.stream(" +
                "  'test', " +
                "  {" +
                "    nodeLabels:['Foo']," +
                "    nodeWeightProperty: 'vector'" +
                "  }" +
                ")"
            ),
            Arguments.of(
                "Mutate",
                "CALL gds.beta.knn.mutate(" +
                "  'test', " +
                "  {" +
                "    nodeLabels:['Foo']," +
                "    nodeWeightProperty: 'vector'," +
                "    mutateRelationshipType: 'filteredTestMutate'," +
                "    mutateProperty: 'filteredTestMutateProperty'" +
                "  }" +
                ")"
            ),
            Arguments.of(
                "Write",
                "CALL gds.beta.knn.write(" +
                "  'test', " +
                "  {" +
                "    nodeLabels:['Foo']," +
                "    nodeWeightProperty: 'vector'," +
                "    writeRelationshipType: 'filteredTestWrite'," +
                "    writeProperty: 'filteredTestWriteProperty'" +
                "  }" +
                ")"
            )
        );
    }
}
