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
package org.neo4j.graphalgo.louvain;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.newapi.GraphCreateProc;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

final class LouvainAlmostEmptyGraphTest extends BaseProcTest {

    @BeforeEach
    void setupGraph() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(LouvainStreamProc.class, GraphCreateProc.class);
        runQuery("CREATE (:Node)");
        runQuery(GdsCypher.call()
            .withNodeLabel("Node")
            .withAnyRelationshipType()
            .graphCreate("myGraph")
            .yields());
    }

    @AfterEach
    void clearCommunities() {
        db.shutdown();
        GraphCatalog.removeAllLoadedGraphs();
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("graphVariations")
    void testStream(GdsCypher.QueryBuilder queryBuilder, String testCaseName) {
        runQueryWithRowConsumer(
            queryBuilder
                .algo("louvain")
                .streamMode()
                .addParameter("concurrency", 1)
                .addParameter("maxLevels", 5)
                .addParameter("maxIterations", 10)
                .addParameter("tolerance", 0.00001D)
                .addParameter("includeIntermediateCommunities", false)
                .yields(),
            row -> {
                assertEquals(0, row.getNumber("nodeId").intValue());
                assertEquals(0, row.getNumber("communityId").intValue());
            }
        );
    }

    static Stream<Arguments> graphVariations() {
        return Stream.of(
            arguments(
                GdsCypher.call().explicitCreation("myGraph"),
                "explicit graph"
            ),
            arguments(
                GdsCypher.call()
                    .withNodeLabel("Node")
                    .withAnyRelationshipType(),
                "implicit graph"
            )
        );
    }
}
