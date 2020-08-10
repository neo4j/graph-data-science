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
package org.neo4j.gds.embeddings.randomprojections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.Arguments;
import org.neo4j.graphalgo.AlgoBaseProcTest;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.utils.ExceptionUtil.rootCause;

public abstract class RandomProjectionProcTest<CONFIG extends RandomProjectionBaseConfig> extends BaseProcTest implements AlgoBaseProcTest<RandomProjection, CONFIG, RandomProjection> {

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node)" +
        ", (b:Node)" +
        ", (c:Isolated)" +
        ", (d:Isolated)" +
        ", (a)-[:REL]->(b)";

    @Override
    public String createQuery() {
        return DB_CYPHER;
    }

    @BeforeEach
    void setUp() throws Exception {
        runQuery(createQuery());
        registerProcedures(
            getProcedureClazz(),
            GraphCreateProc.class
        );
    }

    public GraphDatabaseAPI graphDb() {
        return db;
    }

    @Override
    public void assertResultEquals(
        RandomProjection result1, RandomProjection result2
    ) {
        // TODO: This just tests that the dimensions are the same for node 0, it's not a very good equality test
        assertEquals(result1.embeddings().get(0).length, result1.embeddings().get(0).length);
    }

    private static Stream<Arguments> weights() {
        return Stream.of(
            Arguments.of(Collections.emptyList()),
            Arguments.of(List.of(1.0f, 1.0f, 2.0f, 4.0f))
        );
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper userInput) {
        var withDimensions = userInput.containsKey("embeddingSize")
            ? userInput
            : userInput.withEntry("embeddingSize", 128);

        var withMaxIterations = withDimensions.containsKey("maxIterations")
            ? withDimensions
            : withDimensions.withEntry("maxIterations", 10);

        return withMaxIterations;
    }

    @Test
    void shouldFailWhenWeightsLengthUnequalToIterations() {
        int embeddingSize = 128;
        int maxIterations = 4;

        applyOnProcedure(proc -> {
            getProcedureMethods(proc).forEach(method -> {
                CypherMapWrapper configWrapper = CypherMapWrapper.empty()
                    .withEntry("embeddingSize", embeddingSize)
                    .withEntry("maxIterations", maxIterations)
                    .withEntry("iterationWeights", List.of(1.0f, 1.0f));

                Map<String, Object> config = createMinimalImplicitConfig(configWrapper).toMap();

                InvocationTargetException ex = assertThrows(
                    InvocationTargetException.class,
                    () -> method.invoke(proc, config, Collections.emptyMap())
                );

                var rootMessage = rootCause(ex).getMessage();
                assertEquals(
                    "The value of `iterationWeights` must be a list where its length is the " +
                    "same value as the configured value for `maxIterations`." + System.lineSeparator() +
                    "`maxIterations` is defined as `4` but `iterationWeights` contains `2` entries.",
                    rootMessage
                );
            });
        });
    }

    @Override
    public void loadGraph(String graphName) {
        String graphCreateQuery = GdsCypher.call()
            .withNodeLabel("Node")
            .withRelationshipType("REL", Orientation.UNDIRECTED)
            .graphCreate(graphName)
            .yields();
        runQuery(graphCreateQuery);
    }
}
