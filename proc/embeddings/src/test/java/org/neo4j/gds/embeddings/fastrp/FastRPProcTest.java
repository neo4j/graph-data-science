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
package org.neo4j.gds.embeddings.fastrp;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.AlgoBaseProcTest;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.TestSupport.crossArguments;

public abstract class FastRPProcTest<CONFIG extends FastRPBaseConfig> extends BaseProcTest implements
    AlgoBaseProcTest<FastRP, CONFIG, FastRP.FastRPResult> {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node {name: 'a', f1: 0.4, f2: 1.3})" +
        ", (b:Node {name: 'b', f1: 2.1, f2: 0.5})" +
        ", (e:Node2 {name: 'e'})" +
        ", (c:Isolated {name: 'c'})" +
        ", (d:Isolated {name: 'd'})" +
        ", (a)-[:REL]->(b)" +

        ", (a)<-[:REL2 {weight: 2.0}]-(b)" +
        ", (a)<-[:REL2 {weight: 1.0}]-(e)";

    protected static final String FASTRP_GRAPH = "myGraph";

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            getProcedureClazz(),
            GraphProjectProc.class
        );

        loadGraph(FASTRP_GRAPH);
    }

    public GraphDatabaseService graphDb() {
        return db;
    }

    @Override
    public void assertResultEquals(
        FastRP.FastRPResult result1, FastRP.FastRPResult result2
    ) {
        // TODO: This just tests that the dimensions are the same for node 0, it's not a very good equality test
        assertThat(result1.embeddings().get(0))
            .hasSameSizeAs(result2.embeddings().get(0));
    }

    private static Stream<Arguments> weights() {
        return crossArguments(
            () -> Stream.of(
                Arguments.of(Collections.emptyList()),
                Arguments.of(List.of(1.0f, 1.0f, 2.0f, 4.0f))
            ),
            () -> Stream.of(
                Arguments.of(0f),
                Arguments.of(0.5f),
                Arguments.of(1f)
            )
        );
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper userInput) {
        return userInput.containsKey("embeddingDimension")
            ? userInput
            : userInput.withEntry("embeddingDimension", 128);
    }

    @Override
    public void loadGraph(String graphName) {
        String graphCreateQuery = GdsCypher.call(graphName)
            .graphProject()
            .withNodeLabel("Node")
            .withRelationshipType("REL", Orientation.UNDIRECTED)
            .withNodeProperties(List.of("f1","f2"), DefaultValue.of(0.0f))
            .yields();
        runQuery(graphCreateQuery);
    }

    abstract GdsCypher.ExecutionModes mode();

    @Test
    void acceptsIntegerIterationWeights() {
        var query = GdsCypher
            .call(FASTRP_GRAPH)
            .algo("fastRP")
            .executionMode(mode())
            .addAllParameters(createMinimalConfig(CypherMapWrapper.empty()).toMap())
            .addParameter("embeddingDimension", 64)
            .addParameter("iterationWeights", List.of(1, 2L, 3.0))
            .yields();

        // doesn't crash
        runQuery(query);
    }

    @ParameterizedTest
    @MethodSource("invalidWeights")
    void validatesWeights(List<?> iterationWeights, String messagePart) {
        var query = GdsCypher
            .call(FASTRP_GRAPH)
            .algo("fastRP")
            .executionMode(mode())
            .addAllParameters(createMinimalConfig(CypherMapWrapper.empty()).toMap())
            .addParameter("embeddingDimension", 64)
            .addPlaceholder("iterationWeights", "iterationWeights")
            .yields();

        assertThatThrownBy(() -> runQuery(query, Map.of("iterationWeights", iterationWeights)))
            .hasCauseInstanceOf(QueryExecutionKernelException.class)
            .hasRootCauseInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(messagePart);
    }

    private static Stream<Arguments> invalidWeights() {
        return Stream.of(
            Arguments.of(List.of(), "must not be empty"),
            Arguments.of(List.of(1, "2"), "Iteration weights must be numbers"),
            Arguments.of(Arrays.asList(1, null), "Iteration weights must be numbers")
        );
    }

}
