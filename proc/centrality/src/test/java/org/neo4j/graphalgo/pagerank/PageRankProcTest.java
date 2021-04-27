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
package org.neo4j.graphalgo.pagerank;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.AlgoBaseProcTest;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.HeapControlTest;
import org.neo4j.graphalgo.ImmutablePropertyMapping;
import org.neo4j.graphalgo.IterationsConfigTest;
import org.neo4j.graphalgo.MemoryEstimateTest;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipWeightConfigTest;
import org.neo4j.graphalgo.SourceNodesConfigTest;
import org.neo4j.graphalgo.ToleranceConfigTest;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.extension.Neo4jGraph;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.hamcrest.Matchers.closeTo;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

abstract class PageRankProcTest<CONFIG extends PageRankConfig> extends BaseProcTest implements
    AlgoBaseProcTest<PageRankAlgorithm, CONFIG, PageRankResult>,
    IterationsConfigTest<PageRankAlgorithm, CONFIG, PageRankResult>,
    RelationshipWeightConfigTest<PageRankAlgorithm, CONFIG, PageRankResult>,
    ToleranceConfigTest<PageRankAlgorithm, CONFIG, PageRankResult>,
    MemoryEstimateTest<PageRankAlgorithm, CONFIG, PageRankResult>,
    SourceNodesConfigTest<PageRankAlgorithm, CONFIG, PageRankResult>,
    HeapControlTest<PageRankAlgorithm, CONFIG, PageRankResult> {

    static final double RESULT_ERROR = 1e-5;

    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE" +
           "  (a:Label1 {name: 'a'})" +
           ", (b:Label1 {name: 'b'})" +
           ", (c:Label1 {name: 'c'})" +
           ", (d:Label1 {name: 'd'})" +
           ", (e:Label1 {name: 'e'})" +
           ", (f:Label1 {name: 'f'})" +
           ", (g:Label1 {name: 'g'})" +
           ", (h:Label1 {name: 'h'})" +
           ", (i:Label1 {name: 'i'})" +
           ", (j:Label1 {name: 'j'})" +
           ", (k:Label2 {name: 'k'})" +
           ", (l:Label2 {name: 'l'})" +
           ", (m:Label2 {name: 'm'})" +
           ", (n:Label2 {name: 'n'})" +
           ", (o:Label2 {name: 'o'})" +
           ", (p:Label2 {name: 'p'})" +
           ", (q:Label2 {name: 'q'})" +
           ", (r:Label2 {name: 'r'})" +
           ", (s:Label2 {name: 's'})" +
           ", (t:Label2 {name: 't'})" +
           ", (u:Label3 {name: 'u'})" +
           ", (v:Label3 {name: 'v'})" +
           ", (w:Label3 {name: 'w'})" +
           ", (b)-[:TYPE1 {weight: 1.0,  equalWeight: 1.0}]->(c)" +
           ", (c)-[:TYPE1 {weight: 1.2,  equalWeight: 1.0}]->(b)" +
           ", (d)-[:TYPE1 {weight: 1.3,  equalWeight: 1.0}]->(a)" +
           ", (d)-[:TYPE1 {weight: 1.7,  equalWeight: 1.0}]->(b)" +
           ", (e)-[:TYPE1 {weight: 6.1,  equalWeight: 1.0}]->(b)" +
           ", (e)-[:TYPE1 {weight: 2.2,  equalWeight: 1.0}]->(d)" +
           ", (e)-[:TYPE1 {weight: 1.5,  equalWeight: 1.0}]->(f)" +
           ", (f)-[:TYPE1 {weight: 10.5, equalWeight: 1.0}]->(b)" +
           ", (f)-[:TYPE1 {weight: 2.9,  equalWeight: 1.0}]->(e)" +
           ", (g)-[:TYPE2 {weight: 3.2,  equalWeight: 1.0}]->(b)" +
           ", (g)-[:TYPE2 {weight: 5.3,  equalWeight: 1.0}]->(e)" +
           ", (h)-[:TYPE2 {weight: 9.5,  equalWeight: 1.0}]->(b)" +
           ", (h)-[:TYPE2 {weight: 0.3,  equalWeight: 1.0}]->(e)" +
           ", (i)-[:TYPE2 {weight: 5.4,  equalWeight: 1.0}]->(b)" +
           ", (i)-[:TYPE2 {weight: 3.2,  equalWeight: 1.0}]->(e)" +
           ", (j)-[:TYPE2 {weight: 9.5,  equalWeight: 1.0}]->(e)" +
           ", (k)-[:TYPE2 {weight: 4.2,  equalWeight: 1.0}]->(e)" +
           ", (u)-[:TYPE3 {weight: 1.0}]->(v)" +
           ", (u)-[:TYPE3 {weight: 1.0}]->(w)" +
           ", (v)-[:TYPE3 {weight: 1.0}]->(w)";

    static List<Map<String, Object>> expected;
    static List<Map<String, Object>> weightedExpected;

    @Override
    public GraphDatabaseAPI graphDb() {
        return db;
    }

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(
            getProcedureClazz(),
            GraphCreateProc.class,
            GraphWriteNodePropertiesProc.class
        );

        graphCreateQueries().forEach(this::runQuery);

        expected = List.of(
            Map.of("nodeId", idFunction.of("a"), "score", closeTo(0.24301, RESULT_ERROR)),
            Map.of("nodeId", idFunction.of("b"), "score", closeTo(1.83865, RESULT_ERROR)),
            Map.of("nodeId", idFunction.of("c"), "score", closeTo(1.69774, RESULT_ERROR)),
            Map.of("nodeId", idFunction.of("d"), "score", closeTo(0.21885, RESULT_ERROR)),
            Map.of("nodeId", idFunction.of("e"), "score", closeTo(0.24301, RESULT_ERROR)),
            Map.of("nodeId", idFunction.of("f"), "score", closeTo(0.21885, RESULT_ERROR)),
            Map.of("nodeId", idFunction.of("g"), "score", closeTo(0.15, RESULT_ERROR)),
            Map.of("nodeId", idFunction.of("h"), "score", closeTo(0.15, RESULT_ERROR)),
            Map.of("nodeId", idFunction.of("i"), "score", closeTo(0.15, RESULT_ERROR)),
            Map.of("nodeId", idFunction.of("j"), "score", closeTo(0.15, RESULT_ERROR))
        );

        weightedExpected = List.of(
            Map.of("nodeId", idFunction.of("a"), "score", closeTo(0.21803, RESULT_ERROR)),
            Map.of("nodeId", idFunction.of("b"), "score", closeTo(2.00083, RESULT_ERROR)),
            Map.of("nodeId", idFunction.of("c"), "score", closeTo(1.83298, RESULT_ERROR)),
            Map.of("nodeId", idFunction.of("d"), "score", closeTo(0.18471, RESULT_ERROR)),
            Map.of("nodeId", idFunction.of("e"), "score", closeTo(0.18194, RESULT_ERROR)),
            Map.of("nodeId", idFunction.of("f"), "score", closeTo(0.17367, RESULT_ERROR)),
            Map.of("nodeId", idFunction.of("g"), "score", closeTo(0.15, RESULT_ERROR)),
            Map.of("nodeId", idFunction.of("h"), "score", closeTo(0.15, RESULT_ERROR)),
            Map.of("nodeId", idFunction.of("i"), "score", closeTo(0.15, RESULT_ERROR)),
            Map.of("nodeId", idFunction.of("j"), "score", closeTo(0.15, RESULT_ERROR))
        );
    }

    List<String> graphCreateQueries() {
        return Arrays.asList(
            GdsCypher
                .call()
                .withNodeLabel("Label3")
                .withRelationshipType("TYPE3", RelationshipProjection.of(
                    "TYPE3",
                    Orientation.UNDIRECTED,
                    Aggregation.DEFAULT
                ).withProperties(PropertyMappings.of(PropertyMapping.of("equalWeight"))))
                .graphCreate("graphLabel3").yields(),
            GdsCypher.call()
                .withNodeLabel("Label1")
                .withRelationshipType("TYPE1", RelationshipProjection.builder().type("TYPE1")
                    .addProperties(PropertyMapping.of("equalWeight"), PropertyMapping.of("weight"))
                    .build())
                .graphCreate("graphLabel1")
                .yields()
        );
    }

    @AfterEach
    void clearCommunities() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    static Stream<Arguments> graphVariations() {
        return Stream.of(
            arguments(
                GdsCypher.call().explicitCreation("graphLabel1").algo("pageRank"),
                "explicit graph"
            ),
            arguments(
                GdsCypher.call()
                    .withNodeLabel("Label1")
                    .withRelationshipType("TYPE1")
                    .algo("pageRank"),
                "implicit graph"
            )
        );
    }

    static Stream<Arguments> graphVariationsWeight() {
        return Stream.of(
            arguments(
                GdsCypher.call().explicitCreation("graphLabel1").algo("pageRank"),
                "explicit graph"
            ),
            arguments(
                GdsCypher.call()
                    .withNodeLabel("Label1")
                    .withRelationshipType(
                        "TYPE1",
                        RelationshipProjection.builder()
                            .type("TYPE1")
                            .addProperty(ImmutablePropertyMapping.builder().propertyKey("weight").build())
                            .build()
                    )
                    .algo("pageRank"),
                "implicit graph"
            )
        );
    }

    static Stream<Arguments> graphVariationsEqualWeight() {
        return Stream.of(
            arguments(
                GdsCypher.call().explicitCreation("graphLabel1").algo("pageRank"),
                "explicit graph"
            ),
            arguments(
                GdsCypher.call()
                    .withNodeLabel("Label1")
                    .withRelationshipType(
                        "TYPE1",
                        RelationshipProjection.builder()
                            .type("TYPE1")
                            .addProperty(ImmutablePropertyMapping.builder().propertyKey("equalWeight").build())
                            .build()
                    )
                    .algo("pageRank"),
                "implicit graph"
            )
        );
    }

    static Stream<Arguments> graphVariationsLabel3() {
        return Stream.of(
            arguments(
                GdsCypher.call().explicitCreation("graphLabel3").algo("pageRank"),
                "explicit graph"
            ),
            arguments(
                GdsCypher.call()
                    .withNodeLabel("Label3")
                    .withRelationshipType(
                        "TYPE3",
                        RelationshipProjection.builder()
                            .type("TYPE3")
                            .orientation(Orientation.UNDIRECTED)
                            .addProperty(ImmutablePropertyMapping.builder().propertyKey("equalWeight").build())
                            .build()
                    )
                    .algo("pageRank"),
                "implicit graph"
            )
        );
    }

    @Override
    public void assertResultEquals(PageRankResult result1, PageRankResult result2) {
        HugeDoubleArray resultArray1 = result1.scores();
        HugeDoubleArray resultArray2 = result2.scores();
        assertArrayEquals(resultArray1.toArray(), resultArray2.toArray());
    }

    @Test
    void testDampingFactorFromConfig() {
        CypherMapWrapper mapWrapper = CypherMapWrapper.create(MapUtil.map("dampingFactor", 0.85));
        CONFIG config = createConfig(createMinimalConfig(mapWrapper));
        assertEquals(0.85, config.dampingFactor());
    }

    @ParameterizedTest
    @ValueSource(doubles = {-1, 1})
    void shouldFailOnInvalidDampingFactor(double dampingFactor) {
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> createConfig(createMinimalConfig(CypherMapWrapper.create(MapUtil.map(
                "dampingFactor",
                dampingFactor
            )))))
            .withMessageContainingAll("dampingFactor", String.valueOf(dampingFactor), "[0.00, 1.00)");
    }
}
