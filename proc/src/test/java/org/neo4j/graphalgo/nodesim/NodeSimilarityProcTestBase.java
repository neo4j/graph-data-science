/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo.nodesim;

import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.utility.Iterate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.BaseAlgoProcTests;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.GraphLoadProc;
import org.neo4j.graphalgo.MemoryEstimateTests;
import org.neo4j.graphalgo.ProcTestBase;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.impl.nodesim.NodeSimilarityConfigBase;
import org.neo4j.graphalgo.impl.nodesim.NodeSimilarityResult;
import org.neo4j.graphalgo.impl.nodesim.SimilarityGraphResult;
import org.neo4j.graphalgo.impl.nodesim.SimilarityResult;
import org.neo4j.graphalgo.newapi.GraphCatalogProcs;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.graphalgo.Projection.NATURAL;
import static org.neo4j.graphalgo.Projection.REVERSE;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;

abstract class NodeSimilarityProcTestBase<CONFIG extends NodeSimilarityConfigBase> extends ProcTestBase implements
    BaseAlgoProcTests<CONFIG, NodeSimilarityResult>,
    MemoryEstimateTests<CONFIG, NodeSimilarityResult> {


    static final String DB_CYPHER =
        "CREATE" +
        "  (a:Person {id: 0,  name: 'Alice'})" +
        ", (b:Person {id: 1,  name: 'Bob'})" +
        ", (c:Person {id: 2,  name: 'Charlie'})" +
        ", (d:Person {id: 3,  name: 'Dave'})" +
        ", (i1:Item  {id: 10, name: 'p1'})" +
        ", (i2:Item  {id: 11, name: 'p2'})" +
        ", (i3:Item  {id: 12, name: 'p3'})" +
        ", (i4:Item  {id: 13, name: 'p4'})" +
        ", (a)-[:LIKES]->(i1)" +
        ", (a)-[:LIKES]->(i2)" +
        ", (a)-[:LIKES]->(i3)" +
        ", (b)-[:LIKES]->(i1)" +
        ", (b)-[:LIKES]->(i2)" +
        ", (c)-[:LIKES]->(i3)";

    static Stream<Projection> allValidProjections() {
        return Stream.of(NATURAL, REVERSE);
    }

    static Stream<Arguments> allValidGraphVariationsWithProjections() {
        return allValidProjections().flatMap(NodeSimilarityProcTestBase::graphVariationForProjection);
    }

    private static Stream<Arguments> graphVariationForProjection(Projection projection) {
        String name = "myGraph" + projection.name();
        return Stream.of(
            arguments(
                GdsCypher.call().explicitCreation(name),
                projection,
                "explicit graph - " + projection
            ),
            arguments(
                GdsCypher
                    .call()
                    .withNodeLabel("Person | Item")
                    .withRelationshipType("LIKES", RelationshipProjection
                        .builder()
                        .type("LIKES")
                        .projection(projection)
                        .build()
                    ),
                projection,
                "implicit graph - " + projection
            )
        );
    }

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(
            NodeSimilarityWriteProc.class,
            NodeSimilarityStreamProc.class,
            GraphLoadProc.class,
            GraphCatalogProcs.class
        );
        runQuery(DB_CYPHER);

        allValidProjections().forEach(projection -> {
            String name = "myGraph" + projection.name();
            runQuery("CALL algo.beta.graph.create(" +
                     "    $graphName," +
                     "    'Person | Item'," +
                     "    {" +
                     "        LIKES: {" +
                     "            type: 'LIKES'," +
                     "            projection: $projection" +
                     "        }" +
                     "    }" +
                     ")",
                MapUtil.map(
                    "graphName", name,
                    "projection", projection.name()
                )
            );
        });
    }

    @AfterEach
    void teardown() {
        db.shutdown();
        GraphCatalog.removeAllLoadedGraphs();
    }

    @Override
    public GraphDatabaseAPI graphDb() {
        return db;
    }

    @Override
    public void compareResults(NodeSimilarityResult result1, NodeSimilarityResult result2) {
        Optional<Stream<SimilarityResult>> maybeStream1 = result1.maybeStreamResult();
        if (maybeStream1.isPresent()) {
            Optional<Stream<SimilarityResult>> maybeStream2 = result2.maybeStreamResult();
            assertTrue(
                maybeStream2.isPresent(),
                "The two results are of different kind, left is a stream result, right is a graph result."
            );
            Collection<Pair<SimilarityResult, SimilarityResult>> comparableResults = Iterate.zip(
                maybeStream1.get().collect(Collectors.toList()),
                maybeStream2.get().collect(Collectors.toList())
            );
            for (Pair<SimilarityResult, SimilarityResult> pair : comparableResults) {
                SimilarityResult left = pair.getOne();
                SimilarityResult right = pair.getTwo();
                assertEquals(left, right);
            }
            return;
        }
        Optional<SimilarityGraphResult> maybeGraph1 = result1.maybeGraphResult();
        if (maybeGraph1.isPresent()) {
            Optional<SimilarityGraphResult> maybeGraph2 = result2.maybeGraphResult();
            assertTrue(
                maybeGraph2.isPresent(),
                "The two results are of different kind, left is a graph result, right is a stream result."
            );
            assertGraphEquals(maybeGraph1.get().similarityGraph(), maybeGraph2.get().similarityGraph());
            return;
        }

        fail("Result is neither a stream result or a graph result. Congratulations, this should never happen.");
    }

    @Override
    public CypherMapWrapper createMinimallyValidConfig(CypherMapWrapper mapWrapper) {
        if (!mapWrapper.containsKey("writeProperty")) {
            mapWrapper = mapWrapper.withString("writeProperty", "foo");
        }
        if (!mapWrapper.containsKey("writeRelationshipType")) {
            mapWrapper = mapWrapper.withString("writeRelationshipType", "bar");
        }
        return mapWrapper;
    }

    @ParameterizedTest(name = "parameter: {0}, value: {1}")
    @CsvSource(value = {"topN, -2", "bottomN, -2", "topK, -2", "bottomK, -2", "topK, 0", "bottomK, 0"})
    void shouldThrowForInvalidTopsAndBottoms(String parameter, long value) {
        String message = String.format("Invalid value for %s: must be a positive integer", parameter);
        CypherMapWrapper input = baseUserInput().withNumber(parameter, value);

        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> config(input)
        );
        assertThat(illegalArgumentException.getMessage(), containsString(message));
    }

    @ParameterizedTest
    @CsvSource(value = {"topK, bottomK", "topN, bottomN"})
    void shouldThrowForInvalidTopAndBottomCombination(String top, String bottom) {
        CypherMapWrapper input = baseUserInput().withNumber(top, 1).withNumber(bottom, 1);

        String expectedMessage = String.format("Invalid parameter combination: %s combined with %s", top, bottom);

        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> config(input)
        );
        assertThat(illegalArgumentException.getMessage(), is(expectedMessage));
    }

    @Test
    void shouldThrowIfDegreeCutoffSetToZero() {
        CypherMapWrapper input = baseUserInput().withNumber("degreeCutoff", 0);

        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> config(input)
        );
        assertThat(illegalArgumentException.getMessage(), is("Must set degree cutoff to 1 or greater"));
    }

    @Test
    void shouldCreateValidDefaultAlgoConfig() {
        CypherMapWrapper input = baseUserInput();
        NodeSimilarityConfigBase config = config(input);

        assertEquals(10, config.topK());
        assertEquals(0, config.topN());
        assertEquals(1, config.degreeCutoff());
        assertEquals(1E-42, config.similarityCutoff());
        assertEquals(Pools.DEFAULT_CONCURRENCY, config.concurrency());
    }

    @ParameterizedTest(name = "top or bottom: {0}")
    @ValueSource(strings = {"top", "bottom"})
    void shouldCreateValidCustomAlgoConfig(String parameter) {
        CypherMapWrapper input = baseUserInput()
            .withNumber(parameter + "K", 100)
            .withNumber(parameter + "N", 1000)
            .withNumber("degreeCutoff", 42)
            .withNumber("similarityCutoff", 0.23)
            .withNumber("concurrency", 1);

        NodeSimilarityConfigBase config = config(input);

        assertEquals(parameter.equals("top") ? 100 : -100, config.normalizedK());
        assertEquals(parameter.equals("top") ? 1000 : -1000, config.normalizedN());
        assertEquals(42, config.degreeCutoff());
        assertEquals(0.23, config.similarityCutoff());
        assertEquals(1, config.concurrency());
    }

    private CypherMapWrapper baseUserInput() {
        return createMinimallyValidConfig(CypherMapWrapper.empty());
    }

    private CONFIG config(CypherMapWrapper input) {
        return createConfig(input);
    }
}
