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
package org.neo4j.graphalgo.labelpropagation;

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.provider.Arguments;
import org.neo4j.graphalgo.BaseAlgoProcTests;
import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.GraphLoadProc;
import org.neo4j.graphalgo.MemoryEstimateTests;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.ProcTestBase;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.SeedConfigTests;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.impl.labelprop.LabelPropagation;
import org.neo4j.graphalgo.newapi.GraphCatalogProcs;
import org.neo4j.graphalgo.newapi.ImmutableGraphCreateConfig;
import org.neo4j.graphalgo.newapi.IterationsConfigTest;
import org.neo4j.graphalgo.WeightConfigTest;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

abstract class LabelPropagationProcTestBase<CONFIG extends LabelPropagationConfigBase> extends ProcTestBase implements
    BaseAlgoProcTests<CONFIG, LabelPropagation>,
    SeedConfigTests<CONFIG, LabelPropagation>,
    IterationsConfigTest<CONFIG, LabelPropagation>,
    WeightConfigTest<CONFIG, LabelPropagation>,
    MemoryEstimateTests<CONFIG, LabelPropagation>
{

    static final List<Long> RESULT = Arrays.asList(2L, 7L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L);
    public static final String TEST_GRAPH_NAME = "myGraph";

    @Override
    public GraphDatabaseAPI graphDb() {
        return db;
    }

    @BeforeEach
    void setupGraph() throws KernelException {

        db = TestDatabaseCreator.createTestDatabase();

        @Language("Cypher") String cypher =
            "CREATE" +
            "  (a:A {id: 0, seed: 42}) " +
            ", (b:B {id: 1, seed: 42}) " +

            ", (a)-[:X]->(:A {id: 2,  weight: 1.0, score: 1.0, seed: 1}) " +
            ", (a)-[:X]->(:A {id: 3,  weight: 2.0, score: 2.0, seed: 1}) " +
            ", (a)-[:X]->(:A {id: 4,  weight: 1.0, score: 1.0, seed: 1}) " +
            ", (a)-[:X]->(:A {id: 5,  weight: 1.0, score: 1.0, seed: 1}) " +
            ", (a)-[:X]->(:A {id: 6,  weight: 8.0, score: 8.0, seed: 2}) " +

            ", (b)-[:X]->(:B {id: 7,  weight: 1.0, score: 1.0, seed: 1}) " +
            ", (b)-[:X]->(:B {id: 8,  weight: 2.0, score: 2.0, seed: 1}) " +
            ", (b)-[:X]->(:B {id: 9,  weight: 1.0, score: 1.0, seed: 1}) " +
            ", (b)-[:X]->(:B {id: 10, weight: 1.0, score: 1.0, seed: 1}) " +
            ", (b)-[:X]->(:B {id: 11, weight: 8.0, score: 8.0, seed: 2})";

        registerProcedures(LabelPropagationStreamProc.class, LabelPropagationWriteProc.class, GraphLoadProc.class, GraphCatalogProcs.class);
        runQuery(cypher);

        runQuery(createGraphQuery(Projection.NATURAL, TEST_GRAPH_NAME));
        // TODO: is this flaky?
        runQuery("CALL algo.graph.load('myCypherGraph', '" +
                 nodeQuery +
                 "','" +
                 relQuery +
                 "', {" +
                 "  graph: 'cypher'," +
                 "  nodeProperties: ['seed', 'score', 'weight']" +
                 "})");
    }

    private static final String nodeQuery = "MATCH (n) RETURN id(n) AS id, n.weight AS weight, n.seed AS seed";
    private static final String relQuery = "MATCH (s)-[:X]->(t) RETURN id(s) AS source, id(t) AS target";

    String createGraphQuery(Projection projection, String graphName) {
        return String.format(
            "CALL algo.beta.graph.create(" +
            "    '%s'," +
            "    {" +
            "      A: {" +
            "        label: 'A | B'" +
            "      }" +
            "    }," +
            "    {" +
            "      TYPE: {" +
            "        type: 'X', " +
            "        projection: '%s'" +
            "      }" +
            "    }, {" +
            "      nodeProperties: ['seed', 'weight', 'score']" +
            "    }" +
            ")", graphName, projection.name());
    }

    @AfterEach
    void clearCommunities() {
        db.shutdown();
        GraphCatalog.removeAllLoadedGraphs();
    }

    static Stream<Arguments> gdsGraphVariations() {
        return Stream.of(
            arguments(
                GdsCypher.call().explicitCreation("myGraph"),
                "explicit graph"
            ),
            arguments(
                GdsCypher.call().implicitCreation(ImmutableGraphCreateConfig
                    .builder()
                    .graphName("")
                    .nodeProjection(NodeProjections.of(MapUtil.map("A", "A | B")))
                    .nodeProperties(PropertyMappings.fromObject(Arrays.asList("seed", "weight", "score")))
                    .relationshipProjection(RelationshipProjections.builder()
                        .putProjection(
                            ElementIdentifier.of("TYPE"),
                            RelationshipProjection.builder()
                                .type("X")
                                .projection(Projection.NATURAL)
                                .build()
                        )
                        .build()
                    )
                    .build()
                ),
                "implicit graph"
            )
        );
    }

    static Stream<Arguments> graphVariations() {
        return Stream.of(
            arguments("'myGraph', {", "explicit huge graph"),
            arguments("'myCypherGraph', {", "explicit cypher graph"),
            arguments(
                "{" +
                "  nodeProjection: {" +
                "    A: {" +
                "      label: 'A | B'" +
                "    }" +
                "  }, " +
                "  relationshipProjection: {" +
                "    TYPE: {" +
                "      type: 'X'" +
                "    }" +
                "  }," +
                "  nodeProperties: ['seed', 'score', 'weight'],",
                "implicit huge graph"
            ),
            arguments(
                "{" +
                "  nodeQuery: '" +
                nodeQuery +
                "', " +
                "  relationshipQuery: '" +
                relQuery +
                "'," +
                " nodeProperties: ['seed', 'score', 'weight'],",
                "implicit cypher graph"
            )
        );
    }

    @Override
    public void compareResults(LabelPropagation result1, LabelPropagation result2) {
        assertArrayEquals(result1.labels().toArray(), result2.labels().toArray());
        assertEquals(result1.didConverge(), result2.didConverge());
        assertEquals(result1.ranIterations(), result2.ranIterations());
    }
}
