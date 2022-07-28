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
package org.neo4j.gds.labelpropagation;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.neo4j.gds.AlgoBaseProcTest;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MemoryEstimateTest;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.compat.MapUtil;
import org.neo4j.gds.config.ImmutableGraphProjectFromStoreConfig;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.test.config.IterationsConfigProcTest;
import org.neo4j.gds.test.config.NodeWeightConfigProcTest;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.RelationshipType.ALL_RELATIONSHIPS;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

abstract class LabelPropagationProcTest<CONFIG extends LabelPropagationBaseConfig> extends BaseProcTest implements
    AlgoBaseProcTest<LabelPropagation, CONFIG, LabelPropagation>,
    MemoryEstimateTest<LabelPropagation, CONFIG, LabelPropagation> {

    @TestFactory
    final Stream<DynamicTest> configTests() {
        return Stream.concat(modeSpecificConfigTests(), Stream.of(
            IterationsConfigProcTest.test(proc(), createMinimalConfig()),
            NodeWeightConfigProcTest.defaultTest(proc(), createMinimalConfig())
        ).flatMap(Collection::stream));
    }

    Stream<DynamicTest> modeSpecificConfigTests() {
        return Stream.empty();
    }

    static final List<Long> RESULT = Arrays.asList(2L, 7L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L);
    static final String TEST_GRAPH_NAME = "myGraph";
    private static final String TEST_CYPHER_GRAPH_NAME = "myCypherGraph";

    private static final String nodeQuery = "MATCH (n) RETURN id(n) AS id, n.weight AS weight, n.seed AS seed";
    private static final String relQuery = "MATCH (s)-[:X]->(t) RETURN id(s) AS source, id(t) AS target";

    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE" +
                                       "  (a:A {id: 0, seed: 42}) " +
                                       ", (b:B {id: 1, seed: 42}) " +

                                       ", (a)-[:X]->(:A {id: 2,  weight: 1.0, seed: 1}) " +
                                       ", (a)-[:X]->(:A {id: 3,  weight: 2.0, seed: 1}) " +
                                       ", (a)-[:X]->(:A {id: 4,  weight: 1.0, seed: 1}) " +
                                       ", (a)-[:X]->(:A {id: 5,  weight: 1.0, seed: 1}) " +
                                       ", (a)-[:X]->(:A {id: 6,  weight: 8.0, seed: 2}) " +

                                       ", (b)-[:X]->(:B {id: 7,  weight: 1.0, seed: 1}) " +
                                       ", (b)-[:X]->(:B {id: 8,  weight: 2.0, seed: 1}) " +
                                       ", (b)-[:X]->(:B {id: 9,  weight: 1.0, seed: 1}) " +
                                       ", (b)-[:X]->(:B {id: 10, weight: 1.0, seed: 1}) " +
                                       ", (b)-[:X]->(:B {id: 11, weight: 8.0, seed: 2})";

    @Override
    public GraphDatabaseService graphDb() {
        return db;
    }

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(
            getProcedureClazz(),
            GraphProjectProc.class,
            GraphWriteNodePropertiesProc.class
        );
        // Create explicit graphs with both projection variants
        runQuery(graphProjectQuery(Orientation.NATURAL, TEST_GRAPH_NAME));
        runQuery(formatWithLocale(
            "CALL gds.graph.project.cypher('%s', '%s', '%s', {})",
            TEST_CYPHER_GRAPH_NAME,
            nodeQuery,
            relQuery
        ));
    }

    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        super.configuration(builder);
    }

    @AfterEach
    void clearCommunities() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    static String graphProjectQuery(Orientation orientation, String graphName) {
        return graphProjectQuery(graphName, orientation).yields();
    }

    static GdsCypher.GraphProjectBuilder graphProjectQuery(String graphName, Orientation orientation) {
        return GdsCypher
            .call(graphName)
            .graphProject()
            .withGraphProjectConfig(ImmutableGraphProjectFromStoreConfig
                .builder()
                .graphName("")
                .nodeProjections(NodeProjections.fromObject(MapUtil.map("A", "A", "B", "B")))
                .nodeProperties(PropertyMappings.fromObject(Arrays.asList("seed", "weight")))
                .relationshipProjections(RelationshipProjections.builder()
                    .putProjection(
                        ALL_RELATIONSHIPS,
                        RelationshipProjection.builder()
                            .type("X")
                            .orientation(orientation)
                            .build()
                    )
                    .build()
                )
                .build()
             );
    }

    @Override
    public void assertResultEquals(LabelPropagation result1, LabelPropagation result2) {
        assertArrayEquals(result1.labels().toArray(), result2.labels().toArray());
        assertEquals(result1.didConverge(), result2.didConverge());
        assertEquals(result1.ranIterations(), result2.ranIterations());
    }
}
