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
package org.neo4j.gds.louvain;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.neo4j.gds.AlgoBaseProcTest;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MemoryEstimateTest;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.functions.AsNodeFunc;
import org.neo4j.gds.test.config.IterationsConfigProcTest;
import org.neo4j.gds.test.config.ToleranceConfigProcTest;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

abstract class LouvainProcTest<CONFIG extends LouvainBaseConfig> extends BaseProcTest implements
    AlgoBaseProcTest<Louvain, CONFIG, LouvainResult>,
    MemoryEstimateTest<Louvain, CONFIG, LouvainResult> {

    @TestFactory
    final Stream<DynamicTest> configTests() {
        return Stream.concat(modeSpecificConfigTests(), Stream.of(
            IterationsConfigProcTest.test(proc(), createMinimalConfig()),
            ToleranceConfigProcTest.test(proc(), createMinimalConfig())
        ).flatMap(Collection::stream));
    }

    Stream<DynamicTest> modeSpecificConfigTests() {
        return Stream.empty();
    }

    static final List<List<Long>> RESULT = Arrays.asList(
        Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L, 14L),
        Arrays.asList(6L, 7L, 8L),
        Arrays.asList(9L, 10L, 11L, 12L, 13L)
    );

    static final String LOUVAIN_GRAPH = "myGraph";

    static final String WRITE_PROPERTY = "writeProperty";
    static final String SEED_PROPERTY = "seed";


    @Neo4jGraph
    private static final String DB_CYPHER = "CREATE" +
                                       "  (a:Node {seed: 1})" +        // 0
                                       ", (b:Node {seed: 1})" +        // 1
                                       ", (c:Node {seed: 1})" +        // 2
                                       ", (d:Node {seed: 1})" +        // 3
                                       ", (e:Node {seed: 1})" +        // 4
                                       ", (f:Node {seed: 1})" +        // 5
                                       ", (g:Node {seed: 2})" +        // 6
                                       ", (h:Node {seed: 2})" +        // 7
                                       ", (i:Node {seed: 2})" +        // 8
                                       ", (j:Node {seed: 42})" +       // 9
                                       ", (k:Node {seed: 42})" +       // 10
                                       ", (l:Node {seed: 42})" +       // 11
                                       ", (m:Node {seed: 42})" +       // 12
                                       ", (n:Node {seed: 42})" +       // 13
                                       ", (x:Node {seed: 1})" +        // 14

                                       ", (a)-[:TYPE {weight: 1.0}]->(b)" +
                                       ", (a)-[:TYPE {weight: 1.0}]->(d)" +
                                       ", (a)-[:TYPE {weight: 1.0}]->(f)" +
                                       ", (b)-[:TYPE {weight: 1.0}]->(d)" +
                                       ", (b)-[:TYPE {weight: 1.0}]->(x)" +
                                       ", (b)-[:TYPE {weight: 1.0}]->(g)" +
                                       ", (b)-[:TYPE {weight: 1.0}]->(e)" +
                                       ", (c)-[:TYPE {weight: 1.0}]->(x)" +
                                       ", (c)-[:TYPE {weight: 1.0}]->(f)" +
                                       ", (d)-[:TYPE {weight: 1.0}]->(k)" +
                                       ", (e)-[:TYPE {weight: 1.0}]->(x)" +
                                       ", (e)-[:TYPE {weight: 0.01}]->(f)" +
                                       ", (e)-[:TYPE {weight: 1.0}]->(h)" +
                                       ", (f)-[:TYPE {weight: 1.0}]->(g)" +
                                       ", (g)-[:TYPE {weight: 1.0}]->(h)" +
                                       ", (h)-[:TYPE {weight: 1.0}]->(i)" +
                                       ", (h)-[:TYPE {weight: 1.0}]->(j)" +
                                       ", (i)-[:TYPE {weight: 1.0}]->(k)" +
                                       ", (j)-[:TYPE {weight: 1.0}]->(k)" +
                                       ", (j)-[:TYPE {weight: 1.0}]->(m)" +
                                       ", (j)-[:TYPE {weight: 1.0}]->(n)" +
                                       ", (k)-[:TYPE {weight: 1.0}]->(m)" +
                                       ", (k)-[:TYPE {weight: 1.0}]->(l)" +
                                       ", (l)-[:TYPE {weight: 1.0}]->(n)" +
                                       ", (m)-[:TYPE {weight: 1.0}]->(n)";

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
        registerFunctions(AsNodeFunc.class);

        graphProjectQueries().forEach(this::runQuery);
    }

    List<String> graphProjectQueries() {
        return singletonList(
            GdsCypher.call(LOUVAIN_GRAPH)
                .graphProject()
                .withNodeLabel("Node")
                .withNodeProperty("seed")
                .withRelationshipType(
                    "TYPE",
                    RelationshipProjection.of(
                        "TYPE",
                        Orientation.UNDIRECTED,
                        Aggregation.DEFAULT
                    )
                )
                .yields()
        );
    }

    @AfterEach
    void clearCommunities() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Override
    public void assertResultEquals(LouvainResult result1, LouvainResult result2) {
        assertEquals(result1.ranLevels(), result2.ranLevels());
        assertEquals(result1.modularities()[result1.ranLevels() - 1], result2.modularities()[result2.ranLevels() - 1]);
        assertArrayEquals(
            result1.dendrogramManager().getCurrent().toArray(),
            result2.dendrogramManager().getCurrent().toArray()
        );
    }
}
