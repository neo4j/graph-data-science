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
package org.neo4j.gds.beta.k1coloring;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.neo4j.gds.AlgoBaseProcTest;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MemoryEstimateTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.test.config.IterationsConfigProcTest;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Collection;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

abstract class K1ColoringProcBaseTest<CONFIG extends K1ColoringConfig> extends BaseProcTest implements
    AlgoBaseProcTest<K1Coloring, CONFIG, HugeLongArray>,
    MemoryEstimateTest<K1Coloring, CONFIG, HugeLongArray> {

    @TestFactory
    final Stream<DynamicTest> configTests() {
        return Stream.concat(
            modeSpecificConfigTests(), Stream.of(
                IterationsConfigProcTest.test(proc(), createMinimalConfig())
            ).flatMap(Collection::stream)
        );
    }

    Stream<DynamicTest> modeSpecificConfigTests() {
        return Stream.empty();
    }

    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE" +
           " (a)" +
           ",(b)" +
           ",(c)" +
           ",(d)" +
           ",(a)-[:REL]->(b)" +
           ",(a)-[:REL]->(c)";

    @Override
    public GraphDatabaseService graphDb() {
        return db;
    }

    static final String K1COLORING_GRAPH = "myGraph";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            getProcedureClazz(),
            GraphProjectProc.class
        );
        loadGraph(K1COLORING_GRAPH);
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Override
    public void assertResultEquals(HugeLongArray result1, HugeLongArray result2) {
        assertArrayEquals(result1.toArray(), result2.toArray());
    }

    GdsCypher.ModeBuildStage algoBuildStage() {
        return GdsCypher.call(K1COLORING_GRAPH).algo("gds", "beta", "k1coloring");
    }

}
