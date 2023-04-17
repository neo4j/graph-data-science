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

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MutateNodePropertyTest;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class K1ColoringMutateProcTest extends BaseProcTest
    implements MutateNodePropertyTest<K1Coloring, K1ColoringMutateConfig, HugeLongArray> {

    private static final String K1COLORING_GRAPH = "myGraph";

    @Neo4jGraph
    public static final String DB_CYPHER =
        "CREATE" +
        " (a)" +
        ",(b)" +
        ",(c)" +
        ",(d)" +
        ",(a)-[:REL]->(b)" +
        ",(a)-[:REL]->(c)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            K1ColoringMutateProc.class,
            GraphWriteNodePropertiesProc.class,
            GraphProjectProc.class
        );
        loadGraph(K1COLORING_GRAPH);
    }

    @Override
    public String mutateProperty() {
        return "color";
    }

    @Override
    public ValueType mutatePropertyType() {
        return ValueType.LONG;
    }

    @Override
    public String expectedMutatedGraph() {
        return
            "  (x { color: 0 }) " +
            ", (y { color: 0 }) " +
            ", (z { color: 0 }) " +
            ", (w { color: 1 })-->(y) " +
            ", (w)-->(z) ";
    }

    @Override
    public K1ColoringMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return K1ColoringMutateConfig.of(mapWrapper);
    }

    @Override
    public Class<K1ColoringMutateProc> getProcedureClazz() {
        return K1ColoringMutateProc.class;
    }

    @Test
    void testMutateYields() {
        @Language("Cypher")
        String query = GdsCypher.call(K1COLORING_GRAPH).algo("gds", "beta", "k1coloring")
            .mutateMode()
            .addParameter("mutateProperty", mutateProperty())
            .yields();

        runQueryWithRowConsumer(query, row -> {
            assertEquals(4, row.getNumber("nodeCount").longValue(), "wrong nodeCount");
            assertTrue(row.getBoolean("didConverge"), "did not converge");
            assertTrue(row.getNumber("ranIterations").longValue() < 3, "wrong ranIterations");
            assertEquals(2, row.getNumber("colorCount").longValue(), "wrong color count");
            assertTrue(row.getNumber("preProcessingMillis").longValue() >= 0, "invalid preProcessingMillis");
            assertTrue(row.getNumber("mutateMillis").longValue() >= 0, "invalid mutateMillis");
            assertTrue(row.getNumber("computeMillis").longValue() >= 0, "invalid computeMillis");
        });
    }

    @Test
    void testMutateEstimate() {
        @Language("Cypher")
        String query = GdsCypher.call(K1COLORING_GRAPH).algo("gds", "beta", "k1coloring")
            .mutateEstimation()
            .addParameter("mutateProperty", "color")
            .yields("nodeCount", "bytesMin", "bytesMax", "requiredMemory");

        assertCypherResult(query, List.of(Map.of(
            "nodeCount", 4L,
            "bytesMin", 552L,
            "bytesMax", 552L,
            "requiredMemory", "552 Bytes"
        )));
    }

    @Test
    @Disabled("Mutate on empty graph has not been covered in AlgoBaseProcTest 🙈")
    public void testRunOnEmptyGraph() {
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Override
    public GraphDatabaseService graphDb() {
        return db;
    }

}
