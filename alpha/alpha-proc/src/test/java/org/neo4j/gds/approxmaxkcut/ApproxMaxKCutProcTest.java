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
package org.neo4j.gds.approxmaxkcut;

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProcTest;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MemoryEstimateTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.compat.MapUtil;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.impl.approxmaxkcut.ApproxMaxKCut;
import org.neo4j.gds.impl.approxmaxkcut.config.ApproxMaxKCutConfig;
import org.neo4j.graphdb.GraphDatabaseService;

import static org.junit.jupiter.api.Assertions.assertEquals;

abstract class ApproxMaxKCutProcTest<CONFIG extends ApproxMaxKCutConfig> extends BaseProcTest implements
    AlgoBaseProcTest<ApproxMaxKCut, CONFIG, ApproxMaxKCut.CutResult>,
    MemoryEstimateTest<ApproxMaxKCut, CONFIG, ApproxMaxKCut.CutResult> {

    static final String GRAPH_NAME = "myGraph";

    @Override
    public GraphDatabaseService graphDb() {
        return db;
    }

    // The optimal max cut for this graph when k = 2 is:
    //     {a, b, c}, {d, e, f, g} if the graph is unweighted.
    //     {a, c}, {b, d, e, f, g} if the graph is weighted.
    @Neo4jGraph
    @Language("Cypher")
    private static final
    String DB_CYPHER =
        "CREATE" +
        "  (a:Label1)" +
        ", (b:Label1)" +
        ", (c:Label1)" +
        ", (d:Label1)" +
        ", (e:Label1)" +
        ", (f:Label1)" +
        ", (g:Label1)" +

        ", (a)-[:TYPE1 {weight: 81.0}]->(b)" +
        ", (a)-[:TYPE1 {weight: 7.0}]->(d)" +
        ", (b)-[:TYPE1 {weight: 1.0}]->(d)" +
        ", (b)-[:TYPE1 {weight: 1.0}]->(e)" +
        ", (b)-[:TYPE1 {weight: 1.0}]->(f)" +
        ", (b)-[:TYPE1 {weight: 1.0}]->(g)" +
        ", (c)-[:TYPE1 {weight: 45.0}]->(b)" +
        ", (c)-[:TYPE1 {weight: 3.0}]->(e)" +
        ", (d)-[:TYPE1 {weight: 3.0}]->(c)" +
        ", (d)-[:TYPE1 {weight: 1.0}]->(b)" +
        ", (e)-[:TYPE1 {weight: 1.0}]->(b)" +
        ", (f)-[:TYPE1 {weight: 3.0}]->(a)" +
        ", (f)-[:TYPE1 {weight: 1.0}]->(b)" +
        ", (g)-[:TYPE1 {weight: 1.0}]->(b)" +
        ", (g)-[:TYPE1 {weight: 4.0}]->(c)";

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(
            getProcedureClazz(),
            GraphProjectProc.class,
            GraphWriteNodePropertiesProc.class
        );

        String createQuery = GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .loadEverything()
            .yields();

        runQuery(createQuery);
    }

    @AfterEach
    void clearStore() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Override
    public void assertResultEquals(ApproxMaxKCut.CutResult result1, ApproxMaxKCut.CutResult result2) {
        assertEquals(result1.cutCost(), result2.cutCost());
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        return minimalConfigWithDefaults(mapWrapper);
    }

    // We need to override this to make sure that we set the config to generate a deterministic result when comparing
    // output from actual computation.
    protected CypherMapWrapper minimalConfigWithDefaults(CypherMapWrapper mapWrapper) {
        if (!mapWrapper.containsKey("k")) {
            mapWrapper = mapWrapper.withNumber("k", 2);
        }
        if (!mapWrapper.containsKey("iterations")) {
            mapWrapper = mapWrapper.withNumber("iterations", 8);
        }
        if (!mapWrapper.containsKey("vnsMaxNeighborhoodOrder")) {
            mapWrapper = mapWrapper.withNumber("vnsMaxNeighborhoodOrder", 0);
        }
        if (!mapWrapper.containsKey("concurrency")) {
            mapWrapper = mapWrapper.withNumber("concurrency", 1);
        }
        if (!mapWrapper.containsKey("randomSeed") && mapWrapper.getInt("concurrency", 1) == 1) {
            mapWrapper = mapWrapper.withNumber("randomSeed", 1337L);
        }

        return mapWrapper;
    }

    @Test
    void testK() {
        CypherMapWrapper config = createMinimalConfig(CypherMapWrapper.create(MapUtil.map("k", 18)));
        applyOnProcedure(proc -> {
            CONFIG approxMaxKCutConfig = proc.configParser().processInput(config.toMap());
            assertEquals(18, approxMaxKCutConfig.k());
        });
    }

    @Test
    void testIterations() {
        CypherMapWrapper config = createMinimalConfig(CypherMapWrapper.create(MapUtil.map("iterations", 87)));
        applyOnProcedure(proc -> {
            CONFIG approxMaxKCutConfig = proc.configParser().processInput(config.toMap());
            assertEquals(87, approxMaxKCutConfig.iterations());
        });
    }

    @Test
    void testVnsMaxNeighborhoodOrder() {
        CypherMapWrapper config = createMinimalConfig(CypherMapWrapper.create(MapUtil.map(
            "vnsMaxNeighborhoodOrder",
            31
        )));
        applyOnProcedure(proc -> {
            CONFIG approxMaxKCutConfig = proc.configParser().processInput(config.toMap());
            assertEquals(31, approxMaxKCutConfig.vnsMaxNeighborhoodOrder());
        });
    }

    @Test
    void testRandomSeed() {
        CypherMapWrapper config = createMinimalConfig(CypherMapWrapper.create(MapUtil.map(
            "randomSeed",
            42L,
            "concurrency",
            1
        )));
        applyOnProcedure(proc -> {
            CONFIG approxMaxKCutConfig = proc.configParser().processInput(config.toMap());
            assertEquals(42L, approxMaxKCutConfig.randomSeed().get());
        });
    }
}
