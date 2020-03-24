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
package org.neo4j.graphalgo.beta.modularity;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.GraphMutationTest;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.config.ImmutableGraphCreateFromStoreConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.GdsCypher.ExecutionModes.MUTATE;

class ModularityOptimizationMutateProcTest extends ModularityOptimizationProcTest implements GraphMutationTest<ModularityOptimizationMutateConfig, ModularityOptimization> {

    private static final String TEST_GRAPH_NAME = "myGraph";

    @Override
    public String mutateProperty() {
        return "community";
    }

    @BeforeEach
    @Override
    void setup() throws Exception{
        super.setup();
        runQuery(graphCreateQuery());
    }

    @Test
    void testMutate() {
        String query = explicitAlgoBuildStage()
            .mutateMode()
            .addParameter("mutateProperty", mutateProperty())
            .yields();

        runQueryWithRowConsumer(query, row -> {
            assertEquals(true, row.getBoolean("didConverge"));
            assertEquals(0.12244, row.getNumber("modularity").doubleValue(), 0.001);
            assertEquals(2, row.getNumber("communityCount").longValue());
            assertTrue(row.getNumber("ranIterations").longValue() <= 3);
        });
    }

    @Test
    void testMutateWeighted() {
        String query = explicitAlgoBuildStage()
            .mutateMode()
            .addParameter("relationshipWeightProperty", "weight")
            .addParameter("mutateProperty", mutateProperty())
            .yields();

        runQueryWithRowConsumer(query, row -> {
            assertEquals(true, row.getBoolean("didConverge"));
            assertEquals(0.4985, row.getNumber("modularity").doubleValue(), 0.001);
            assertEquals(2, row.getNumber("communityCount").longValue());
            assertTrue(row.getNumber("ranIterations").longValue() <= 3);
        });
    }

    @Test
    void testMutateSeeded() {
        String query = explicitAlgoBuildStage()
            .mutateMode()
            .addParameter("seedProperty", "seed1")
            .addParameter("mutateProperty", mutateProperty())
            .yields();

        runQuery(query);

        Graph mutatedGraph = GraphStoreCatalog.get(TEST_USERNAME, TEST_GRAPH_NAME).getGraph();
        NodeProperties communities = mutatedGraph.nodeProperties(mutateProperty());
        NodeProperties seeds = mutatedGraph.nodeProperties("seed1");
        for (int i = 0; i < mutatedGraph.nodeCount(); i++) {
            assertEquals(communities.nodeProperty(i), seeds.nodeProperty(i));
        }
    }

    @Test
    void testMutateTolerance() {
        String query = explicitAlgoBuildStage()
            .mutateMode()
            .addParameter("tolerance", 1)
            .addParameter("mutateProperty", mutateProperty())
            .yields();

        runQueryWithRowConsumer(query, (row) -> {
            assertTrue(row.getBoolean("didConverge"));
            assertEquals(1, row.getNumber("ranIterations").longValue());
        });
    }

    @Test
    void testMutateIterations() {
        String query = explicitAlgoBuildStage()
            .mutateMode()
            .addParameter("maxIterations", 1)
            .addParameter("mutateProperty", mutateProperty())
            .yields();

        runQueryWithRowConsumer(query, (row) -> {
            assertFalse(row.getBoolean("didConverge"));
            assertEquals(1, row.getNumber("ranIterations").longValue());
        });
    }

    @Test
    void testMutateEstimate() {
        String query = explicitAlgoBuildStage()
            .estimationMode(MUTATE)
            .addParameter("mutateProperty", mutateProperty())
            .yields();

        runQueryWithRowConsumer(query, (row) -> {
            assertTrue(row.getNumber("bytesMin").longValue() > 0);
            assertTrue(row.getNumber("bytesMax").longValue() > 0);
        });
    }

    @Override
    public String expectedMutatedGraph() {
        return
            "  (a { community: 4 }) " +
            ", (b { community: 4 }) " +
            ", (c { community: 4 }) " +
            ", (d { community: 3 }) " +
            ", (e { community: 4 }) " +
            ", (f { community: 3 }) " +
            ", (a)-[:TYPE]->(b)" +
            ", (a)-[:TYPE]->(e)" +
            ", (a)-[:TYPE]->(f)" +
            ", (b)-[:TYPE]->(c)" +
            ", (b)-[:TYPE]->(d)" +
            ", (c)-[:TYPE]->(e)" +
            ", (f)-[:TYPE]->(d)";
    }

    @Override
    public Class<? extends AlgoBaseProc<?, ModularityOptimization, ModularityOptimizationMutateConfig>> getProcedureClazz() {
        return ModularityOptimizationMutateProc.class;
    }

    @Override
    public GraphDatabaseAPI graphDb() {
        return db;
    }

    @Override
    public ModularityOptimizationMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return ModularityOptimizationMutateConfig.of(getUsername(), Optional.empty(),Optional.empty(), mapWrapper);
    }

    @Override
    public void assertResultEquals(
        ModularityOptimization result1, ModularityOptimization result2
    ) {
        assertEquals(result1.getModularity(), result2.getModularity());
        assertEquals(result1.getIterations(), result2.getIterations());
    }

    GdsCypher.ModeBuildStage explicitAlgoBuildStage() {
        return GdsCypher.call()
            .explicitCreation(TEST_GRAPH_NAME)
            .algo("gds", "beta", "modularityOptimization");
    }

    static String graphCreateQuery() {
        return GdsCypher
            .call()
            .implicitCreation(ImmutableGraphCreateFromStoreConfig
                .builder()
                .graphName("")
                .nodeProjections(NodeProjections.of())
                .nodeProperties(PropertyMappings.fromObject(Arrays.asList("seed1", "seed2")))
                .relationshipProjections(RelationshipProjections.builder()
                    .putProjection(
                        RelationshipProjections.PROJECT_ALL,
                        RelationshipProjection.builder()
                            .type("TYPE")
                            .orientation(Orientation.UNDIRECTED)
                            .properties(PropertyMappings.of(
                                Collections.singletonList(PropertyMapping.of("weight", 1D))))
                            .build()
                    )
                    .build()
                )
                .build()
            )
            .graphCreate(TEST_GRAPH_NAME)
            .yields();
    }
}
