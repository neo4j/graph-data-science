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
package org.neo4j.gds.modularityoptimization;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.ImmutableRelationshipProjections;
import org.neo4j.gds.MutateNodePropertyTest;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.config.ImmutableGraphProjectFromStoreConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.GdsCypher.ExecutionModes.MUTATE;
import static org.neo4j.gds.RelationshipType.ALL_RELATIONSHIPS;

class ModularityOptimizationMutateProcTest extends ModularityOptimizationProcTest implements MutateNodePropertyTest<ModularityOptimization, ModularityOptimizationMutateConfig, ModularityOptimizationResult> {

    private static final String TEST_GRAPH_NAME = "myGraph";

    @Override
    public String mutateProperty() {
        return "community";
    }

    @Override
    public ValueType mutatePropertyType() {
        return ValueType.LONG;
    }

    @BeforeEach
    @Override
    void setup() throws Exception{
        super.setup();
        runQuery(graphProjectQuery());
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

        GraphStore mutatedGraph = GraphStoreCatalog.get(TEST_USERNAME, databaseId(), TEST_GRAPH_NAME).graphStore();
        var communities = mutatedGraph.nodeProperty(mutateProperty()).values();
        var seeds = mutatedGraph.nodeProperty("seed1").values();
        for (int i = 0; i < mutatedGraph.nodeCount(); i++) {
            assertEquals(communities.longValue(i), seeds.longValue(i));
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
            // Cannot converge after one iteration,
            // because it doesn't have anything to compare the computed modularity against.
            assertEquals(2, row.getNumber("ranIterations").longValue());
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
            ", (a)-[]->(b)" +
            ", (a)-[]->(e)" +
            ", (a)-[]->(f)" +
            ", (b)-[]->(c)" +
            ", (b)-[]->(d)" +
            ", (c)-[]->(e)" +
            ", (f)-[]->(d)";
    }

    @Override
    public Class<? extends AlgoBaseProc<ModularityOptimization, ModularityOptimizationResult, ModularityOptimizationMutateConfig, ?>> getProcedureClazz() {
        return ModularityOptimizationMutateProc.class;
    }

    @Override
    public GraphDatabaseService graphDb() {
        return db;
    }

    @Override
    public ModularityOptimizationMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return ModularityOptimizationMutateConfig.of(mapWrapper);
    }

    @Override
    public void assertResultEquals(
        ModularityOptimizationResult result1, ModularityOptimizationResult result2
    ) {
        assertEquals(result1.modularity(), result2.modularity());
        assertEquals(result1.ranIterations(), result2.ranIterations());
    }

    GdsCypher.ModeBuildStage explicitAlgoBuildStage() {
        return GdsCypher.call(TEST_GRAPH_NAME).algo("gds", "beta", "modularityOptimization");
    }

    static String graphProjectQuery() {
        GraphProjectFromStoreConfig config = ImmutableGraphProjectFromStoreConfig
            .builder()
            .graphName("")
            .nodeProjections(NodeProjections.of())
            .nodeProperties(PropertyMappings.fromObject(Arrays.asList("seed1", "seed2")))
            .relationshipProjections(ImmutableRelationshipProjections.builder()
                .putProjection(
                    ALL_RELATIONSHIPS,
                    RelationshipProjection.builder()
                        .type("TYPE")
                        .orientation(Orientation.UNDIRECTED)
                        .properties(PropertyMappings.of(
                            Collections.singletonList(PropertyMapping.of("weight", 1D))))
                        .build()
                ).build()
            ).build();

        return GdsCypher
            .call(TEST_GRAPH_NAME)
            .graphProject()
            .withGraphProjectConfig(config)
            .yields();
    }
}
