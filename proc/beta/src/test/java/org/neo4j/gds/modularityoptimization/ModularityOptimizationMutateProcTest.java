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

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MutateNodePropertyTest;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.config.GraphProjectFromStoreConfigImpl;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.GdsCypher.ExecutionModes.MUTATE;
import static org.neo4j.gds.RelationshipType.ALL_RELATIONSHIPS;

class ModularityOptimizationMutateProcTest extends BaseProcTest implements MutateNodePropertyTest<ModularityOptimization, ModularityOptimizationMutateConfig, ModularityOptimizationResult> {

    private static final String TEST_GRAPH_NAME = "myGraph";

    @Neo4jGraph
    static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node {name:'a', seed1: 0, seed2: 1})" +
        ", (b:Node {name:'b', seed1: 0, seed2: 1})" +
        ", (c:Node {name:'c', seed1: 2, seed2: 1})" +
        ", (d:Node {name:'d', seed1: 2, seed2: 42})" +
        ", (e:Node {name:'e', seed1: 2, seed2: 42})" +
        ", (f:Node {name:'f', seed1: 2, seed2: 42})" +
        ", (a)-[:TYPE {weight: 0.01}]->(b)" +
        ", (a)-[:TYPE {weight: 5.0}]->(e)" +
        ", (a)-[:TYPE {weight: 5.0}]->(f)" +
        ", (b)-[:TYPE {weight: 5.0}]->(c)" +
        ", (b)-[:TYPE {weight: 5.0}]->(d)" +
        ", (c)-[:TYPE {weight: 0.01}]->(e)" +
        ", (f)-[:TYPE {weight: 0.01}]->(d)";


    @Override
    public String mutateProperty() {
        return "community";
    }

    @Override
    public ValueType mutatePropertyType() {
        return ValueType.LONG;
    }

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            ModularityOptimizationMutateProc.class,
            GraphProjectProc.class,
            // this is needed by `MutateNodePropertyTest.testWriteBackGraphMutationOnFilteredGraph` 🤨
            GraphWriteNodePropertiesProc.class
        );

        runQuery(graphProjectQuery());
    }

    @Test
    void testMutate() {
        String query = GdsCypher.call(TEST_GRAPH_NAME)
            .algo("gds", "beta", "modularityOptimization")
            .mutateMode()
            .addParameter("mutateProperty", mutateProperty())
            .yields();

        runQueryWithRowConsumer(query, row -> {
            assertThat(row.getBoolean("didConverge")).isTrue();
            assertThat(row.getNumber("modularity"))
                .asInstanceOf(DOUBLE)
                .isEqualTo(0.12244, Offset.offset(0.001));
            assertThat(row.getNumber("communityCount"))
                .asInstanceOf(LONG)
                .isEqualTo(2);
            assertThat(row.getNumber("ranIterations"))
                .asInstanceOf(LONG)
                .isLessThanOrEqualTo(3);
        });
    }

    @Test
    void testMutateWeighted() {
        String query = GdsCypher.call(TEST_GRAPH_NAME).algo("gds", "beta", "modularityOptimization")
            .mutateMode()
            .addParameter("relationshipWeightProperty", "weight")
            .addParameter("mutateProperty", mutateProperty())
            .yields();

        runQueryWithRowConsumer(query, row -> {
            assertThat(row.getBoolean("didConverge")).isTrue();
            assertThat(row.getNumber("modularity"))
                .asInstanceOf(DOUBLE)
                .isEqualTo(0.4985, Offset.offset(0.001));
            assertThat(row.getNumber("communityCount"))
                .asInstanceOf(LONG)
                .isEqualTo(2);
            assertThat(row.getNumber("ranIterations"))
                .asInstanceOf(LONG)
                .isLessThanOrEqualTo(3);
        });
    }

    @Test
    void testMutateSeeded() {
        String query = GdsCypher.call(TEST_GRAPH_NAME).algo("gds", "beta", "modularityOptimization")
            .mutateMode()
            .addParameter("seedProperty", "seed1")
            .addParameter("mutateProperty", mutateProperty())
            .yields();

        runQuery(query);

        GraphStore mutatedGraph = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), TEST_GRAPH_NAME).graphStore();
        var communities = mutatedGraph.nodeProperty(mutateProperty()).values();
        var seeds = mutatedGraph.nodeProperty("seed1").values();
        for (int i = 0; i < mutatedGraph.nodeCount(); i++) {
            assertEquals(communities.longValue(i), seeds.longValue(i));
        }
    }

    @Test
    void testMutateTolerance() {
        String query = GdsCypher.call(TEST_GRAPH_NAME).algo("gds", "beta", "modularityOptimization")
            .mutateMode()
            .addParameter("tolerance", 1)
            .addParameter("mutateProperty", mutateProperty())
            .yields();

        runQueryWithRowConsumer(query, (row) -> {
            assertThat(row.getBoolean("didConverge")).isTrue();

            // Cannot converge after one iteration,
            // because it doesn't have anything to compare the computed modularity against.
            assertThat(row.getNumber("ranIterations"))
                .asInstanceOf(LONG)
                .isEqualTo(2);
        });
    }

    @Test
    void testMutateIterations() {
        String query = GdsCypher.call(TEST_GRAPH_NAME).algo("gds", "beta", "modularityOptimization")
            .mutateMode()
            .addParameter("maxIterations", 1)
            .addParameter("mutateProperty", mutateProperty())
            .yields();

        runQueryWithRowConsumer(query, (row) -> {
            assertThat(row.getBoolean("didConverge")).isFalse();
            assertThat(row.getNumber("ranIterations"))
                .asInstanceOf(LONG)
                .isEqualTo(1);
        });
    }

    // This should not be tested here...
    @Test
    void testMutateEstimate() {
        String query = GdsCypher.call(TEST_GRAPH_NAME).algo("gds", "beta", "modularityOptimization")
            .estimationMode(MUTATE)
            .addParameter("mutateProperty", mutateProperty())
            .yields();

        runQueryWithRowConsumer(query, (row) -> {
            assertThat(row.getNumber("bytesMin"))
                .asInstanceOf(LONG)
                .isPositive();
            assertThat(row.getNumber("bytesMax"))
                .asInstanceOf(LONG)
                .isPositive();
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
    public Class<ModularityOptimizationMutateProc> getProcedureClazz() {
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

    private static String graphProjectQuery() {
        GraphProjectFromStoreConfig config = GraphProjectFromStoreConfigImpl
            .builder()
            .graphName("")
            .username("")
            .nodeProjections(NodeProjections.all())
            .nodeProperties(PropertyMappings.fromObject(Arrays.asList("seed1", "seed2")))
            .relationshipProjections(RelationshipProjections.single(
                    ALL_RELATIONSHIPS,
                    RelationshipProjection.builder()
                        .type("TYPE")
                        .orientation(Orientation.UNDIRECTED)
                        .addProperty(PropertyMapping.of("weight", 1D))
                        .build()
                )
            ).build();

        return GdsCypher
            .call(TEST_GRAPH_NAME)
            .graphProject()
            .withGraphProjectConfig(config)
            .yields();
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }
}
