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

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MutateNodePropertyTest;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;

public class LouvainMutateProcTest extends LouvainProcTest<LouvainMutateConfig> implements
    MutateNodePropertyTest<Louvain, LouvainMutateConfig, LouvainResult> {

    @Override
    public String mutateProperty() {
        return "communityId";
    }

    @Override
    public ValueType mutatePropertyType() {
        return ValueType.LONG;
    }

    @Override
    public Optional<String> mutateGraphName() {
        return Optional.of(LOUVAIN_GRAPH);
    }

    @Override
    public String expectedMutatedGraph() {
        return
            "  (a:Node { communityId: 14, seed: 1 })" +
            ", (b:Node { communityId: 14, seed: 1 })" +
            ", (c:Node { communityId: 14, seed: 1 })" +
            ", (d:Node { communityId: 14, seed: 1 })" +
            ", (e:Node { communityId: 14, seed: 1 })" +
            ", (f:Node { communityId: 14, seed: 1 })" +
            ", (g:Node { communityId: 7, seed: 2 })" +
            ", (h:Node { communityId: 7, seed: 2 })" +
            ", (i:Node { communityId: 7, seed: 2 })" +
            ", (j:Node { communityId: 12, seed: 42 })" +
            ", (k:Node { communityId: 12, seed: 42 })" +
            ", (l:Node { communityId: 12, seed: 42 })" +
            ", (m:Node { communityId: 12, seed: 42 })" +
            ", (n:Node { communityId: 12, seed: 42 })" +
            ", (x:Node { communityId: 14, seed: 1 })" +
            // 'LOUVAIN_GRAPH' is UNDIRECTED, e.g. each rel twice
            ", (a)-[:TYPE]->(b)-[:TYPE]->(a)" +
            ", (a)-[:TYPE]->(d)-[:TYPE]->(a)" +
            ", (a)-[:TYPE]->(f)-[:TYPE]->(a)" +
            ", (b)-[:TYPE]->(d)-[:TYPE]->(b)" +
            ", (b)-[:TYPE]->(x)-[:TYPE]->(b)" +
            ", (b)-[:TYPE]->(g)-[:TYPE]->(b)" +
            ", (b)-[:TYPE]->(e)-[:TYPE]->(b)" +
            ", (c)-[:TYPE]->(x)-[:TYPE]->(c)" +
            ", (c)-[:TYPE]->(f)-[:TYPE]->(c)" +
            ", (d)-[:TYPE]->(k)-[:TYPE]->(d)" +
            ", (e)-[:TYPE]->(x)-[:TYPE]->(e)" +
            ", (e)-[:TYPE]->(f)-[:TYPE]->(e)" +
            ", (e)-[:TYPE]->(h)-[:TYPE]->(e)" +
            ", (f)-[:TYPE]->(g)-[:TYPE]->(f)" +
            ", (g)-[:TYPE]->(h)-[:TYPE]->(g)" +
            ", (h)-[:TYPE]->(i)-[:TYPE]->(h)" +
            ", (h)-[:TYPE]->(j)-[:TYPE]->(h)" +
            ", (i)-[:TYPE]->(k)-[:TYPE]->(i)" +
            ", (j)-[:TYPE]->(k)-[:TYPE]->(j)" +
            ", (j)-[:TYPE]->(m)-[:TYPE]->(j)" +
            ", (j)-[:TYPE]->(n)-[:TYPE]->(j)" +
            ", (k)-[:TYPE]->(m)-[:TYPE]->(k)" +
            ", (k)-[:TYPE]->(l)-[:TYPE]->(k)" +
            ", (l)-[:TYPE]->(n)-[:TYPE]->(l)" +
            ", (m)-[:TYPE]->(n)-[:TYPE]->(m)";
    }

    @Override
    public Class<? extends AlgoBaseProc<Louvain, LouvainResult, LouvainMutateConfig, ?>> getProcedureClazz() {
        return LouvainMutateProc.class;
    }

    @Override
    public LouvainMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return LouvainMutateConfig.of(mapWrapper);
    }

    @Test
    void testMutateAndWriteWithSeeding() throws Exception {
        registerProcedures(LouvainWriteProc.class);
        var testGraphName = mutateGraphName().orElseThrow();

        var mutateQuery = GdsCypher
            .call(testGraphName)
            .algo("louvain")
            .mutateMode()
            .addParameter("mutateProperty", mutateProperty())
            .yields();

        runQuery(mutateQuery);

        var writeQuery = GdsCypher
            .call(testGraphName)
            .algo("louvain")
            .writeMode()
            .addParameter("seedProperty", mutateProperty())
            .addParameter("writeProperty", mutateProperty())
            .yields();

        runQuery(writeQuery);

        var updatedGraph = new StoreLoaderBuilder().databaseService(db)
            .addNodeLabel("Node")
            .addRelationshipType("TYPE")
            .globalOrientation(Orientation.UNDIRECTED)
            .addNodeProperty(mutateProperty(), mutateProperty(), DefaultValue.of(42.0), Aggregation.NONE)
            .addNodeProperty("seed", "seed", DefaultValue.of(42.0), Aggregation.NONE)
            .build()
            .graph();

        assertGraphEquals(fromGdl(expectedMutatedGraph()), updatedGraph);
    }

    @Test
    void testMutateYields() {
        String query = GdsCypher
            .call(mutateGraphName().orElseThrow())
            .algo("louvain")
            .mutateMode()
            .addParameter("mutateProperty", mutateProperty())
            .yields(
                "nodePropertiesWritten",
                "preProcessingMillis",
                "computeMillis",
                "mutateMillis",
                "postProcessingMillis",
                "ranLevels",
                "communityCount",
                "modularities",
                "communityDistribution",
                "configuration"
            );

        runQueryWithRowConsumer(
            query,
            row -> {
                assertThat(row.getNumber("nodePropertiesWritten"))
                    .asInstanceOf(LONG)
                    .isEqualTo(15);

                assertThat(row.getNumber("preProcessingMillis"))
                    .asInstanceOf(LONG)
                    .isGreaterThan(-1L);

                assertThat(row.getNumber("computeMillis"))
                    .asInstanceOf(LONG)
                    .isGreaterThan(-1L);

                assertThat(row.getNumber("mutateMillis"))
                    .asInstanceOf(LONG)
                    .isGreaterThan(-1L);

                assertThat(row.getNumber("ranLevels"))
                    .asInstanceOf(LONG)
                    .isEqualTo(2L);

                assertThat(row.getNumber("communityCount"))
                    .asInstanceOf(LONG)
                    .isEqualTo(3L);

                assertThat(row.get("modularities"))
                    .asList()
                    .first(DOUBLE)
                    .isEqualTo(0.376, Offset.offset(1e-3));


                assertThat(row.get("communityDistribution"))
                    .isInstanceOf(Map.class)
                    .asInstanceOf(MAP)
                    .containsExactlyInAnyOrderEntriesOf(
                        Map.of(
                            "p99", 7L,
                            "min", 3L,
                            "max", 7L,
                            "mean", 5.0D,
                            "p50", 5L,
                            "p75", 7L,
                            "p90", 7L,
                            "p95", 7L,
                            "p999", 7L
                        )
                    );
            }
        );
    }

    @Test
    void zeroCommunitiesInEmptyGraph() {
        runQuery("CALL db.createLabel('VeryTemp')");
        runQuery("CALL db.createRelationshipType('VERY_TEMP')");

        String graphName = "emptyGraph";

        var loadQuery = GdsCypher.call(graphName)
            .graphProject()
            .withNodeLabel("VeryTemp")
            .withRelationshipType("VERY_TEMP")
            .yields();

        runQuery(loadQuery);

        String query = GdsCypher
            .call(graphName)
            .algo("louvain")
            .mutateMode()
            .addParameter("mutateProperty", "foo")
            .yields("communityCount");

        runQueryWithRowConsumer(query, row -> {
           assertThat(row.getNumber("communityCount"))
               .asInstanceOf(LONG)
               .isEqualTo(0L);
        });
    }
}
