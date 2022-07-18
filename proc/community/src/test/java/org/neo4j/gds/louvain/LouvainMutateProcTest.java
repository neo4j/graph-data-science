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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MutateNodePropertyTest;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.compat.MapUtil;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;

public class LouvainMutateProcTest extends LouvainProcTest<LouvainMutateConfig> implements
    MutateNodePropertyTest<Louvain, LouvainMutateConfig, Louvain> {

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
    public Class<? extends AlgoBaseProc<Louvain, Louvain, LouvainMutateConfig, ?>> getProcedureClazz() {
        return LouvainMutateProc.class;
    }

    @Override
    public LouvainMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return LouvainMutateConfig.of(mapWrapper);
    }

    @Test
    void testMutateAndWriteWithSeeding() throws Exception {
        registerProcedures(LouvainWriteProc.class);
        var testGraphName = mutateGraphName().get();

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
            .call(mutateGraphName().get())
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
                assertEquals(15L, row.getNumber("nodePropertiesWritten"));

                assertThat(-1L, lessThan(row.getNumber("preProcessingMillis").longValue()));
                assertThat(-1L, lessThan(row.getNumber("computeMillis").longValue()));
                assertThat(-1L, lessThan(row.getNumber("mutateMillis").longValue()));

                assertEquals(2L, row.get("ranLevels"));
                assertEquals(3L, row.getNumber("communityCount"));
                assertEquals(0.376, ((List<Double>) row.get("modularities")).get(0), 1E-3);

                assertEquals(MapUtil.map(
                    "p99", 7L,
                    "min", 3L,
                    "max", 7L,
                    "mean", 5.0D,
                    "p90", 7L,
                    "p50", 5L,
                    "p999", 7L,
                    "p95", 7L,
                    "p75", 5L
                ), row.get("communityDistribution"));
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

        assertCypherResult(query, List.of(Map.of("communityCount", 0L)));
    }
}
