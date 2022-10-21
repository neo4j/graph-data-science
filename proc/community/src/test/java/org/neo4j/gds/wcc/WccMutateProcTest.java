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
package org.neo4j.gds.wcc;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MutateNodePropertyTest;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.compat.MapUtil;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;

class WccMutateProcTest extends WccProcTest<WccMutateConfig> implements
    MutateNodePropertyTest<Wcc, WccMutateConfig, DisjointSetStruct> {

    @Override
    public String mutateProperty() {
        return "componentId";
    }

    @Override
    public ValueType mutatePropertyType() {
        return ValueType.LONG;
    }

    @Override
    public Class<? extends AlgoBaseProc<Wcc, DisjointSetStruct, WccMutateConfig, ?>> getProcedureClazz() {
        return WccMutateProc.class;
    }

    @Override
    public WccMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return WccMutateConfig.of(mapWrapper);
    }

    @Override
    public String expectedMutatedGraph() {
        return
            "  (a {componentId: 0})" +
            ", (b {componentId: 0})" +
            ", (c {componentId: 0})" +
            ", (d {componentId: 0})" +
            ", (e {componentId: 0})" +
            ", (f {componentId: 0})" +
            ", (g {componentId: 0})" +
            ", (h {componentId: 7})" +
            ", (i {componentId: 7})" +
            ", (j {componentId: 9})" +
            // {A, B, C, D}
            ", (a)-[{w: 1.0d}]->(b)" +
            ", (b)-[{w: 1.0d}]->(c)" +
            ", (c)-[{w: 1.0d}]->(d)" +
            ", (d)-[{w: 1.0d}]->(e)" +
            // {E, F, G}
            ", (e)-[{w: 1.0d}]->(f)" +
            ", (f)-[{w: 1.0d}]->(g)" +
            // {H, I}
            ", (h)-[{w: 1.0d}]->(i)";
    }

    @Test
    void testMutateAndWriteWithSeeding() throws Exception {
        registerProcedures(WccWriteProc.class);
        var testGraphName = "wccGraph";
        var initialGraphStore = new StoreLoaderBuilder().databaseService(db)
            .build()
            .graphStore();

        GraphStoreCatalog.set(emptyWithNameNative(getUsername(), testGraphName, nodeProperties()), initialGraphStore);

        var mutateQuery = GdsCypher
            .call(testGraphName)
            .algo("wcc")
            .mutateMode()
            .addParameter("mutateProperty", mutateProperty())
            .yields();

        runQuery(mutateQuery);

        var writeQuery = GdsCypher
            .call(testGraphName)
            .algo("wcc")
            .writeMode()
            .addParameter("seedProperty", mutateProperty())
            .addParameter("writeProperty", mutateProperty())
            .yields();

        runQuery(writeQuery);

        var updatedGraph = new StoreLoaderBuilder().databaseService(db)
            .addNodeProperty(mutateProperty(), mutateProperty(), DefaultValue.of(42.0), Aggregation.NONE)
            .build()
            .graph();

        assertGraphEquals(fromGdl(expectedMutatedGraph()), updatedGraph);
    }

    @Test
    void testMutateYields() {
        var testGraphName = "wccGraph";
        var initialGraphStore = new StoreLoaderBuilder().databaseService(db)
            .build()
            .graphStore();

        GraphStoreCatalog.set(emptyWithNameNative(getUsername(), testGraphName, nodeProperties()), initialGraphStore);

        String query = GdsCypher
            .call(testGraphName)
            .algo("wcc")
            .mutateMode()
            .addParameter("mutateProperty", mutateProperty())
            .yields(
                "nodePropertiesWritten",
                "preProcessingMillis",
                "computeMillis",
                "mutateMillis",
                "postProcessingMillis",
                "componentCount",
                "componentDistribution",
                "configuration"
            );

        runQueryWithRowConsumer(
            query,
            row -> {
                assertEquals(10L, row.getNumber("nodePropertiesWritten"));

                assertThat(-1L, lessThan(row.getNumber("preProcessingMillis").longValue()));
                assertThat(-1L, lessThan(row.getNumber("computeMillis").longValue()));
                assertThat(-1L, lessThan(row.getNumber("mutateMillis").longValue()));
                assertThat(-1L, lessThan(row.getNumber("postProcessingMillis").longValue()));

                assertEquals(3L, row.getNumber("componentCount"));
                assertUserInput(row, "threshold", 0D);
                assertUserInput(row, "consecutiveIds", false);

                assertEquals(MapUtil.map(
                    "p99", 7L,
                    "min", 1L,
                    "max", 7L,
                    "mean", 3.3333333333333335D,
                    "p999", 7L,
                    "p95", 7L,
                    "p90", 7L,
                    "p75", 7L,
                    "p50", 2L
                ), row.get("componentDistribution"));
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
            .algo("wcc")
            .mutateMode()
            .addParameter("mutateProperty", "foo")
            .yields("componentCount");

        assertCypherResult(query, List.of(Map.of("componentCount", 0L)));
    }

    @Disabled("This procedure for now does no longer extend MutateProc")
    @Override
    public void testExceptionLogging() {}
}
