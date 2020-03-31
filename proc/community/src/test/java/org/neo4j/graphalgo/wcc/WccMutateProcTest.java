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
package org.neo4j.graphalgo.wcc;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.GraphMutationTest;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;

import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;

class WccMutateProcTest extends WccProcTest<WccMutateConfig> implements GraphMutationTest<WccMutateConfig, DisjointSetStruct> {

    @Override
    public String mutateProperty() {
        return "componentId";
    }

    @Override
    public Class<? extends AlgoBaseProc<?, DisjointSetStruct, WccMutateConfig>> getProcedureClazz() {
        return WccMutateProc.class;
    }

    @Override
    public WccMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return WccMutateConfig.of(getUsername(), Optional.empty(), Optional.empty(), mapWrapper);
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
    void testMutateYields() {
        String query = GdsCypher
            .call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("wcc")
            .mutateMode()
            .addParameter("mutateProperty", mutateProperty())
            .yields(
                "nodePropertiesWritten",
                "createMillis",
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

                assertThat(-1L, lessThan(row.getNumber("createMillis").longValue()));
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
                    "p90", 7L,
                    "p50", 2L,
                    "p999", 7L,
                    "p95", 7L,
                    "p75", 2L
                ), row.get("componentDistribution"));
            }
        );
    }
}


