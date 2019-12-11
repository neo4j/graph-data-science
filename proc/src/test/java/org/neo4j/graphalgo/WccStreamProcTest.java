/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo;

import com.carrotsearch.hppc.IntIntScatterMap;
import org.neo4j.graphalgo.TestSupport.AllGraphNamesTest;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphalgo.impl.wcc.WccStreamConfig;
import org.neo4j.graphdb.QueryExecutionException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.core.ProcedureConstants.DEPRECATED_RELATIONSHIP_PROPERTY_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.RELATIONSHIP_WEIGHT_KEY;

class WccStreamProcTest extends WccProcBaseTest<WccStreamConfig> {

    @Override
    public Class<? extends BaseAlgoProc<?, DisjointSetStruct, WccStreamConfig>> getProcedureClazz() {
        return null;
    }

    @Override
    public WccStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return null;
    }

    @AllGraphNamesTest
    void testWCCStream(String graphImpl) {
        String query = "CALL algo.beta.wcc.stream(" +
                       "    '', 'TYPE', {" +
                       "        graph: $graph" +
                       "    }" +
                       ") YIELD setId";

        IntIntScatterMap map = new IntIntScatterMap(11);
        runQuery(query, MapUtil.map("graph", graphImpl),
            row -> map.addTo(row.getNumber("setId").intValue(), 1)
        );
        assertMapContains(map, 1, 2, 7);
    }

    @AllGraphNamesTest
    void testThresholdWCCStream(String graphImpl) {
        String query = "CALL algo.beta.wcc.stream(" +
                       "    '', 'TYPE', {" +
                       "        weightProperty: 'cost', defaultValue: 10.0, threshold: 5.0, concurrency: 1, graph: $graph" +
                       "    }" +
                       ") YIELD setId";

        IntIntScatterMap map = new IntIntScatterMap(11);
        runQuery(query, MapUtil.map("graph", graphImpl),
            row -> map.addTo(row.getNumber("setId").intValue(), 1)
        );
        assertMapContains(map, 4, 3, 2, 1);
    }

    @AllGraphNamesTest
    void testThresholdWCCLowThreshold(String graphImpl) {
        String query = "CALL algo.beta.wcc.stream(" +
                       "    '', 'TYPE', {" +
                       "        weightProperty: 'cost', defaultValue: 10.0, concurrency: 1, threshold: 3.14, graph: $graph" +
                       "    }" +
                       ") YIELD setId";
        IntIntScatterMap map = new IntIntScatterMap(11);
        runQuery(query, MapUtil.map("graph", graphImpl),
            row -> {
                map.addTo(row.getNumber("setId").intValue(), 1);
            }
        );
        assertMapContains(map, 1, 2, 7);
    }

    @AllGraphNamesTest
    void shouldFailWhenSpecifyingThresholdWithoutRelationshipWeight(String graphImpl) {
        String query = "CALL algo.beta.wcc.stream(" +
                       "    '', 'TYPE', {" +
                       "        defaultValue: 10.0, concurrency: 1, threshold: 3.14, graph: $graph" +
                       "    }" +
                       ") YIELD setId";
        QueryExecutionException exception = assertThrows(
            QueryExecutionException.class,
            () -> runQuery(query, MapUtil.map("graph", graphImpl))
        );
        Throwable rootCause = ExceptionUtil.rootCause(exception);

        assertTrue(rootCause
            .getMessage()
            .contains(String.format(
                "%s requires a `%s` or `%s`",
                0D,
                RELATIONSHIP_WEIGHT_KEY,
                DEPRECATED_RELATIONSHIP_PROPERTY_KEY
            )));
    }
}
