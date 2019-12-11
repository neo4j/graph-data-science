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
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphalgo.impl.wcc.WccStreamConfig;
import org.neo4j.graphalgo.newapi.ImmutableGraphCreateConfig;
import org.neo4j.graphalgo.wcc.WccStreamProc;

import java.util.Optional;

class WccStreamProcTest extends WccProcBaseTest<WccStreamConfig> {

    @Override
    public Class<? extends BaseAlgoProc<?, DisjointSetStruct, WccStreamConfig>> getProcedureClazz() {
        return WccStreamProc.class;
    }

    @Override
    public WccStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return WccStreamConfig.of("", Optional.empty(), Optional.empty(), mapWrapper);
    }

    @Test
    void testWCCStreamWithDefaults() {
        String query = GdsCypher.call("wcc")
            .streamMode()
            .implicitCreation(ImmutableGraphCreateConfig
                .builder()
                .graphName("testGraph")
                .nodeProjection(NodeProjections.empty())
                .relationshipProjection(RelationshipProjections.empty())
                .build()
            ).yields("setId");

        IntIntScatterMap map = new IntIntScatterMap(11);
        runQuery(query,
            row -> map.addTo(row.getNumber("setId").intValue(), 1)
        );
        assertMapContains(map, 1, 2, 7);
    }
}
