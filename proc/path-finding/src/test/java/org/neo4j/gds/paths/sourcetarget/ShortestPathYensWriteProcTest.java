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
package org.neo4j.gds.paths.sourcetarget;

import org.apache.commons.lang3.mutable.MutableInt;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;
import org.neo4j.gds.paths.yens.Yens;
import org.neo4j.gds.paths.yens.config.ShortestPathYensWriteConfig;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.neo4j.gds.paths.PathTestUtil.WRITE_RELATIONSHIP_TYPE;
import static org.neo4j.gds.paths.PathTestUtil.validationQuery;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.graphalgo.config.WriteRelationshipConfig.WRITE_RELATIONSHIP_TYPE_KEY;

class ShortestPathYensWriteProcTest extends ShortestPathYensProcTest<ShortestPathYensWriteConfig> {

    @Override
    public Class<? extends AlgoBaseProc<Yens, DijkstraResult, ShortestPathYensWriteConfig>> getProcedureClazz() {
        return ShortestPathYensWriteProc.class;
    }

    @Override
    public ShortestPathYensWriteConfig createConfig(CypherMapWrapper mapWrapper) {
        return ShortestPathYensWriteConfig.of("", Optional.empty(), Optional.empty(), mapWrapper);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        mapWrapper = super.createMinimalConfig(mapWrapper);

        if (!mapWrapper.containsKey(WRITE_RELATIONSHIP_TYPE_KEY)) {
            mapWrapper = mapWrapper.withString(WRITE_RELATIONSHIP_TYPE_KEY, WRITE_RELATIONSHIP_TYPE);
        }

        return mapWrapper;
    }

    @Test
    void testWrite() {
        var config = createConfig(createMinimalConfig(CypherMapWrapper.empty()));

        var query = GdsCypher.call().explicitCreation("graph")
            .algo("gds.shortestPath.yens")
            .writeMode()
            .addParameter("sourceNode", config.sourceNode())
            .addParameter("targetNode", config.targetNode())
            .addParameter("k", config.k())
            .addParameter("relationshipWeightProperty", "cost")
            .addParameter("writeRelationshipType", WRITE_RELATIONSHIP_TYPE)
            .addParameter("writeNodeIds", true)
            .addParameter("writeCosts", true)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "relationshipsWritten", 3L,
            "createMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "writeMillis", greaterThan(-1L),
            "configuration", Matchers.isA(Map.class)
        )));

        assertCypherResult(validationQuery(idC), List.of(
            Map.of("totalCost", 5.0D, "nodeIds", ids0, "costs", costs0),
            Map.of("totalCost", 7.0D, "nodeIds", ids1, "costs", costs1),
            Map.of("totalCost", 8.0D, "nodeIds", ids2, "costs", costs2)
        ));
    }

    @ParameterizedTest
    @CsvSource(value = {"true,false", "false,true", "false,false"})
    void testWriteFlags(boolean writeNodeIds, boolean writeCosts) {
        var relationshipWeightProperty = "cost";

        var config = createConfig(createMinimalConfig(CypherMapWrapper.empty()));

        var query = GdsCypher.call().explicitCreation("graph")
            .algo("gds.shortestPath.yens")
            .writeMode()
            .addParameter("sourceNode", config.sourceNode())
            .addParameter("targetNode", config.targetNode())
            .addParameter("k", config.k())
            .addParameter("relationshipWeightProperty", relationshipWeightProperty)
            .addParameter("writeRelationshipType", WRITE_RELATIONSHIP_TYPE)
            .addParameter("writeNodeIds", writeNodeIds)
            .addParameter("writeCosts", writeCosts)
            .yields();

        runQuery(query);

        var validationQuery = "MATCH ()-[r:%s]->() RETURN r.nodeIds AS nodeIds, r.costs AS costs";
        var rowCount = new MutableInt(0);
        runQueryWithRowConsumer(formatWithLocale(validationQuery, WRITE_RELATIONSHIP_TYPE), row -> {
            rowCount.increment();
            var nodeIds = row.get("nodeIds");
            var costs = row.get("costs");

            if (writeNodeIds) {
                assertNotNull(nodeIds);
            } else {
                assertNull(nodeIds);
            }

            if (writeCosts) {
                assertNotNull(costs);
            } else {
                assertNull(costs);
            }
        });
        assertEquals(3, rowCount.getValue());
    }
}
