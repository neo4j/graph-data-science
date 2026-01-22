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
package org.neo4j.gds.paths.singlesource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.GdsCypher;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;
import static org.neo4j.gds.paths.PathTestUtil.WRITE_RELATIONSHIP_TYPE;
import static org.neo4j.gds.paths.PathTestUtil.validationQuery;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public abstract class AllShortestPathsWriteProcTest extends AllShortestPathsProcTest {
    @Test
    void testWrite() {
        var query = GdsCypher.call("graph")
            .algo(getProcedureName())
            .writeMode()
            .addParameter("sourceNode", idFunction.of("a"))
            .addParameter("relationshipWeightProperty", "cost")
            .addParameter("writeRelationshipType", WRITE_RELATIONSHIP_TYPE)
            .addParameter("writeNodeIds", true)
            .addParameter("writeCosts", true)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "relationshipsWritten", 6L,
            "preProcessingMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "writeMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));

        assertCypherResult(validationQuery(idA), List.of(
            Map.of("totalCost", 0.0D, "nodeIds", ids0, "costs", costs0),
            Map.of("totalCost", 2.0D, "nodeIds", ids1, "costs", costs1),
            Map.of("totalCost", 4.0D, "nodeIds", ids2, "costs", costs2),
            Map.of("totalCost", 5.0D, "nodeIds", ids3, "costs", costs3),
            Map.of("totalCost", 9.0D, "nodeIds", ids4, "costs", costs4),
            Map.of("totalCost", 20.0D, "nodeIds", ids5, "costs", costs5)
        ));
    }

    @ParameterizedTest
    @CsvSource(value = {"true,false", "false,true", "false,false"})
    void testWriteFlags(boolean writeNodeIds, boolean writeCosts) {
        var relationshipWeightProperty = "cost";

        var query = GdsCypher.call("graph")
            .algo(getProcedureName())
            .writeMode()
            .addParameter("sourceNode", idFunction.of("a"))
            .addParameter("relationshipWeightProperty", relationshipWeightProperty)
            .addParameter("writeRelationshipType", WRITE_RELATIONSHIP_TYPE)
            .addParameter("writeNodeIds", writeNodeIds)
            .addParameter("writeCosts", writeCosts)
            .yields();

        runQuery(query);

        var validationQuery = "MATCH ()-[r:%s]->() RETURN r.nodeIds AS nodeIds, r.costs AS costs";
        var rowCount = runQueryWithRowConsumer(formatWithLocale(validationQuery, WRITE_RELATIONSHIP_TYPE), row -> {
            var nodeIds = row.get("nodeIds");
            var costs = row.get("costs");

            if (writeNodeIds) {
                assertThat(nodeIds).isNotNull();
            } else {
                assertThat(nodeIds).isNull();
            }

            if (writeCosts) {
                assertThat(costs).isNotNull();
            } else {
                assertThat(costs).isNull();
            }
        });
        assertThat(rowCount).isEqualTo(6L);
    }

    @Test
    void testMemoryEstimation() {
        var query = GdsCypher.call("graph")
            .algo(getProcedureName())
            .estimationMode(GdsCypher.ExecutionModes.WRITE)
            .addParameter("sourceNode", idFunction.of("a"))
            .addParameter("writeRelationshipType", "WRITE")
            .yields("bytesMin", "bytesMax", "nodeCount", "relationshipCount");

        assertCypherResult(query, List.of(Map.of(
            "bytesMin", greaterThan(0L),
            "bytesMax", greaterThan(0L),
            "nodeCount", 6L,
            "relationshipCount", 7L
        )));
    }
}
