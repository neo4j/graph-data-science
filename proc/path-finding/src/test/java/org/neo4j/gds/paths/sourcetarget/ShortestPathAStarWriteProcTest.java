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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;
import static org.neo4j.gds.paths.PathTestUtil.WRITE_RELATIONSHIP_TYPE;
import static org.neo4j.gds.paths.PathTestUtil.validationQuery;
import static org.neo4j.gds.paths.astar.config.ShortestPathAStarBaseConfig.LATITUDE_PROPERTY_KEY;
import static org.neo4j.gds.paths.astar.config.ShortestPathAStarBaseConfig.LONGITUDE_PROPERTY_KEY;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class ShortestPathAStarWriteProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
            "  (:Offset)" +
            ", (nA:Label {latitude: 1.304444,    longitude: 103.717373})" + // name: 'SINGAPORE'
            ", (nB:Label {latitude: 1.1892,      longitude: 103.4689})" + // name: 'SINGAPORE STRAIT'
            ", (nC:Label {latitude: 8.83055556,  longitude: 111.8725})" + // name: 'WAYPOINT 68'
            ", (nD:Label {latitude: 10.82916667, longitude: 113.9722222})" + // name: 'WAYPOINT 70'
            ", (nE:Label {latitude: 11.9675,     longitude: 115.2366667})" + // name: 'WAYPOINT 74'
            ", (nF:Label {latitude: 16.0728,     longitude: 119.6128})" + // name: 'SOUTH CHINA SEA'
            ", (nG:Label {latitude: 20.5325,     longitude: 121.845})" + // name: 'LUZON STRAIT'
            ", (nH:Label {latitude: 29.32611111, longitude: 131.2988889})" + // name: 'WAYPOINT 87'
            ", (nI:Label {latitude: -2.0428,     longitude: 108.6225})" + // name: 'KARIMATA STRAIT'
            ", (nJ:Label {latitude: -8.3256,     longitude: 115.8872})" + // name: 'LOMBOK STRAIT'
            ", (nK:Label {latitude: -8.5945,     longitude: 116.6867})" + // name: 'SUMBAWA STRAIT'
            ", (nL:Label {latitude: -8.2211,     longitude: 125.2411})" + // name: 'KOLANA AREA'
            ", (nM:Label {latitude: -1.8558,     longitude: 126.5572})" + // name: 'EAST MANGOLE'
            ", (nN:Label {latitude: 3.96861111,  longitude: 128.3052778})" + // name: 'WAYPOINT 88'
            ", (nO:Label {latitude: 12.76305556, longitude: 131.2980556})" + // name: 'WAYPOINT 89'
            ", (nP:Label {latitude: 22.32027778, longitude: 134.700000})" + // name: 'WAYPOINT 90'
            ", (nX:Label {latitude: 35.562222,   longitude: 140.059187})" + // name: 'CHIBA'
            ", (nA)-[:TYPE {cost: 29.0}]->(nB)" +
            ", (nB)-[:TYPE {cost: 694.0}]->(nC)" +
            ", (nC)-[:TYPE {cost: 172.0}]->(nD)" +
            ", (nD)-[:TYPE {cost: 101.0}]->(nE)" +
            ", (nE)-[:TYPE {cost: 357.0}]->(nF)" +
            ", (nF)-[:TYPE {cost: 299.0}]->(nG)" +
            ", (nG)-[:TYPE {cost: 740.0}]->(nH)" +
            ", (nH)-[:TYPE {cost: 587.0}]->(nX)" +
            ", (nB)-[:TYPE {cost: 389.0}]->(nI)" +
            ", (nI)-[:TYPE {cost: 584.0}]->(nJ)" +
            ", (nJ)-[:TYPE {cost: 82.0}]->(nK)" +
            ", (nK)-[:TYPE {cost: 528.0}]->(nL)" +
            ", (nL)-[:TYPE {cost: 391.0}]->(nM)" +
            ", (nM)-[:TYPE {cost: 364.0}]->(nN)" +
            ", (nN)-[:TYPE {cost: 554.0}]->(nO)" +
            ", (nO)-[:TYPE {cost: 603.0}]->(nP)" +
            ", (nP)-[:TYPE {cost: 847.0}]->(nX)";

    long idA, idB, idC, idD, idE, idF, idG, idH, idX;
    long[] ids0;
    double[] costs0;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            ShortestPathAStarWriteProc.class,
            GraphProjectProc.class
        );

        idA = idFunction.of("nA");
        idB = idFunction.of("nB");
        idC = idFunction.of("nC");
        idD = idFunction.of("nD");
        idE = idFunction.of("nE");
        idF = idFunction.of("nF");
        idG = idFunction.of("nG");
        idH = idFunction.of("nH");
        idX = idFunction.of("nX");

        ids0 = new long[]{idA, idB, idC, idD, idE, idF, idG, idH, idX};
        costs0 = new double[]{0.0, 29.0, 723.0, 895.0, 996.0, 1353.0, 1652.0, 2392.0, 2979.0};

        runQuery(GdsCypher.call("graph")
            .graphProject()
            .withNodeLabel("Label")
            .withNodeProperty("latitude")
            .withNodeProperty("longitude")
            .withAnyRelationshipType()
            .withRelationshipProperty("cost")
            .yields());
    }

    @Test
    void testWrite() {
        var relationshipWeightProperty = "cost";


        var query = GdsCypher.call("graph")
            .algo("gds.shortestPath.astar")
            .writeMode()
            .addParameter("sourceNode", idA)
            .addParameter("targetNode", idX)
            .addParameter(LATITUDE_PROPERTY_KEY, "latitude")
            .addParameter(LONGITUDE_PROPERTY_KEY, "longitude")
            .addParameter("relationshipWeightProperty", relationshipWeightProperty)
            .addParameter("writeRelationshipType", WRITE_RELATIONSHIP_TYPE)
            .addParameter("writeNodeIds", true)
            .addParameter("writeCosts", true)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "relationshipsWritten", 1L,
            "preProcessingMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "writeMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));

        assertCypherResult(validationQuery(idA), List.of(Map.of("totalCost", 2979.0D, "nodeIds", ids0, "costs", costs0)));
    }

    @ParameterizedTest
    @CsvSource(value = {"true,false", "false,true", "false,false"})
    void testWriteFlags(boolean writeNodeIds, boolean writeCosts) {
        var relationshipWeightProperty = "cost";


        var query = GdsCypher.call("graph")
            .algo("gds.shortestPath.astar")
            .writeMode()
            .addParameter("sourceNode", idA)
            .addParameter("targetNode", idX)
            .addParameter(LATITUDE_PROPERTY_KEY, "latitude")
            .addParameter(LONGITUDE_PROPERTY_KEY, "longitude")
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
        assertThat(rowCount).isEqualTo(1);
    }
}
