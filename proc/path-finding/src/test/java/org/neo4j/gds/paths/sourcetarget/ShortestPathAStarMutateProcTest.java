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
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.paths.PathTestUtil.WRITE_RELATIONSHIP_TYPE;
import static org.neo4j.gds.paths.astar.config.ShortestPathAStarBaseConfig.LATITUDE_PROPERTY_KEY;
import static org.neo4j.gds.paths.astar.config.ShortestPathAStarBaseConfig.LONGITUDE_PROPERTY_KEY;

class ShortestPathAStarMutateProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
            "  (nA:Label {latitude: 1.304444D,    longitude: 103.717373D})" +
            ", (nB:Label {latitude: 1.1892D,      longitude: 103.4689D})" +
            ", (nC:Label {latitude: 8.83055556D,  longitude: 111.8725D})" +
            ", (nD:Label {latitude: 10.82916667D, longitude: 113.9722222D})" +
            ", (nE:Label {latitude: 11.9675D,     longitude: 115.2366667D})" +
            ", (nF:Label {latitude: 16.0728D,     longitude: 119.6128D})" +
            ", (nG:Label {latitude: 20.5325D,     longitude: 121.845D})" +
            ", (nH:Label {latitude: 29.32611111D, longitude: 131.2988889D})" +
            ", (nI:Label {latitude: -2.0428D,     longitude: 108.6225D})" +
            ", (nJ:Label {latitude: -8.3256D,     longitude: 115.8872D})" +
            ", (nK:Label {latitude: -8.5945D,     longitude: 116.6867D})" +
            ", (nL:Label {latitude: -8.2211D,     longitude: 125.2411D})" +
            ", (nM:Label {latitude: -1.8558D,     longitude: 126.5572D})" +
            ", (nN:Label {latitude: 3.96861111D,  longitude: 128.3052778D})" +
            ", (nO:Label {latitude: 12.76305556D, longitude: 131.2980556D})" +
            ", (nP:Label {latitude: 22.32027778D, longitude: 134.700000D})" +
            ", (nX:Label {latitude: 35.562222D,   longitude: 140.059187D})" +
            ", (nA)-[:T{w: 29.0}]->(nB)" +
            ", (nB)-[:T{w: 694.0}]->(nC)" +
            ", (nC)-[:T{w: 172.0}]->(nD)" +
            ", (nD)-[:T{w: 101.0}]->(nE)" +
            ", (nE)-[:T{w: 357.0}]->(nF)" +
            ", (nF)-[:T{w: 299.0}]->(nG)" +
            ", (nG)-[:T{w: 740.0}]->(nH)" +
            ", (nH)-[:T{w: 587.0}]->(nX)" +
            ", (nB)-[:T{w: 389.0}]->(nI)" +
            ", (nI)-[:T{w: 584.0}]->(nJ)" +
            ", (nJ)-[:T{w: 82.0}]->(nK)" +
            ", (nK)-[:T{w: 528.0}]->(nL)" +
            ", (nL)-[:T{w: 391.0}]->(nM)" +
            ", (nM)-[:T{w: 364.0}]->(nN)" +
            ", (nN)-[:T{w: 554.0}]->(nO)" +
            ", (nO)-[:T{w: 603.0}]->(nP)" +
            ", (nP)-[:T{w: 847.0}]->(nX)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            ShortestPathAStarMutateProc.class,
            GraphProjectProc.class
        );

        runQuery(GdsCypher.call("graph")
            .graphProject()
            .withNodeLabel("Label")
            .withNodeProperty("latitude")
            .withNodeProperty("longitude")
            .withRelationshipType("T")
            .withRelationshipProperty("w")
            .yields());
    }

    @Test
    void testWeightedMutate() {

        var query = GdsCypher.call("graph")
            .algo("gds.shortestPath.astar")
            .mutateMode()
            .addParameter("sourceNode", idFunction.of("nA"))
            .addParameter("targetNode", idFunction.of("nX"))
            .addParameter(LATITUDE_PROPERTY_KEY, "latitude")
            .addParameter(LONGITUDE_PROPERTY_KEY, "longitude")
            .addParameter("relationshipWeightProperty", "w")
            .addParameter("mutateRelationshipType", WRITE_RELATIONSHIP_TYPE)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "relationshipsWritten", 1L,
            "preProcessingMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "mutateMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));

        var actualGraph = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), "graph")
            .graphStore()
            .getUnion();
        var expected = TestSupport.fromGdl(DB_CYPHER + ", (nA)-[:PATH {w: 2979.0D}]->(nX)");

        assertGraphEquals(expected, actualGraph);

    }
}
