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
package org.neo4j.graphalgo.triangle;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.WritePropertyConfigTest;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TriangleCountWriteProcTest
    extends TriangleCountBaseProcTest<TriangleCountWriteConfig>
    implements WritePropertyConfigTest<IntersectingTriangleCount, TriangleCountWriteConfig, IntersectingTriangleCount.TriangleCountResult> {

    String dbCypher() {
        return "CREATE " +
               "(a:A { name: 'a' })-[:T]->(b:A { name: 'b' }), " +
               "(b)-[:T]->(c:A { name: 'c' }), " +
               "(c)-[:T]->(a), " +
               "(a)-[:T]->(d:A { name: 'd' }), " +
               "(b)-[:T]->(d), " +
               "(c)-[:T]->(d), " +
               "(a)-[:T]->(e:A { name: 'e' }), " +
               "(b)-[:T]->(e) ";
    }

    @Test
    void testWrite() {
        var query = GdsCypher.call()
            .loadEverything(Orientation.UNDIRECTED)
            .algo("triangleCount")
            .writeMode()
            .addParameter("writeProperty", "triangles")
            .addParameter("clusteringCoefficientProperty", "clusteringCoefficient")
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "triangleCount", 5L,
            "averageClusteringCoefficient", closeTo(13.0 / 15, 1e-10),
            "nodeCount", 5L,
            "createMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "configuration", isA(Map.class),
            "writeMillis", greaterThan(-1L),
            "nodePropertiesWritten", 10L
        )));

        Map<String, Map<Long, Double>> expectedResult = Map.of(
            "a", Map.of(4L, 2.0 / 3),
            "b", Map.of(4L, 2.0 / 3),
            "c", Map.of(3L, 1.0),
            "d", Map.of(3L, 1.0),
            "e", Map.of(1L, 1.0)
        );

        assertWriteResult(expectedResult, "triangles", "clusteringCoefficient");
    }

    @Test
    void testMissingClusteringCoefficientPropertyFails() {
        CypherMapWrapper mapWrapper =
            createMinimalConfig(CypherMapWrapper.empty())
                .withoutEntry("clusteringCoefficientProperty");

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> createConfig(mapWrapper)
        );
        assertEquals(
            "No value specified for the mandatory configuration parameter `clusteringCoefficientProperty`",
            exception.getMessage()
        );
    }

    @Test
    void testEmptyClusteringCoefficientPropertyValues() {
        CypherMapWrapper mapWrapper = CypherMapWrapper.create(MapUtil.map("clusteringCoefficientProperty", null));
        assertThrows(IllegalArgumentException.class, () -> createConfig(mapWrapper));
    }

    @Test
    void testTrimmedToNullClusteringCoefficientProperty() {
        CypherMapWrapper mapWrapper = CypherMapWrapper.create(MapUtil.map("clusteringCoefficientProperty", "  "));
        assertThrows(IllegalArgumentException.class, () -> createConfig(mapWrapper));
    }

    @Override
    public Class<? extends AlgoBaseProc<IntersectingTriangleCount, IntersectingTriangleCount.TriangleCountResult, TriangleCountWriteConfig>> getProcedureClazz() {
        return TriangleCountWriteProc.class;
    }

    @Override
    public TriangleCountWriteConfig createConfig(CypherMapWrapper mapWrapper) {
        return TriangleCountWriteConfig.of(
            getUsername(),
            Optional.empty(),
            Optional.empty(),
            mapWrapper
        );
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        if (!mapWrapper.containsKey("clusteringCoefficientProperty")) {
            mapWrapper = mapWrapper.withString("clusteringCoefficientProperty", "clusteringCoefficientProperty");
        }
        if (!mapWrapper.containsKey("writeProperty")) {
            mapWrapper = mapWrapper.withString("writeProperty", "writeProperty");
        }
        return mapWrapper;
    }

    private void assertWriteResult(
        Map<String, Map<Long, Double>> expectedResult,
        String writeProperty,
        String clusteringCoefficientProperty
    ) {
        runQueryWithRowConsumer(String.format(
            "MATCH (n) RETURN n.name as name, n.%s as triangles, n.%s as clusteringCoefficient",
            writeProperty,
            clusteringCoefficientProperty
        ), (row) -> {
            long triangles = row.getNumber("triangles").longValue();
            double clusteringCoefficient = row.getNumber("clusteringCoefficient").doubleValue();
            String name = row.getString("name");
            Map<Long, Double> trianglesAndCoefficient = expectedResult.get(name);
            assertNotNull(
                trianglesAndCoefficient,
                String.format("There is no record in the expected result for node : %s", name)
            );
            Double coefficient = trianglesAndCoefficient.get(triangles);
            assertEquals(coefficient, clusteringCoefficient,
                String.format("Incorrect clustering coefficient for node %s", name)
            );
        });
    }

}