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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MutateRelationshipWithPropertyTest;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.paths.astar.AStar;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarMutateConfig;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.config.MutateRelationshipConfig.MUTATE_RELATIONSHIP_TYPE_KEY;
import static org.neo4j.gds.paths.PathTestUtil.WRITE_RELATIONSHIP_TYPE;
import static org.neo4j.gds.paths.astar.config.ShortestPathAStarBaseConfig.LATITUDE_PROPERTY_KEY;
import static org.neo4j.gds.paths.astar.config.ShortestPathAStarBaseConfig.LONGITUDE_PROPERTY_KEY;

class ShortestPathAStarMutateProcTest extends ShortestPathAStarCompanionTest<ShortestPathAStarMutateConfig>
    implements MutateRelationshipWithPropertyTest<AStar, ShortestPathAStarMutateConfig, DijkstraResult> {

    private static final String EXISTING_GRAPH = "CREATE" +
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
        ", (nA)-[{w: 29.0}]->(nB)" +
        ", (nB)-[{w: 694.0}]->(nC)" +
        ", (nC)-[{w: 172.0}]->(nD)" +
        ", (nD)-[{w: 101.0}]->(nE)" +
        ", (nE)-[{w: 357.0}]->(nF)" +
        ", (nF)-[{w: 299.0}]->(nG)" +
        ", (nG)-[{w: 740.0}]->(nH)" +
        ", (nH)-[{w: 587.0}]->(nX)" +
        ", (nB)-[{w: 389.0}]->(nI)" +
        ", (nI)-[{w: 584.0}]->(nJ)" +
        ", (nJ)-[{w: 82.0}]->(nK)" +
        ", (nK)-[{w: 528.0}]->(nL)" +
        ", (nL)-[{w: 391.0}]->(nM)" +
        ", (nM)-[{w: 364.0}]->(nN)" +
        ", (nN)-[{w: 554.0}]->(nO)" +
        ", (nO)-[{w: 603.0}]->(nP)" +
        ", (nP)-[{w: 847.0}]->(nX)";

    @Override
    public String expectedMutatedGraph() {
        return EXISTING_GRAPH + ", (nA)-[:PATH {w: 8.0D}]->(nX)";
    }

    @Override
    public String mutateRelationshipType() {
        return WRITE_RELATIONSHIP_TYPE;
    }

    @Override
    public String mutateProperty() {
        return null;
    }

    @Override
    public ValueType mutatePropertyType() {
        return ValueType.DOUBLE;
    }

    public Optional<String> mutateGraphName() {
        return Optional.of(GRAPH_NAME);
    }

    @Override
    public Class<? extends AlgoBaseProc<AStar, DijkstraResult, ShortestPathAStarMutateConfig, ?>> getProcedureClazz() {
        return ShortestPathAStarMutateProc.class;
    }

    @Override
    public ShortestPathAStarMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return ShortestPathAStarMutateConfig.of(mapWrapper);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        mapWrapper = super.createMinimalConfig(mapWrapper);

        if (!mapWrapper.containsKey(MUTATE_RELATIONSHIP_TYPE_KEY)) {
            mapWrapper = mapWrapper.withString(MUTATE_RELATIONSHIP_TYPE_KEY, WRITE_RELATIONSHIP_TYPE);
        }

        return mapWrapper;
    }

    @Override
    @Test
    @Disabled("This test does not work for Dijkstra as no property is written")
    public void testMutateFailsOnExistingToken() {}


    @Override
    @Test
    @Disabled("This test does not work for Dijkstra as the source node is filtered")
    public void testGraphMutationOnFilteredGraph() {}

    @Override
    @Test
    @Disabled("This test does not work for Dijkstra as the source node is filtered")
    public void testWriteBackGraphMutationOnFilteredGraph() {}

    @Test
    void testWeightedMutate() {
        var config = createConfig(createMinimalConfig(CypherMapWrapper.empty()));

        var query = GdsCypher.call("graph")
            .algo("gds.shortestPath.astar")
            .mutateMode()
            .addParameter("sourceNode", config.sourceNode())
            .addParameter("targetNode", config.targetNode())
            .addParameter(LATITUDE_PROPERTY_KEY, config.latitudeProperty())
            .addParameter(LONGITUDE_PROPERTY_KEY, config.longitudeProperty())
            .addParameter("relationshipWeightProperty", "cost")
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

        var actual = GraphStoreCatalog.get(getUsername(), databaseId(), "graph").graphStore().getUnion();
        var expected = TestSupport.fromGdl(EXISTING_GRAPH + ", (nA)-[:PATH {w: 2979.0D}]->(nX)");

        assertGraphEquals(expected, actual);
    }
}
