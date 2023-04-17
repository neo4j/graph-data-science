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

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProcTest;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.QueryRunner;
import org.neo4j.gds.SourceNodeConfigTest;
import org.neo4j.gds.TargetNodeConfigTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.config.ImmutableGraphProjectFromCypherConfig;
import org.neo4j.gds.config.ImmutableGraphProjectFromStoreConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.GraphLoader;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.paths.ShortestPathBaseConfig;
import org.neo4j.gds.paths.astar.AStar;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;
import org.neo4j.graphdb.GraphDatabaseService;

import static org.neo4j.gds.paths.ShortestPathBaseConfig.SOURCE_NODE_KEY;
import static org.neo4j.gds.paths.ShortestPathBaseConfig.TARGET_NODE_KEY;
import static org.neo4j.gds.paths.astar.config.ShortestPathAStarBaseConfig.LATITUDE_PROPERTY_KEY;
import static org.neo4j.gds.paths.astar.config.ShortestPathAStarBaseConfig.LONGITUDE_PROPERTY_KEY;

abstract class ShortestPathAStarProcTest<CONFIG extends ShortestPathBaseConfig> extends BaseProcTest implements
    AlgoBaseProcTest<AStar, CONFIG, DijkstraResult>,
    SourceNodeConfigTest<AStar, CONFIG, DijkstraResult>,
    TargetNodeConfigTest<AStar, CONFIG, DijkstraResult> {

    private static final String NODE_QUERY = "MATCH (n) RETURN id(n) AS id, n.latitude AS latitude, n.longitude AS longitude";

    static final String LONGITUDE_PROPERTY = "longitude";
    static final String LATITUDE_PROPERTY = "latitude";
    static final String COST_PROPERTY = "cost";
    protected static final String GRAPH_NAME = "graph";

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
            getProcedureClazz(),
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

        runQuery(GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .withNodeLabel("Label")
            .withNodeProperty(LATITUDE_PROPERTY)
            .withNodeProperty(LONGITUDE_PROPERTY)
            .withAnyRelationshipType()
            .withRelationshipProperty(COST_PROPERTY)
            .yields());
    }

    @AfterEach
    void teardown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Override
    public GraphDatabaseService graphDb() {
        return db;
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        return mapWrapper
            .withNumber(SOURCE_NODE_KEY, idFunction.of("nA"))
            .withNumber(TARGET_NODE_KEY, idFunction.of("nX"))
            .withString(LONGITUDE_PROPERTY_KEY, LONGITUDE_PROPERTY)
            .withString(LATITUDE_PROPERTY_KEY, LATITUDE_PROPERTY);
    }

    /**
     * From here it's just some voodoo to make all this test machinery work ...
     */
    @Override
    public @NotNull GraphLoader graphLoader(GraphProjectConfig graphProjectConfig) {
        GraphProjectConfig configWithNodeProperty = graphProjectConfig instanceof GraphProjectFromStoreConfig
            ? ImmutableGraphProjectFromStoreConfig
            .builder()
            .from(graphProjectConfig)
            .nodeProperties(PropertyMappings.of(
                PropertyMapping.of(LONGITUDE_PROPERTY),
                PropertyMapping.of(LATITUDE_PROPERTY))
            )
            .build()
            : ImmutableGraphProjectFromCypherConfig
                .builder()
                .from(graphProjectConfig)
                .nodeQuery(NODE_QUERY)
                .build();

        return graphLoader(graphDb(), configWithNodeProperty);
    }

    @Override
    public void loadGraph(String graphName) {
        QueryRunner.runQuery(
            graphDb(),
            GdsCypher.call(graphName)
                .graphProject()
                .withAnyLabel()
                .withNodeProperty(LATITUDE_PROPERTY)
                .withNodeProperty(LONGITUDE_PROPERTY)
                .withAnyRelationshipType()
                .withRelationshipProperty(COST_PROPERTY)
                .yields()
        );
    }

    @Test
    @Disabled
    @Override
    public void testRunOnEmptyGraph() {
        // graph must not be empty
    }
}
