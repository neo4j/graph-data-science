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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.graphalgo.AbstractRelationshipProjections;
import org.neo4j.graphalgo.AlgoBaseProcTest;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.HeapControlTest;
import org.neo4j.graphalgo.MemoryEstimateTest;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.ALL_RELATIONSHIPS_UNDIRECTED_QUERY;
import static org.neo4j.graphalgo.config.GraphCreateFromStoreConfig.RELATIONSHIP_PROJECTION_KEY;

abstract class LocalClusteringCoefficientBaseProcTest<CONFIG extends LocalClusteringCoefficientBaseConfig> extends BaseProcTest
    implements AlgoBaseProcTest<LocalClusteringCoefficient, CONFIG, LocalClusteringCoefficient.Result>,
    MemoryEstimateTest<LocalClusteringCoefficient, CONFIG, LocalClusteringCoefficient.Result>,
    HeapControlTest<LocalClusteringCoefficient, CONFIG, LocalClusteringCoefficient.Result> {

    String dbCypher() {
        return "CREATE " +
               "(a:A)-[:T]->(b:A), " +
               "(b)-[:T]->(c:A), " +
               "(c)-[:T]->(a)";
    }

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphCreateProc.class,
            GraphWriteNodePropertiesProc.class,
            LocalClusteringCoefficientStreamProc.class
        );

        runQuery(dbCypher());
        runQuery("CALL gds.graph.create('g', 'A', {T: {orientation: 'UNDIRECTED'}})");
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }


    @Override
    public GraphDatabaseAPI graphDb() {
        return db;
    }

    @Override
    public void assertResultEquals(
        LocalClusteringCoefficient.Result result1, LocalClusteringCoefficient.Result result2
    ) {

    }

    @Override
    public RelationshipProjections relationshipProjections() {
        return AbstractRelationshipProjections.ALL_UNDIRECTED;
    }


    @Override
    public CypherMapWrapper createMinimalImplicitConfig(CypherMapWrapper mapWrapper) {
        if (mapWrapper.containsKey(RELATIONSHIP_PROJECTION_KEY)) {
            return createMinimalConfig(CypherMapWrapper.create(anonymousGraphConfig(mapWrapper.toMap())));
        }

        return createMinimalConfig(CypherMapWrapper.create(anonymousGraphConfig(mapWrapper
            .withEntry(RELATIONSHIP_PROJECTION_KEY, relationshipProjections())
            .toMap())));
    }

    @Override
    public String relationshipQuery() {
        return ALL_RELATIONSHIPS_UNDIRECTED_QUERY;
    }
}
