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
package org.neo4j.graphalgo.triangle;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AbstractRelationshipProjections;
import org.neo4j.graphalgo.AlgoBaseProcTest;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.HeapControlTest;
import org.neo4j.graphalgo.MemoryEstimateTest;
import org.neo4j.graphalgo.OnlyUndirectedTest;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.extension.Neo4jGraph;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.ALL_RELATIONSHIPS_UNDIRECTED_QUERY;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.RELATIONSHIP_QUERY_KEY;
import static org.neo4j.graphalgo.config.GraphCreateFromStoreConfig.RELATIONSHIP_PROJECTION_KEY;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

abstract class TriangleCountBaseProcTest<CONFIG extends TriangleCountBaseConfig> extends BaseProcTest
    implements AlgoBaseProcTest<IntersectingTriangleCount, CONFIG, IntersectingTriangleCount.TriangleCountResult>,
    OnlyUndirectedTest<IntersectingTriangleCount, CONFIG, IntersectingTriangleCount.TriangleCountResult>,
    MemoryEstimateTest<IntersectingTriangleCount, CONFIG, IntersectingTriangleCount.TriangleCountResult>,
    HeapControlTest<IntersectingTriangleCount, CONFIG, IntersectingTriangleCount.TriangleCountResult> {

    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE " +
           "(a:A)-[:T]->(b:A), " +
           "(b)-[:T]->(c:A), " +
           "(c)-[:T]->(a)";

    protected static final String TEST_GRAPH_NAME = "g";


    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphCreateProc.class,
            GraphWriteNodePropertiesProc.class,
            getProcedureClazz()
        );

        runQuery(formatWithLocale("CALL gds.graph.create('%s', 'A', {T: { orientation: 'UNDIRECTED'}})", TEST_GRAPH_NAME));
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
        IntersectingTriangleCount.TriangleCountResult result1, IntersectingTriangleCount.TriangleCountResult result2
    ) {
        // TODO: add checks for the HugeArrays
        assertEquals(result1.globalTriangles(), result2.globalTriangles());
    }

    @Override
    public RelationshipProjections relationshipProjections() {
        return AbstractRelationshipProjections.ALL_UNDIRECTED;
    }


    @Override
    public CypherMapWrapper createMinimalImplicitConfig(CypherMapWrapper mapWrapper) {
        if (mapWrapper.containsKey(RELATIONSHIP_PROJECTION_KEY) || mapWrapper.containsKey(RELATIONSHIP_QUERY_KEY)) {
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

    @Test
    void testMaxDegreeValidation() {
        CypherMapWrapper config = createMinimalConfig(CypherMapWrapper.empty().withNumber("maxDegree", 1L));

        applyOnProcedure(proc -> {
            IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> proc.newConfig(Optional.of("g"), config)
            );

            String message = exception.getMessage();
            assertTrue(message.contains("maxDegree") && message.contains("greater than 1") );
        });
    }

    @Override
    public void loadGraph(String graphName){
        String graphCreateQuery = GdsCypher.call()
            .withAnyLabel()
            .withRelationshipType("T", Orientation.UNDIRECTED)
            .graphCreate(graphName)
            .yields();

        runQuery(graphCreateQuery);
    }
}
