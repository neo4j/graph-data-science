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
package org.neo4j.gds.core;

import org.neo4j.gds.BaseTest;
import org.neo4j.gds.CypherLoaderBuilder;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.config.GraphProjectFromCypherConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.GraphFactoryTestSupport.AllGraphStoreFactoryTypesTest;
import static org.neo4j.gds.GraphFactoryTestSupport.FactoryType;
import static org.neo4j.gds.GraphFactoryTestSupport.FactoryType.CYPHER;
import static org.neo4j.gds.GraphHelper.assertRelationships;
import static org.neo4j.gds.compat.GraphDatabaseApiProxy.applyInTransaction;

class GraphLoaderDirectionalityTest extends BaseTest {

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node)" +
        ", (b:Node)" +
        ", (c:Node)" +
        ", (d:Node) " +
        ", (a)-[:REL]->(a)" +
        ", (b)-[:REL]->(b)" +
        ", (a)-[:REL]->(b)" +
        ", (b)-[:REL]->(c)" +
        ", (b)-[:REL]->(d)";

    private static final String RELATIONSHIP_QUERY_BOTH = "MATCH (n)--(m) RETURN id(n) AS source, id(m) AS target";
    private static final String RELATIONSHIP_QUERY_OUTGOING = "MATCH (n)-->(m) RETURN id(n) AS source, id(m) AS target";
    private static final String RELATIONSHIP_QUERY_INCOMING = "MATCH (n)<--(m) RETURN id(n) AS source, id(m) AS target";
    private static final String RELATIONSHIP_QUERY_UNDIRECTED =
        "MATCH (n)-->(m)" +
        "RETURN id(n) AS source, id(m) AS target " +
        "UNION ALL " +
        "MATCH (n)<--(m) " +
        "RETURN id(n) AS source, id(m) AS target";

    private static final String RELATIONSHIP_QUERY_UNDIRECTED_SINGLE =
        "MATCH (n)-->(m)" +
        "RETURN id(n) AS source, id(m) AS target " +
        "UNION " +
        "MATCH (n)<--(m) " +
        "RETURN id(n) AS source, id(m) AS target";

    @AllGraphStoreFactoryTypesTest
    void loadUndirected(FactoryType factoryType) {
        Graph graph = loadDirectedGraph(factoryType, RELATIONSHIP_QUERY_BOTH, Orientation.UNDIRECTED);
        assertEquals(4L, graph.nodeCount());
        assertRelationships(graph, 0, 0, 1);
        assertRelationships(graph, 1, 0, 1, 2, 3);
        assertRelationships(graph, 2, 1);
        assertRelationships(graph, 3, 1);
    }

    @AllGraphStoreFactoryTypesTest
    void loadNatural(FactoryType factoryType) {
        Graph graph = loadDirectedGraph(factoryType, RELATIONSHIP_QUERY_OUTGOING, Orientation.NATURAL);

        assertEquals(4L, graph.nodeCount());
        assertRelationships(graph, 0, 0, 1);
        assertRelationships(graph, 1, 1, 2, 3);
        assertRelationships(graph, 2);
        assertRelationships(graph, 3);
    }

    @AllGraphStoreFactoryTypesTest
    void loadReverse(FactoryType factoryType) {
        Graph graph = loadDirectedGraph(factoryType, RELATIONSHIP_QUERY_INCOMING, Orientation.REVERSE);

        assertEquals(4L, graph.nodeCount());
        assertRelationships(graph, 0, 0);
        assertRelationships(graph, 1, 0, 1);
        assertRelationships(graph, 2, 1);
        assertRelationships(graph, 3, 1);
    }

    @AllGraphStoreFactoryTypesTest
    void loadNaturalWithoutAggregation(FactoryType factoryType) {
        Graph graph = loadDirectedGraph(factoryType, RELATIONSHIP_QUERY_OUTGOING, Orientation.NATURAL);

        assertEquals(4L, graph.nodeCount());
        assertRelationships(graph, 0, 0, 1);
        assertRelationships(graph, 1, 1, 2, 3);
        assertRelationships(graph, 2);
        assertRelationships(graph, 3);
    }

    @AllGraphStoreFactoryTypesTest
    void loadUndirectedWithAggregation(FactoryType factoryType) {
        Graph graph = loadUndirectedGraph(factoryType, Aggregation.SINGLE);

        assertEquals(4L, graph.nodeCount());
        assertRelationships(graph, 0, 0, 1);
        assertRelationships(graph, 1, 0, 1, 2, 3);
        assertRelationships(graph, 2, 1);
        assertRelationships(graph, 3, 1);
    }

    @AllGraphStoreFactoryTypesTest
    void loadUndirectedWithoutAggregation(FactoryType factoryType) {
        Graph graph = loadUndirectedGraph(factoryType, Aggregation.NONE);

        assertEquals(4L, graph.nodeCount());
        assertRelationships(graph, 0, 0, 0, 1);
        assertRelationships(graph, 1, 0, 1, 1, 2, 3);
        assertRelationships(graph, 2, 1);
        assertRelationships(graph, 3, 1);
    }

    @AllGraphStoreFactoryTypesTest
    void loadUndirectedNodeWithSelfReference(FactoryType factoryType) {
        runUndirectedNodeWithSelfReference(
            factoryType,
            "CREATE" +
            "  (a:Node)" +
            ", (b:Node) " +
            ", (a)-[:REL]->(a)" +
            ", (a)-[:REL]->(b)"
        );
    }

    @AllGraphStoreFactoryTypesTest
    void loadUndirectedNodeWithSelfReference2(FactoryType factoryType) {
        runUndirectedNodeWithSelfReference(
            factoryType,
            "CREATE" +
            "  (a:Node)" +
            ", (b:Node) " +
            ", (a)-[:REL]->(b)" +
            ", (a)-[:REL]->(a)"
        );
    }

    @AllGraphStoreFactoryTypesTest
    void loadUndirectedNodesWithMultipleSelfReferences(FactoryType factoryType) {
        runUndirectedNodesWithMultipleSelfReferences(
            factoryType,
            "CREATE" +
            "  (a:Node)" +
            ", (b:Node)" +
            ", (c:Node)" +
            ", (d:Node) " +
            ", (a)-[:REL]->(a)" +
            ", (b)-[:REL]->(b)" +
            ", (a)-[:REL]->(b)" +
            ", (b)-[:REL]->(c)" +
            ", (b)-[:REL]->(d)"
        );
    }

    @AllGraphStoreFactoryTypesTest
    void loadUndirectedNodesWithMultipleSelfReferences2(FactoryType factoryType) {
        runUndirectedNodesWithMultipleSelfReferences(
            factoryType,
            "CREATE" +
            "  (a:Node)" +
            ", (b:Node)" +
            ", (c:Node)" +
            ", (d:Node) " +
            ", (a)-[:REL]->(b)" +
            ", (a)-[:REL]->(a)" +
            ", (b)-[:REL]->(c)" +
            ", (b)-[:REL]->(d)" +
            ", (b)-[:REL]->(b)"
        );
    }

    private void runUndirectedNodeWithSelfReference(FactoryType factoryType, String cypher) {
        Graph graph = loadGraph(
            cypher,
            factoryType,
            RELATIONSHIP_QUERY_UNDIRECTED_SINGLE,
            Orientation.UNDIRECTED,
            Aggregation.SINGLE
        );

        assertEquals(2L, graph.nodeCount());
        assertRelationships(graph, 0, 0, 1);
        assertRelationships(graph, 1, 0);
    }

    private void runUndirectedNodesWithMultipleSelfReferences(FactoryType factoryType, String cypher) {
        Graph graph = loadGraph(
            cypher,
            factoryType,
            RELATIONSHIP_QUERY_UNDIRECTED_SINGLE,
            Orientation.UNDIRECTED,
            Aggregation.SINGLE
        );

        assertEquals(4L, graph.nodeCount());
        assertRelationships(graph, 0, 0, 1);
        assertRelationships(graph, 1, 0, 1, 2, 3);
        assertRelationships(graph, 2, 1);
        assertRelationships(graph, 3, 1);
    }

    private Graph loadDirectedGraph(
        FactoryType factoryType,
        String relationshipQuery,
        Orientation orientation
    ) {
        return loadGraph(DB_CYPHER, factoryType, relationshipQuery, orientation, Aggregation.SINGLE);
    }

    private Graph loadUndirectedGraph(
        FactoryType factoryType,
        Aggregation aggregation
    ) {
        String relQuery = aggregation == Aggregation.SINGLE ? RELATIONSHIP_QUERY_UNDIRECTED_SINGLE : RELATIONSHIP_QUERY_UNDIRECTED;
        return loadGraph(DB_CYPHER, factoryType,
            relQuery, Orientation.UNDIRECTED, aggregation
        );
    }

    private Graph loadGraph(
        String dbQuery,
        FactoryType factoryType,
        String relationshipQuery,
        Orientation orientation,
        Aggregation aggregation
    ) {
        runQuery(dbQuery);

        GraphLoader graphLoader;

        if (factoryType == CYPHER) {

            graphLoader = new CypherLoaderBuilder()
                .databaseService(db)
                .nodeQuery(GraphProjectFromCypherConfig.ALL_NODES_QUERY)
                .relationshipQuery(relationshipQuery)
                .build();
        } else {
            graphLoader = new StoreLoaderBuilder()
                .databaseService(db)
                .globalOrientation(orientation)
                .globalAggregation(aggregation)
                .build();
        }
        return applyInTransaction(db, tx -> graphLoader.graph());
    }

}
