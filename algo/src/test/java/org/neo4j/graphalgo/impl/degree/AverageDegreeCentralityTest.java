/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.impl.degree;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.CypherGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class AverageDegreeCentralityTest {

    private static final String DB_CYPHER =
            "CREATE" +
            "  (_:Label0 {name: '_'})" +
            ", (a:Label1 {name: 'a'})" +
            ", (b:Label1 {name: 'b'})" +
            ", (c:Label1 {name: 'c'})" +
            ", (d:Label1 {name: 'd'})" +
            ", (e:Label1 {name: 'e'})" +
            ", (f:Label1 {name: 'f'})" +
            ", (g:Label1 {name: 'g'})" +
            ", (h:Label1 {name: 'h'})" +
            ", (i:Label1 {name: 'i'})" +
            ", (j:Label1 {name: 'j'})" +
            ", (k:Label2 {name: 'k'})" +
            ", (l:Label2 {name: 'l'})" +
            ", (m:Label2 {name: 'm'})" +
            ", (n:Label2 {name: 'n'})" +
            ", (o:Label2 {name: 'o'})" +
            ", (p:Label2 {name: 'p'})" +
            ", (q:Label2 {name: 'q'})" +
            ", (r:Label2 {name: 'r'})" +
            ", (s:Label2 {name: 's'})" +
            ", (t:Label2 {name: 't'})" +

            ", (b)-[:TYPE1 {weight: 2.0}]->(c)" +
            ", (c)-[:TYPE1 {weight: 2.0}]->(b)" +

            ", (d)-[:TYPE1 {weight: 2.0}]->(a)" +
            ", (d)-[:TYPE1 {weight: 2.0}]->(b)" +

            ", (e)-[:TYPE1 {weight: 2.0}]->(b)" +
            ", (e)-[:TYPE1 {weight: 2.0}]->(d)" +
            ", (e)-[:TYPE1 {weight: 2.0}]->(f)" +

            ", (f)-[:TYPE1 {weight: 2.0}]->(b)" +
            ", (f)-[:TYPE1 {weight: 2.0}]->(e)" +

            ", (a)-[:TYPE3 {weight: -2.0}]->(b)" +

            ", (b)-[:TYPE3 {weight: 2.0}]->(c)" +
            ", (c)-[:TYPE3 {weight: 2.0}]->(b)" +

            ", (d)-[:TYPE3 {weight: 2.0}]->(a)" +
            ", (d)-[:TYPE3 {weight: 2.0}]->(b)" +

            ", (e)-[:TYPE3 {weight: 2.0}]->(b)" +
            ", (e)-[:TYPE3 {weight: 2.0}]->(d)" +
            ", (e)-[:TYPE3 {weight: 2.0}]->(f)" +

            ", (f)-[:TYPE3 {weight: 2.0}]->(b)" +
            ", (f)-[:TYPE3 {weight: 2.0}]->(e)" +

            ", (g)-[:TYPE2]->(b)" +
            ", (g)-[:TYPE2]->(e)" +
            ", (h)-[:TYPE2]->(b)" +
            ", (h)-[:TYPE2]->(e)" +
            ", (i)-[:TYPE2]->(b)" +
            ", (i)-[:TYPE2]->(e)" +
            ", (j)-[:TYPE2]->(e)" +
            ", (k)-[:TYPE2]->(e)";

    private static GraphDatabaseAPI db;

    @BeforeAll
    static void setupGraphDb() {
        db = TestDatabaseCreator.createTestDatabase();
        db.execute(DB_CYPHER);
    }

    @AfterAll
    static void shutdownGraphDb() {
        if (db != null) db.shutdown();
    }

    @AllGraphTypesTest
    void averageOutgoingCentrality(Class<? extends GraphFactory> graphFactory) {
        final Label label = Label.label("Label1");

        final Graph graph;
        if (graphFactory.isAssignableFrom(CypherGraphFactory.class)) {
            graph = new GraphLoader(db)
                    .withLabel("MATCH (n:Label1) RETURN id(n) as id")
                    .withRelationshipType("MATCH (n:Label1)-[:TYPE1]->(m:Label1) RETURN id(n) as source,id(m) as target")
                    .load(graphFactory);

        } else {
            graph = new GraphLoader(db)
                    .withLabel(label)
                    .withRelationshipType("TYPE1")
                    .withDirection(Direction.OUTGOING)
                    .load(graphFactory);
        }

        AverageDegreeCentrality degreeCentrality = new AverageDegreeCentrality(graph, Pools.DEFAULT, 4);
        degreeCentrality.compute();

        assertEquals(0.9, degreeCentrality.average(), 0.01);
    }

    @AllGraphTypesTest
    void averageIncomingCentrality(Class<? extends GraphFactory> graphFactory) {
        final Label label = Label.label("Label1");

        final Graph graph;
        if (graphFactory.isAssignableFrom(CypherGraphFactory.class)) {
            graph = new GraphLoader(db)
                    .withLabel("MATCH (n:Label1) RETURN id(n) as id")
                    .withRelationshipType("MATCH (n:Label1)<-[:TYPE1]-(m:Label1) RETURN id(n) as source,id(m) as target")
                    .load(graphFactory);

        } else {
            graph = new GraphLoader(db)
                    .withLabel(label)
                    .withRelationshipType("TYPE1")
                    .withDirection(Direction.INCOMING)
                    .load(graphFactory);
        }

        AverageDegreeCentrality degreeCentrality = new AverageDegreeCentrality(graph, Pools.DEFAULT, 4);
        degreeCentrality.compute();

        assertEquals(0.9, degreeCentrality.average(), 0.01);
    }

    @AllGraphTypesTest
    void totalCentrality(Class<? extends GraphFactory> graphFactory) {
        final Label label = Label.label("Label1");

        final Graph graph;
        if (graphFactory.isAssignableFrom(CypherGraphFactory.class)) {
            graph = new GraphLoader(db)
                    .withLabel("MATCH (n:Label1) RETURN id(n) as id")
                    .withRelationshipType("MATCH (n:Label1)-[:TYPE1]-(m:Label1) RETURN id(n) as source,id(m) as target")
                    .withDeduplicationStrategy(DeduplicationStrategy.SKIP)
                    .load(graphFactory);

        } else {
            graph = new GraphLoader(db)
                    .withLabel(label)
                    .withRelationshipType("TYPE1")
                    .withDirection(Direction.OUTGOING)
                    .undirected()
                    .load(graphFactory);
        }

        AverageDegreeCentrality degreeCentrality = new AverageDegreeCentrality(graph, Pools.DEFAULT, 4);
        degreeCentrality.compute();

        assertEquals(1.4, degreeCentrality.average(), 0.01);
    }
}
