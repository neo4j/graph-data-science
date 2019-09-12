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
package org.neo4j.graphalgo.impl.louvain;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.loader.CypherGraphFactory;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

abstract class LouvainTestBase {

    static final Louvain.Config DEFAULT_CONFIG = new Louvain.Config(10, 10, false);

    GraphDatabaseAPI DB;

    Map<String, Integer> nameMap = new HashMap<>();

    @BeforeEach
    void setupGraphDb() {
        DB = TestDatabaseCreator.createTestDatabase();
    }

    @AfterEach
    void shutdownGraphDb() {
        if (DB != null) DB.shutdown();
    }

    abstract void setupGraphDb(Graph graph);

    Graph loadGraph(Class<? extends GraphFactory> graphImpl, String cypher) {
        DB.execute(cypher);
        GraphLoader loader = new GraphLoader(DB)
                .withoutNodeProperties()
                .withOptionalRelationshipWeightsFromProperty("weight", 1.0)
                .undirected();
        if (graphImpl == CypherGraphFactory.class) {
            loader
                    .withNodeStatement("MATCH (u) RETURN id(u) as id")
                    .withRelationshipStatement("MATCH (u1)-[rel]-(u2) \n" +
                                               "RETURN id(u1) AS source, id(u2) AS target, rel.weight as weight");
        } else {
            loader
                    .withAnyRelationshipType()
                    .withAnyLabel();
        }
        Graph graph = loader.load(graphImpl);
        setupGraphDb(graph);
        return graph;
    }

    void assertUnion(String[] nodeNames, HugeLongArray values) {
        final long[] communityIds = values.toArray();
        long current = -1L;
        for (String name : nodeNames) {
            if (!nameMap.containsKey(name)) {
                throw new IllegalArgumentException("unknown node name: " + name);
            }
            final int id = nameMap.get(name);
            if (current == -1L) {
                current = communityIds[id];
            } else {
                assertEquals(
                        "Node " + name + " belongs to wrong community " + communityIds[id],
                        current,
                        communityIds[id]);
            }
        }
    }

    void assertDisjoint(String[] nodeNames, HugeLongArray values) {
        final long[] communityIds = values.toArray();
        final LongSet set = new LongHashSet();
        for (String name : nodeNames) {
            final long communityId = communityIds[nameMap.get(name)];
            assertTrue("Node " + name + " belongs to wrong community " + communityId, set.add(communityId));
        }
    }
}
