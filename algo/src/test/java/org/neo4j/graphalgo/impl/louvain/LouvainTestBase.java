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
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.CypherGraphFactory;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.impl.louvain.legacy.Louvain;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

abstract class LouvainTestBase {

    static final Louvain.Config DEFAULT_CONFIG = new Louvain.Config(10, 10, Optional.empty());
    static final Louvain.Config DEFAULT_CONFIG_WITH_DENDROGRAM = new Louvain.Config(10, 10, Optional.empty(), true);

    GraphDatabaseAPI db;

    Map<String, Integer> nameMap = new HashMap<>();

    @BeforeEach
    void setupGraphDb() {
        db = TestDatabaseCreator.createTestDatabase();
    }

    @AfterEach
    void shutdownGraphDb() {
        if (db != null) db.shutdown();
    }

    abstract void setupGraphDb(Graph graph);

    Graph loadGraph(Class<? extends GraphFactory> graphImpl, String cypher, String... nodeProperties) {
        db.execute(cypher);
        GraphLoader loader = new GraphLoader(db)
            .withRelationshipProperties(PropertyMapping.of("weight", 1.0))
            .withOptionalNodeProperties(
                Arrays.stream(nodeProperties)
                    .map(p -> PropertyMapping.of(p, -1))
                    .toArray(PropertyMapping[]::new)
            )
            .withDirection(Direction.BOTH);
        if (graphImpl == CypherGraphFactory.class) {
            loader
                .withNodeStatement("MATCH (u:Node) RETURN id(u) as id, u.seed1 as seed1, u.seed2 as seed2")
                .withRelationshipStatement("MATCH (u1:Node)-[rel]-(u2:Node) \n" +
                                           "RETURN id(u1) AS source, id(u2) AS target, rel.weight as weight")
                .withDeduplicationStrategy(DeduplicationStrategy.SKIP);
        } else {
            loader
                .withAnyRelationshipType()
                .withLabel("Node");
        }
        try (Transaction tx = db.beginTx()) {
            Graph graph = loader.load(graphImpl);
            setupGraphDb(graph);
            return graph;
        }
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
                    current,
                    communityIds[id],
                    "Node " + name + " belongs to wrong community " + communityIds[id]
                );
            }
        }
    }

    void assertDisjoint(String[] nodeNames, HugeLongArray values) {
        final long[] communityIds = values.toArray();
        final LongSet set = new LongHashSet();
        for (String name : nodeNames) {
            final long communityId = communityIds[nameMap.get(name)];
            assertTrue(set.add(communityId), "Node " + name + " belongs to wrong community " + communityId);
        }
    }
}
