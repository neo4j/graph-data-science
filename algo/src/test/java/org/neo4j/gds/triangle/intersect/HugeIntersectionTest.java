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
package org.neo4j.gds.triangle.intersect;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.api.RelationshipIntersect;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

import java.util.Arrays;
import java.util.PrimitiveIterator;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.compat.GraphDatabaseApiProxy.applyInTransaction;

final class HugeIntersectionTest extends BaseTest {

    private static final int DEGREE = 25;
    public static final RelationshipType TYPE = RelationshipType.withName("TYPE");
    private static RelationshipIntersect INTERSECT;
    private static long START1;
    private static long START2;
    private static long[] TARGETS;

    @BeforeEach
    void setup() {
        Random random = new Random(0L);
        long[] neoStarts = new long[2];
        long[] neoTargets = applyInTransaction(db, tx -> {
            Node start1 = tx.createNode();
            Node start2 = tx.createNode();
            Node start3 = tx.createNode();
            neoStarts[0] = start1.getId();
            neoStarts[1] = start2.getId();
            start1.createRelationshipTo(start2, TYPE);
            long[] targets = new long[DEGREE];
            int some = 0;
            for (int i = 0; i < DEGREE; i++) {
                Node target = tx.createNode();
                start1.createRelationshipTo(target, TYPE);
                start3.createRelationshipTo(target, TYPE);
                if (random.nextBoolean()) {
                    start2.createRelationshipTo(target, TYPE);
                    targets[some++] = target.getId();
                }
            }
            return Arrays.copyOf(targets, some);
        });

        var graph = new StoreLoaderBuilder()
            .databaseService(db)
            .globalOrientation(Orientation.UNDIRECTED)
            .build()
            .graph();

        INTERSECT = RelationshipIntersectFactoryLocator.lookup(graph)
            .orElseThrow(IllegalArgumentException::new)
            .load(graph, ImmutableRelationshipIntersectConfig.builder().build());
        START1 = graph.toMappedNodeId(neoStarts[0]);
        START2 = graph.toMappedNodeId(neoStarts[1]);
        TARGETS = Arrays.stream(neoTargets).map(graph::toMappedNodeId).toArray();
        Arrays.sort(TARGETS);
    }

    @Test
    void intersectWithTargets() {
        PrimitiveIterator.OfLong targets = Arrays.stream(TARGETS).iterator();
        INTERSECT.intersectAll(START1, (a, b, c) -> {
            assertEquals(START1, a);
            assertEquals(START2, b);
            assertEquals(targets.nextLong(), c);
        });
    }
}
