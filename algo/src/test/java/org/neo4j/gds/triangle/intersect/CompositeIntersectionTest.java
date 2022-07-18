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
import org.neo4j.gds.core.huge.UnionGraph;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

import java.util.Arrays;
import java.util.PrimitiveIterator;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.gds.compat.GraphDatabaseApiProxy.applyInTransaction;

final class CompositeIntersectionTest extends BaseTest {

    private static final int DEGREE = 25;
    private static final RelationshipType TYPE_1 = RelationshipType.withName("TYPE1");
    private static final RelationshipType TYPE_2 = RelationshipType.withName("TYPE2");
    private static UnionGraph GRAPH;
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
            start1.createRelationshipTo(start2, TYPE_1);
            long[] targets = new long[DEGREE];
            int some = 0;
            for (int i = 0; i < DEGREE; i++) {
                RelationshipType type = i % 2 == 0 ? TYPE_1 : TYPE_2;
                Node target = tx.createNode();
                start1.createRelationshipTo(target, type);
                start3.createRelationshipTo(target, type);
                if (random.nextBoolean()) {
                    start2.createRelationshipTo(target, type);
                    targets[some++] = target.getId();
                }
            }
            return Arrays.copyOf(targets, some);
        });

        GRAPH = ((UnionGraph) new StoreLoaderBuilder()
            .databaseService(db)
            .addRelationshipType(TYPE_1.name())
            .addRelationshipType(TYPE_2.name())
            .globalOrientation(Orientation.UNDIRECTED)
            .build()
            .graph());

        START1 = GRAPH.toMappedNodeId(neoStarts[0]);
        START2 = GRAPH.toMappedNodeId(neoStarts[1]);
        TARGETS = Arrays.stream(neoTargets).map(GRAPH::toMappedNodeId).toArray();
        Arrays.sort(TARGETS);
    }

    @Test
    void intersectWithTargets() {
        var intersect = new UnionGraphIntersect.UnionGraphIntersectFactory().load(
            GRAPH,
            ImmutableRelationshipIntersectConfig.builder().build()
        );

        PrimitiveIterator.OfLong targets = Arrays.stream(TARGETS).iterator();
        intersect.intersectAll(START1, (a, b, c) -> {
            assertEquals(START1, a);
            assertEquals(START2, b);
            assertEquals(targets.nextLong(), c);
        });
    }

    @Test
    void intersectWithTargetsWithMaxDegree() {
        var intersect = new UnionGraphIntersect.UnionGraphIntersectFactory().load(
            GRAPH,
            ImmutableRelationshipIntersectConfig.builder().maxDegree(0).build()
        );

        intersect.intersectAll(
            START1,
            (a, b, c) -> fail("This code should have been protected by the max degree filter")
        );
    }
}
