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
package org.neo4j.gds.core.huge;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.beta.generator.PropertyProducer;
import org.neo4j.gds.beta.generator.RandomGraphGeneratorBuilder;
import org.neo4j.gds.beta.generator.RelationshipDistribution;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

class AllRelationshipsSpliteratorTest {

    private record Relationship(long target, double property) {
    }

    @Test
    void tryAdvance() {
        var graph = TestSupport.fromGdl("(a)-->(b), (b)-->(c), (c)-->(d)");

        var spliterator = new AllRelationshipsSpliterator(graph, 1.0);

        assertThat(spliterator.tryAdvance(relationshipCursor -> {})).isTrue();
        assertThat(spliterator.tryAdvance(relationshipCursor -> {})).isTrue();
        assertThat(spliterator.tryAdvance(relationshipCursor -> {})).isTrue();
        assertThat(spliterator.tryAdvance(relationshipCursor -> {})).isFalse();
    }

    @Property(tries = 50)
    void forEach(
        @ForAll @LongRange(min = 10L, max = 10_000L) long nodeCount,
        @ForAll @IntRange(min = 10, max = 100) int averageDegree,
        @ForAll RelationshipDistribution relationshipDistribution,
        @ForAll long seed,
        @ForAll boolean parallel
    ) {
        var graph = generateGraph(nodeCount, averageDegree, relationshipDistribution, seed);

        var fallback = 42.0;
        var expected = expectedAdjacencyList(graph, fallback);

        var actual = new ConcurrentHashMap<Long, List<Relationship>>();
        var allRelationshipsIterator = new AllRelationshipsSpliterator(graph, fallback);

        StreamSupport.stream(allRelationshipsIterator, parallel).forEach(relationshipCursor -> actual
            .computeIfAbsent(relationshipCursor.sourceId(), __ -> new ArrayList<>())
            .add(new Relationship(
                relationshipCursor.targetId(),
                relationshipCursor.property()
            )));

        assertThat(actual).isEqualTo(expected);
    }

    @Property(tries = 50)
    void iterator(
        @ForAll @LongRange(min = 10L, max = 10_000L) long nodeCount,
        @ForAll @IntRange(min = 10, max = 100) int averageDegree,
        @ForAll RelationshipDistribution relationshipDistribution,
        @ForAll long seed
    ) {
        var graph = generateGraph(nodeCount, averageDegree, relationshipDistribution, seed);
        var fallback = 42.0;
        var expected = expectedAdjacencyList(graph, fallback);

        var actual = new ConcurrentHashMap<Long, List<Relationship>>();
        var allRelationshipsIterator = new AllRelationshipsSpliterator(graph, fallback);

        var iterator = StreamSupport.stream(allRelationshipsIterator, true).iterator();

        while (iterator.hasNext()) {
            var relationshipCursor = iterator.next();
            actual.computeIfAbsent(relationshipCursor.sourceId(), __ -> new ArrayList<>())
                .add(new Relationship(
                    relationshipCursor.targetId(),
                    relationshipCursor.property()
                ));
        }

        assertThat(actual).isEqualTo(expected);
    }

    private static @NotNull HugeGraph generateGraph(
        long nodeCount,
        int averageDegree,
        RelationshipDistribution relationshipDistribution,
        long seed
    ) {
        return new RandomGraphGeneratorBuilder()
            .nodeCount(nodeCount)
            .averageDegree(averageDegree)
            .seed(seed)
            .relationshipType(RelationshipType.ALL_RELATIONSHIPS)
            .relationshipDistribution(relationshipDistribution)
            .relationshipPropertyProducer(PropertyProducer.randomDouble("baz", 0, 100))
            .build()
            .generate();
    }

    private static @NotNull ConcurrentHashMap<Long, List<Relationship>> expectedAdjacencyList(
        Graph graph,
        double fallback
    ) {
        var expected = new ConcurrentHashMap<Long, List<Relationship>>();
        graph.forEachNode(node -> {
            graph.forEachRelationship(node, fallback, (source, target, property) -> {
                expected.computeIfAbsent(source, __ -> new ArrayList<>()).add(new Relationship(
                    target,
                    property
                ));
                return true;
            });
            return true;
        });
        return expected;
    }
}
