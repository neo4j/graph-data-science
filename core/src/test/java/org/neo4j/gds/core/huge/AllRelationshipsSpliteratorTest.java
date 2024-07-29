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
import org.junit.jupiter.api.Test;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.TestSupport;
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

        var iterator = new AllRelationshipsSpliterator(graph, 1.0);

        assertThat(iterator.tryAdvance(relationshipCursor -> {})).isTrue();
        assertThat(iterator.tryAdvance(relationshipCursor -> {})).isTrue();
        assertThat(iterator.tryAdvance(relationshipCursor -> {})).isTrue();
        assertThat(iterator.tryAdvance(relationshipCursor -> {})).isFalse();
    }

    @Property(tries = 50)
    void forEach(
        @ForAll @LongRange(min = 10L, max = 10_000L) long nodeCount,
        @ForAll @IntRange(min = 10, max = 100) int averageDegree,
        @ForAll RelationshipDistribution relationshipDistribution,
        @ForAll long seed,
        @ForAll boolean parallel
    ) {
        double fallback = 42.0;
        var graph = new RandomGraphGeneratorBuilder()
            .nodeCount(nodeCount)
            .averageDegree(averageDegree)
            .seed(seed)
            .relationshipType(RelationshipType.ALL_RELATIONSHIPS)
            .relationshipDistribution(relationshipDistribution)
            .relationshipPropertyProducer(PropertyProducer.randomDouble("baz", 0, 100))
            .build()
            .generate();

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
}