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

package org.neo4j.graphalgo.impl.jaccard;

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TopKMapTest {

    @Test
    void shouldLimitTopK() {
        List<SimilarityResult> expected = new ArrayList<>();
        expected.add(new SimilarityResult(0, 6, 30.0));
        expected.add(new SimilarityResult(1, 0, 5.0));
        expected.add(new SimilarityResult(2, 0, 10.0));
        expected.add(new SimilarityResult(3, 0, 15.0));
        expected.add(new SimilarityResult(4, 0, 20.0));
        expected.add(new SimilarityResult(5, 0, 25.0));
        expected.add(new SimilarityResult(6, 0, 30.0));

        List<SimilarityResult> input = new ArrayList<>();
        input.add(new SimilarityResult(0, 1, 5.0));
        input.add(new SimilarityResult(0, 2, 10.0));
        input.add(new SimilarityResult(0, 3, 15.0));
        input.add(new SimilarityResult(0, 4, 20.0));
        input.add(new SimilarityResult(0, 5, 25.0));
        input.add(new SimilarityResult(0, 6, 30.0));
        input.addAll(input.stream().map(SimilarityResult::reverse).collect(Collectors.toList()));

        BitSet bitSet = new BitSet();
        bitSet.set(0, input.size());

        TopKMap topKMap = new TopKMap(input.size(), bitSet, 1, SimilarityResult.DESCENDING, AllocationTracker.EMPTY);

        input.forEach(sim -> topKMap.put(sim.node1, sim.node2, sim.similarity));

        List<SimilarityResult> actual = topKMap.stream().collect(Collectors.toList());

        assertEquals(expected, actual);
    }

    @Test
    void shouldLimitTopK3() {
        List<SimilarityResult> expected = new ArrayList<>();
        expected.add(new SimilarityResult(0, 6, 30.0));
        expected.add(new SimilarityResult(0, 5, 25.0));
        expected.add(new SimilarityResult(0, 4, 20.0));

        List<SimilarityResult> input = new ArrayList<>();
        input.add(new SimilarityResult(0, 1, 5.0));
        input.add(new SimilarityResult(0, 6, 30.0));
        input.add(new SimilarityResult(0, 2, 10.0));
        input.add(new SimilarityResult(0, 5, 25.0));
        input.add(new SimilarityResult(0, 3, 15.0));
        input.add(new SimilarityResult(0, 4, 20.0));

        BitSet bitSet = new BitSet();
        bitSet.set(0, input.size());

        TopKMap topKMap = new TopKMap(input.size(), bitSet, 3, SimilarityResult.DESCENDING, AllocationTracker.EMPTY);

        input.forEach(sim -> topKMap.put(sim.node1, sim.node2, sim.similarity));

        List<SimilarityResult> actual = topKMap.stream().collect(Collectors.toList());

        assertEquals(expected, actual);
    }

    @Test
    void shouldLimitTopK3Ascending() {
        List<SimilarityResult> expected = new ArrayList<>();
        expected.add(new SimilarityResult(0, 1, 5.0));
        expected.add(new SimilarityResult(0, 2, 10.0));
        expected.add(new SimilarityResult(0, 3, 15.0));

        List<SimilarityResult> input = new ArrayList<>();
        input.add(new SimilarityResult(0, 1, 5.0));
        input.add(new SimilarityResult(0, 6, 30.0));
        input.add(new SimilarityResult(0, 2, 10.0));
        input.add(new SimilarityResult(0, 5, 25.0));
        input.add(new SimilarityResult(0, 3, 15.0));
        input.add(new SimilarityResult(0, 4, 20.0));

        BitSet bitSet = new BitSet();
        bitSet.set(0, input.size());

        TopKMap topKMap = new TopKMap(input.size(), bitSet, 3, SimilarityResult.ASCENDING, AllocationTracker.EMPTY);

        input.forEach(sim -> topKMap.put(sim.node1, sim.node2, sim.similarity));

        List<SimilarityResult> actual = topKMap.stream().collect(Collectors.toList());

        assertEquals(expected, actual);
    }

    static Stream<Comparator<SimilarityResult>> comparators() {
        return Stream.of(SimilarityResult.ASCENDING, SimilarityResult.DESCENDING);
    }

    @ParameterizedTest
    @MethodSource("comparators")
    void shouldKeepFirstWhenValuesAreIdentical(Comparator<SimilarityResult> comparator) {
        List<SimilarityResult> expected = new ArrayList<>();
        expected.add(new SimilarityResult(0, 4, 20.0));
        expected.add(new SimilarityResult(0, 3, 20.0));
        expected.add(new SimilarityResult(0, 6, 20.0));

        List<SimilarityResult> input = new ArrayList<>();
        input.add(new SimilarityResult(0, 6, 20.0));
        input.add(new SimilarityResult(0, 3, 20.0));
        input.add(new SimilarityResult(0, 4, 20.0));
        input.add(new SimilarityResult(0, 2, 20.0));
        input.add(new SimilarityResult(0, 1, 20.0));
        input.add(new SimilarityResult(0, 5, 20.0));

        BitSet bitSet = new BitSet();
        bitSet.set(0, input.size());

        TopKMap topKMap = new TopKMap(input.size(), bitSet, 3, comparator, AllocationTracker.EMPTY);

        input.forEach(sim -> topKMap.put(sim.node1, sim.node2, sim.similarity));

        List<SimilarityResult> actual = topKMap.stream().collect(Collectors.toList());

        assertEquals(expected, actual);
    }
}
