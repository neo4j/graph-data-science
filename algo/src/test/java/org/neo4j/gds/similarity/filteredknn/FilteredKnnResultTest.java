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
package org.neo4j.gds.similarity.filteredknn;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.similarity.SimilarityResult;

import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class FilteredKnnResultTest {

    @Test
    void should() {
        var rng = new SplittableRandom();

        var neighbors0 = new FilteredNeighborList(1);
        neighbors0.add(1, 0.3, rng, 0.0);

        var neighbors1 = new FilteredNeighborList(1);
        neighbors1.add(0, 0.7, rng, 0.0);

        var result = ImmutableFilteredKnnResult.of(
            HugeObjectArray.of(neighbors0, neighbors1),
            1,
            true,
            2,
            List.of()
        );

        var neighborLists = result.neighborList();

        // test1
        var other = Stream.concat(
            neighborLists.get(0).similarityStream(0),
            neighborLists.get(1).similarityStream(1)
        ).collect(Collectors.toList());
        var actual = result.streamSimilarityResult().collect(Collectors.toList());
        assertThat(other).isEqualTo(actual);
        assertThat(actual).isEqualTo(other);


        // test2
        var resultString1 = result.streamSimilarityResult()
            .map(this::formatRecord)
            .collect(Collectors.toList())
            .toString();

        var resultString2 = List.of(
            neighborLists.get(0).similarityStream(0).findFirst().map(this::formatRecord).get(),
            neighborLists.get(1).similarityStream(1).findFirst().map(this::formatRecord).get()
        ).toString();

        // those two should produce same result
        assertThat(resultString1).isEqualTo(resultString2);


        // test3
        var resultString3 = List.of(
            formatNeighborStream(result.neighborsOf(0)),
            formatNeighborStream(result.neighborsOf(1))
        ).toString();

        var resultString4 = List.of(
            formatNeighborStream(result.streamSimilarityResult().filter(sr -> sr.node1 == 0).mapToLong(sr -> sr.node2)),
            formatNeighborStream(result.streamSimilarityResult().filter(sr -> sr.node1 == 1).mapToLong(sr -> sr.node2))
        ).toString();

        var resultString5 = List.of(
            formatNeighborStream(neighborLists.get(0).similarityStream(0).mapToLong(sr -> sr.node2)),
            formatNeighborStream(neighborLists.get(1).similarityStream(1).mapToLong(sr -> sr.node2))
        ).toString();

        assertThat(resultString3).isEqualTo(resultString4).isEqualTo(resultString5);
    }

    private String formatNeighborStream(LongStream stream) {
        return Arrays.toString(stream.toArray());
    }

    private String formatRecord(SimilarityResult sr) {
        return String.format("%d,%d %f", sr.node1, sr.node2, sr.similarity);
    }

}
