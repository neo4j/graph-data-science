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

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.utils.paged.HugeCursor;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.similarity.SimilarityResult;

import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

@ValueClass
public abstract class FilteredKnnResult {
    abstract HugeObjectArray<FilteredNeighborList> neighborList();

    public abstract int ranIterations();

    public abstract boolean didConverge();

    public abstract long nodePairsConsidered();

    public abstract List<Long> sourceNodes();

    public LongStream neighborsOf(long nodeId) {
        return neighborList().get(nodeId).elements().map(FilteredNeighborList::clearCheckedFlag);
    }

    // http://www.flatmapthatshit.com/
    public Stream<SimilarityResult> streamSimilarityResult() {
        // [[],[],[]]
        var neighborList = neighborList();
        return Stream
            .iterate(neighborList.initCursor(neighborList.newCursor()), HugeCursor::next, UnaryOperator.identity())
            .flatMap(cursor -> IntStream.range(cursor.offset, cursor.limit)
//                .filter(index -> sourceNodes().contains(index + cursor.base))
                .mapToObj(index -> cursor.array[index].similarityStream(index + cursor.base))
                .flatMap(Function.identity())
            );
    }

    public long totalSimilarityPairs() {
        var neighborList = neighborList();
        return Stream
            .iterate(neighborList.initCursor(neighborList.newCursor()), HugeCursor::next, UnaryOperator.identity())
            .flatMapToLong(cursor -> IntStream.range(cursor.offset, cursor.limit)
//                .filter(index -> sourceNodes().contains(index + cursor.base))
                .mapToLong(index -> cursor.array[index].size()))
            .sum();
    }

    public long size() {
        return neighborList().size();
    }

    @NotNull
    static FilteredKnnResult empty() {
        return new FilteredKnnResult() {

            @Override
            HugeObjectArray<FilteredNeighborList> neighborList() {
                return HugeObjectArray.of();
            }

            @Override
            public int ranIterations() {
                return 0;
            }

            @Override
            public boolean didConverge() {
                return false;
            }

            @Override
            public long nodePairsConsidered() {
                return 0;
            }

            @Override
            public List<Long> sourceNodes() {
                return List.of();
            }

            @Override
            public LongStream neighborsOf(long nodeId) {
                return LongStream.empty();
            }
        };
    }
}
