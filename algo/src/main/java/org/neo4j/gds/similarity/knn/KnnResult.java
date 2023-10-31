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
package org.neo4j.gds.similarity.knn;

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.collections.cursor.HugeCursor;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.similarity.SimilarityResult;

import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

@ValueClass
public abstract class KnnResult {
    abstract HugeObjectArray<NeighborList> neighborList();

    public abstract int ranIterations();

    public abstract boolean didConverge();

    public abstract long nodePairsConsidered();

    public abstract long nodesCompared();


    public LongStream neighborsOf(long nodeId) {
        return neighborList().get(nodeId).elements().map(NeighborList::clearCheckedFlag);
    }

    // http://www.flatmapthatshit.com/
    public Stream<SimilarityResult> streamSimilarityResult() {
        var neighborList = neighborList();
        return Stream.iterate(
                neighborList.initCursor(neighborList.newCursor()),
                HugeCursor::next,
                UnaryOperator.identity()
            )
            .flatMap(cursor -> IntStream.range(cursor.offset, cursor.limit)
                .mapToObj(index -> cursor.array[index].similarityStream(index + cursor.base))
                .flatMap(Function.identity())
            );
    }

    public long totalSimilarityPairs() {
        var neighborList = neighborList();
        return Stream.iterate(
                neighborList.initCursor(neighborList.newCursor()),
                HugeCursor::next,
                UnaryOperator.identity()
            )
            .flatMapToLong(cursor -> IntStream.range(cursor.offset, cursor.limit)
                .mapToLong(index -> cursor.array[index].size()))
            .sum();
    }

    public long size() {
        return neighborList().size();
    }
}
