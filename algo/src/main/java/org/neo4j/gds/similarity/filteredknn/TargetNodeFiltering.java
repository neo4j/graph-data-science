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

import org.neo4j.gds.core.utils.paged.HugeCursor;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.similarity.knn.NeighbourConsumer;
import org.neo4j.gds.similarity.knn.NeighbourConsumers;

import java.util.function.Function;
import java.util.function.LongPredicate;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class TargetNodeFiltering implements NeighbourConsumers {
    private final HugeObjectArray<TargetNodeFilter> neighbourConsumers;

    static TargetNodeFiltering create(long nodeCount, int k, LongPredicate targetNodePredicate) {
        HugeObjectArray<TargetNodeFilter> neighbourConsumers = HugeObjectArray.newArray(
            TargetNodeFilter.class,
            nodeCount
        );

        for (int i = 0; i < nodeCount; i++) {
            neighbourConsumers.set(i, new TargetNodeFilter(targetNodePredicate, k));
        }

        return new TargetNodeFiltering(neighbourConsumers);
    }

    private TargetNodeFiltering(HugeObjectArray<TargetNodeFilter> neighbourConsumers) {
        this.neighbourConsumers = neighbourConsumers;
    }

    @Override
    public NeighbourConsumer get(long nodeId) {
        return neighbourConsumers.get(nodeId);
    }

    Stream<SimilarityResult> asSimilarityResultStream(LongPredicate sourceNodePredicate) {
        return Stream
            .iterate(
                neighbourConsumers.initCursor(neighbourConsumers.newCursor()),
                HugeCursor::next,
                UnaryOperator.identity()
            )
            .flatMap(cursor -> IntStream.range(cursor.offset, cursor.limit)
                .filter(index -> sourceNodePredicate.test(index + cursor.base))
                .mapToObj(index -> cursor.array[index].asSimilarityStream(index + cursor.base))
                .flatMap(Function.identity())
            );
    }
}
