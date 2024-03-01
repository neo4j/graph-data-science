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

import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.similarity.filtering.NodeFilter;
import org.neo4j.gds.similarity.knn.NeighbourConsumers;
import org.neo4j.gds.similarity.knn.SimilarityFunction;

import java.util.Optional;
import java.util.function.LongPredicate;
import java.util.stream.Stream;

public interface TargetNodeFiltering extends NeighbourConsumers {

    boolean isTargetNodeFiltered();

    Stream<SimilarityResult> asSimilarityResultStream(LongPredicate sourceNodePredicate);

    long numberOfSimilarityPairs(LongPredicate sourceNodePredicate);

    static TargetNodeFiltering create(
        NodeFilter sourceNodeFilter,
        long nodeCount,
        int k,
        LongPredicate targetNodePredicate,
        Optional<SimilarityFunction> optionalSimilarityFunction,
        double similarityCutoff,
        int concurrency
    ) {

        if (targetNodePredicate == NodeFilter.ALLOW_EVERYTHING) {
            return EmptyTargetNodeFiltering.EMPTY_TARGET_FILTERING;
        } else {
            return ProvidedTargetNodeFiltering.create(
                sourceNodeFilter,
                nodeCount,
                k,
                targetNodePredicate,
                optionalSimilarityFunction,
                similarityCutoff,
                concurrency
            );
        }
    }
}
