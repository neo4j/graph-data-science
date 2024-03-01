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
import org.neo4j.gds.similarity.knn.KnnResult;

import java.util.stream.Stream;

public class FilteredKnnResult {

    private final boolean isTargetFiltered;
    private final TargetNodeFiltering targetNodeFiltering;
    private final NodeFilter sourceNodeFilter;
    private final KnnResult knnResult;


    public FilteredKnnResult(
        TargetNodeFiltering targetNodeFiltering,
        KnnResult knnResult,
        NodeFilter sourceNodeFilter
    ) {
        this.isTargetFiltered = targetNodeFiltering.isFiltered();
        this.targetNodeFiltering = targetNodeFiltering;
        this.knnResult = knnResult;
        this.sourceNodeFilter = sourceNodeFilter;
    }

    public Stream<SimilarityResult> similarityResultStream() {


        if (isTargetFiltered) {
            return targetNodeFiltering.asSimilarityResultStream(sourceNodeFilter);
        }

        return knnResult.streamSimilarityResult(sourceNodeFilter);
    }

    public long numberOfSimilarityPairs() {

        if (isTargetFiltered) {
            return targetNodeFiltering.numberOfSimilarityPairs(sourceNodeFilter);
        }

        return knnResult.totalSimilarityPairs(sourceNodeFilter);

    }

    public int ranIterations() {
        return knnResult.ranIterations();

    }

    public boolean didConverge() {
        return knnResult.didConverge();

    }

    public long nodePairsConsidered() {
        return knnResult.nodePairsConsidered();
    }

    public long nodesCompared() {
        return knnResult.nodesCompared();
    }

}
