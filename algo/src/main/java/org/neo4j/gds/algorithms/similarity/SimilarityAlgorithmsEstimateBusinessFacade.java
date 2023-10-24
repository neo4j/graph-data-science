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
package org.neo4j.gds.algorithms.similarity;

import org.neo4j.gds.algorithms.estimation.AlgorithmEstimator;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnBaseConfig;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnFactory;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityBaseConfig;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityFactory;
import org.neo4j.gds.similarity.knn.KnnBaseConfig;
import org.neo4j.gds.similarity.knn.KnnFactory;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityBaseConfig;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityFactory;

import java.util.Optional;

public class SimilarityAlgorithmsEstimateBusinessFacade {

    private final AlgorithmEstimator algorithmEstimator;

    public SimilarityAlgorithmsEstimateBusinessFacade(
        AlgorithmEstimator algorithmEstimator
    ) {
        this.algorithmEstimator = algorithmEstimator;
    }

    public <C extends NodeSimilarityBaseConfig> MemoryEstimateResult nodeSimilarity(Object graphNameOrConfiguration, C configuration) {
        return algorithmEstimator.estimate(
            graphNameOrConfiguration,
            configuration,
            configuration.relationshipWeightProperty(),
            new NodeSimilarityFactory<>()
        );
    }

    public <C extends FilteredNodeSimilarityBaseConfig> MemoryEstimateResult filteredNodeSimilarity(
        Object graphNameOrConfiguration,
        C configuration
    ) {
        return algorithmEstimator.estimate(
            graphNameOrConfiguration,
            configuration,
            configuration.relationshipWeightProperty(),
            new FilteredNodeSimilarityFactory<>()
        );
    }

    public <C extends KnnBaseConfig> MemoryEstimateResult knn(
        Object graphNameOrConfiguration,
        C configuration
    ) {
        return algorithmEstimator.estimate(
            graphNameOrConfiguration,
            configuration,
            Optional.empty(),
            new KnnFactory<>()
        );
    }

    public <C extends FilteredKnnBaseConfig> MemoryEstimateResult filteredKnn(
        Object graphNameOrConfiguration,
        C configuration
    ) {
        return algorithmEstimator.estimate(
            graphNameOrConfiguration,
            configuration,
            Optional.empty(),
            new FilteredKnnFactory<>()
        );
    }
}
