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
package org.neo4j.gds.similarity;

import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.impl.similarity.ApproxNearestNeighborsAlgorithm;
import org.neo4j.gds.impl.similarity.ApproximateNearestNeighborsConfig;
import org.neo4j.gds.impl.similarity.ApproximateNearestNeighborsConfigImpl;
import org.neo4j.gds.impl.similarity.CosineAlgorithm;
import org.neo4j.gds.impl.similarity.CosineConfig;
import org.neo4j.gds.impl.similarity.EuclideanAlgorithm;
import org.neo4j.gds.impl.similarity.EuclideanConfig;
import org.neo4j.gds.impl.similarity.ImmutableCosineConfig;
import org.neo4j.gds.impl.similarity.ImmutableEuclideanConfig;
import org.neo4j.gds.impl.similarity.ImmutableJaccardConfig;
import org.neo4j.gds.impl.similarity.ImmutablePearsonConfig;
import org.neo4j.gds.impl.similarity.JaccardAlgorithm;
import org.neo4j.gds.impl.similarity.JaccardConfig;
import org.neo4j.gds.impl.similarity.PearsonAlgorithm;
import org.neo4j.gds.impl.similarity.PearsonConfig;
import org.neo4j.gds.impl.similarity.SimilarityAlgorithm;
import org.neo4j.gds.impl.similarity.SimilarityInput;

import java.util.Optional;

public abstract class ApproxNearestNeighborsProc extends AlphaSimilarityProc<ApproxNearestNeighborsAlgorithm<SimilarityInput>, ApproximateNearestNeighborsConfig> {

    protected static final String DESCRIPTION =
        "The Approximate Nearest Neighbors algorithm constructs a k-Nearest Neighbors " +
        "graph for a set of objects based on a provided similarity function.";

    @Override
    ApproxNearestNeighborsAlgorithm<SimilarityInput> newAlgo(
        ApproximateNearestNeighborsConfig config,
        AllocationTracker allocationTracker
    ) {
        SimilarityAlgorithm<?, SimilarityInput> similarity =
            (SimilarityAlgorithm<?, SimilarityInput>) similarityAlgorithm(config);
        return new ApproxNearestNeighborsAlgorithm<>(
            config,
            similarity,
            api,
            log,
            Pools.DEFAULT,
            allocationTracker
        );
    }

    @Override
    protected ApproximateNearestNeighborsConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return new ApproximateNearestNeighborsConfigImpl(
            graphName, maybeImplicitCreate, username, config
        );
    }

    @Override
    String taskName() {
        return "ApproximateNearestNeighbors";
    }

    SimilarityAlgorithm<?, ? extends SimilarityInput> similarityAlgorithm(ApproximateNearestNeighborsConfig config) {
        switch (config.algorithm()) {
            case jaccard:
                JaccardConfig jaccardConfig = ImmutableJaccardConfig.builder().from(config).build();
                return new JaccardAlgorithm(jaccardConfig, api);
            case cosine:
                CosineConfig cosineConfig = ImmutableCosineConfig.builder().from(config).build();
                return new CosineAlgorithm(cosineConfig, api);
            case pearson:
                PearsonConfig pearsonConfig = ImmutablePearsonConfig.builder().from(config).build();
                return new PearsonAlgorithm(pearsonConfig, api);
            case euclidean:
                EuclideanConfig euclideanConfig = ImmutableEuclideanConfig.builder().from(config).build();
                return new EuclideanAlgorithm(euclideanConfig, api);
            default:
                throw new IllegalArgumentException("Unexpected value: " + config.algorithm() + " (sad java 😞)");
        }
    }
}
