/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.similarity;

import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.impl.results.SimilarityResult;
import org.neo4j.graphalgo.impl.results.SimilaritySummaryResult;
import org.neo4j.graphalgo.impl.similarity.ApproximateNearestNeighborsConfigImpl;
import org.neo4j.graphalgo.impl.similarity.ImmutableCosineConfig;
import org.neo4j.graphalgo.impl.similarity.ImmutableEuclideanConfig;
import org.neo4j.graphalgo.impl.similarity.ImmutableJaccardConfig;
import org.neo4j.graphalgo.impl.similarity.ImmutablePearsonConfig;
import org.neo4j.graphalgo.impl.similarity.SimilarityInput;
import org.neo4j.graphalgo.impl.similarity.ApproximateNearestNeighborsConfig;
import org.neo4j.graphalgo.impl.similarity.CosineConfig;
import org.neo4j.graphalgo.impl.similarity.EuclideanConfig;
import org.neo4j.graphalgo.impl.similarity.ApproxNearestNeighborsAlgorithm;
import org.neo4j.graphalgo.impl.similarity.CosineAlgorithm;
import org.neo4j.graphalgo.impl.similarity.EuclideanAlgorithm;
import org.neo4j.graphalgo.impl.similarity.JaccardAlgorithm;
import org.neo4j.graphalgo.impl.similarity.JaccardConfig;
import org.neo4j.graphalgo.impl.similarity.PearsonAlgorithm;
import org.neo4j.graphalgo.impl.similarity.PearsonConfig;
import org.neo4j.graphalgo.impl.similarity.SimilarityAlgorithm;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class ApproxNearestNeighborsProc extends SimilarityProc<ApproxNearestNeighborsAlgorithm<SimilarityInput>, ApproximateNearestNeighborsConfig> {

    @Procedure(name = "gds.alpha.ml.ann.stream", mode = READ)
    public Stream<SimilarityResult> annStream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stream(graphNameOrConfig, configuration);
    }

    @Procedure(name = "gds.alpha.ml.ann.write", mode = WRITE)
    public Stream<SimilaritySummaryResult> annWrite(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return write(graphNameOrConfig, configuration);
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
    ApproxNearestNeighborsAlgorithm<SimilarityInput> newAlgo(ApproximateNearestNeighborsConfig config) {
        SimilarityAlgorithm<?, SimilarityInput> similarity =
            (SimilarityAlgorithm<?, SimilarityInput>) similarityAlgorithm(config);
        return new ApproxNearestNeighborsAlgorithm<>(
            config,
            similarity,
            api,
            log
        );
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
