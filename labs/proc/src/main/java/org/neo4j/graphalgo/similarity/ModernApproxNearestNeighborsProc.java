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
import org.neo4j.graphalgo.impl.similarity.SimilarityInput;
import org.neo4j.graphalgo.impl.similarity.modern.ImmutableModernCosineConfig;
import org.neo4j.graphalgo.impl.similarity.modern.ImmutableModernEuclideanConfig;
import org.neo4j.graphalgo.impl.similarity.modern.ImmutableModernPearsonConfig;
import org.neo4j.graphalgo.impl.similarity.modern.ModernApproxNearestNeighborsAlgorithm;
import org.neo4j.graphalgo.impl.similarity.modern.ModernApproximateNearestNeighborsConfig;
import org.neo4j.graphalgo.impl.similarity.modern.ModernApproximateNearestNeighborsConfigImpl;
import org.neo4j.graphalgo.impl.similarity.modern.ModernCosineAlgorithm;
import org.neo4j.graphalgo.impl.similarity.modern.ModernCosineConfig;
import org.neo4j.graphalgo.impl.similarity.modern.ModernEuclideanAlgorithm;
import org.neo4j.graphalgo.impl.similarity.modern.ModernEuclideanConfig;
import org.neo4j.graphalgo.impl.similarity.modern.ModernPearsonAlgorithm;
import org.neo4j.graphalgo.impl.similarity.modern.ModernPearsonConfig;
import org.neo4j.graphalgo.impl.similarity.modern.ModernSimilarityAlgorithm;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class ModernApproxNearestNeighborsProc extends ModernSimilarityProc<ModernApproxNearestNeighborsAlgorithm<SimilarityInput>, ModernApproximateNearestNeighborsConfig> {

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
    protected ModernApproximateNearestNeighborsConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return new ModernApproximateNearestNeighborsConfigImpl(
            graphName, maybeImplicitCreate, username, config
        );
    }

    @Override
    ModernApproxNearestNeighborsAlgorithm<SimilarityInput> newAlgo(ModernApproximateNearestNeighborsConfig config) {
        ModernSimilarityAlgorithm<?, SimilarityInput> similarity =
            (ModernSimilarityAlgorithm<?, SimilarityInput>) similarityAlgorithm(config);
        return new ModernApproxNearestNeighborsAlgorithm<>(config, similarity, api, log);
    }

    ModernSimilarityAlgorithm<?, ? extends SimilarityInput> similarityAlgorithm(ModernApproximateNearestNeighborsConfig config) {
        switch (config.algorithm()) {
            case jaccard:
                throw new IllegalArgumentException("TODO");
            case cosine:
                ModernCosineConfig cosineConfig = ImmutableModernCosineConfig.builder().from(config).build();
                return new ModernCosineAlgorithm(cosineConfig, api);
            case pearson:
                ModernPearsonConfig pearsonConfig = ImmutableModernPearsonConfig.builder().from(config).build();
                return new ModernPearsonAlgorithm(pearsonConfig, api);
            case euclidean:
                ModernEuclideanConfig eucideanConfig = ImmutableModernEuclideanConfig.builder().from(config).build();
                return new ModernEuclideanAlgorithm(eucideanConfig, api);
            default:
                throw new IllegalArgumentException("Unexpected value: " + config.algorithm() + " (sad java 😞)");
        }
    }
}
