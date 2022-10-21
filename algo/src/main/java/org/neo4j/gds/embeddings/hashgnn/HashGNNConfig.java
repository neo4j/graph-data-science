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
package org.neo4j.gds.embeddings.hashgnn;

import org.immutables.value.Value;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.FeaturePropertiesConfig;
import org.neo4j.gds.config.RandomSeedConfig;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static org.neo4j.gds.ElementProjection.PROJECT_ALL;
import static org.neo4j.gds.RelationshipType.ALL_RELATIONSHIPS;

@Configuration
interface HashGNNConfig extends AlgoBaseConfig, FeaturePropertiesConfig, RandomSeedConfig {

    int iterations();

    @Configuration.IntegerRange(min = 1)
    int embeddingDensity();

    Optional<Integer> outputDimension();

    default double neighborInfluence() {
        return 1;
    }

    default boolean heterogeneous() {
        return false;
    }

    @Configuration.ConvertWith(method = "org.neo4j.gds.embeddings.hashgnn.HashGNNConfig#parseBinarizationConfig", inverse = "org.neo4j.gds.embeddings.hashgnn.HashGNNConfig#toMapBinarizationConfig")
    default Optional<FeatureBinarizationConfig> binarizeFeatures() {
        return Optional.empty();
    }

    @Configuration.GraphStoreValidationCheck
    @Value.Default
    default void starRelationshipTypeRequiresNotHeterogeneous(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        if (heterogeneous()) {
            if (relationshipTypes().contains(ALL_RELATIONSHIPS.name()) || relationshipTypes().contains(PROJECT_ALL))
                throw new IllegalArgumentException("Must not use `*` relationship projection with heterogeneous HashGNN");
        }
    }
    static Optional<FeatureBinarizationConfig> parseBinarizationConfig(Object o) {
        if (o instanceof Optional) {
            return (Optional<FeatureBinarizationConfig>) o;
        }
        return Optional.of(new FeatureBinarizationConfigImpl(CypherMapWrapper.create((Map<String, Object>) o)));
    }

    static Map<String, Object> toMapBinarizationConfig(FeatureBinarizationConfig config) {
        return Map.of("dimension", config.dimension(), "densityLevel", config.densityLevel());
    }
}
