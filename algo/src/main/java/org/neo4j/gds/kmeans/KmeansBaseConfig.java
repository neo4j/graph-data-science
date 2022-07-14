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
package org.neo4j.gds.kmeans;


import org.immutables.value.Value;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.IterationsConfig;
import org.neo4j.gds.config.RandomSeedConfig;
import org.neo4j.gds.utils.StringFormatting;

import java.util.Collection;
import java.util.List;

public interface KmeansBaseConfig extends AlgoBaseConfig, IterationsConfig, RandomSeedConfig {

    @Configuration.IntegerRange(min = 1)
    @Override
    @Value.Default
    default int maxIterations() {
        return 10;
    }

    @Value.Default
    @Configuration.IntegerRange(min = 1)
    default int k() {
        return 10;
    }

    @Value.Default
    @Configuration.DoubleRange(min = 0, max = 1)
    default double deltaThreshold() {
        return 0.05;
    }

    @Configuration.IntegerRange(min = 1)
    @Value.Default
    default int numberOfRestarts() {
        return 1;
    }

    @Value.Default
    default boolean computeSilhouette() {
        return false;
    }

    String nodeProperty();

    @Configuration.GraphStoreValidationCheck
    @Value.Default
    default void nodePropertyTypeValidation(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        var valueType = graphStore.nodeProperty(nodeProperty()).valueType();
        if (valueType == ValueType.DOUBLE_ARRAY || valueType == ValueType.FLOAT_ARRAY) {
            return;
        }
        throw new IllegalArgumentException(
            StringFormatting.formatWithLocale(
                "Unsupported node property value type [%s]. Value type required: [%s] or [%s].",
                valueType,
                ValueType.DOUBLE_ARRAY,
                ValueType.FLOAT_ARRAY
            )
        );
    }

    @Value.Default
    @Configuration.ConvertWith("org.neo4j.gds.kmeans.KmeansSampler.SamplerType#parse")
    @Configuration.ToMapValue("org.neo4j.gds.kmeans.KmeansSampler.SamplerType#toString")
    default KmeansSampler.SamplerType initialSampler() {
        return KmeansSampler.SamplerType.UNIFORM;
    }

    List<List<Double>> seedCentroids();
}
