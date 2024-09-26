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
package org.neo4j.gds.similarity.nodesim;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.ConfigNodesValidations;
import org.neo4j.gds.config.RelationshipWeightConfig;

import java.util.Collection;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@Configuration
public interface NodeSimilarityBaseConfig extends AlgoBaseConfig, RelationshipWeightConfig {

    String TOP_K_KEY = "topK";
    int TOP_K_DEFAULT = 10;

    String TOP_N_KEY = "topN";
    int TOP_N_DEFAULT = 0;

    String BOTTOM_K_KEY = "bottomK";
    int BOTTOM_K_DEFAULT = TOP_K_DEFAULT;

    String BOTTOM_N_KEY = "bottomN";
    int BOTTOM_N_DEFAULT = TOP_N_DEFAULT;

    String COMPONENT_PROPERTY_KEY = "componentProperty";

    @Configuration.DoubleRange(min = 0, max = 1)
    default double similarityCutoff() {
        return 1E-42;
    }

    @Configuration.ConvertWith(method = "org.neo4j.gds.similarity.nodesim.MetricSimilarityComputer#parse")
    @Configuration.ToMapValue("org.neo4j.gds.similarity.nodesim.MetricSimilarityComputer#render")
    default MetricSimilarityComputer.MetricSimilarityComputerBuilder similarityMetric() {
        return new JaccardSimilarityComputer.Builder();
    }

    @Configuration.IntegerRange(min = 1)
    default int degreeCutoff() {
        return 1;
    }

    @Configuration.IntegerRange(min = 1)
    default int upperDegreeCutoff() {
        return Integer.MAX_VALUE;
    }

    @Configuration.Key(TOP_K_KEY)
    @Configuration.IntegerRange(min = 1)
    default int topK() {
        return TOP_K_DEFAULT;
    }

    @Configuration.Key(TOP_N_KEY)
    @Configuration.IntegerRange(min = 0)
    default int topN() {
        return TOP_N_DEFAULT;
    }

    @Configuration.Key(BOTTOM_K_KEY)
    @Configuration.IntegerRange(min = 1)
    default int bottomK() {
        return BOTTOM_K_DEFAULT;
    }

    @Configuration.Key(BOTTOM_N_KEY)
    @Configuration.IntegerRange(min = 0)
    default int bottomN() {
        return BOTTOM_N_DEFAULT;
    }

    @Configuration.ConvertWith(method = "org.neo4j.gds.similarity.nodesim.ComponentSpec#parse")
    @Configuration.ToMapValue(          "org.neo4j.gds.similarity.nodesim.ComponentSpec#render")
    default ComponentSpec useComponents() {
        return ComponentSpec.NO;
    }

    @Configuration.Ignore
    @Deprecated(forRemoval = true) // Don't use configs for internal parameters
    default boolean computeToStream() {
        return false;
    }

    @Configuration.Check
    default void validate() {
        if (topK() != TOP_K_DEFAULT && bottomK() != BOTTOM_K_DEFAULT) {
            throw new IllegalArgumentException(formatWithLocale(
                "Invalid parameter combination: %s combined with %s",
                TOP_K_KEY,
                BOTTOM_K_KEY
            ));
        }
        if (topN() != TOP_N_DEFAULT && bottomN() != BOTTOM_N_DEFAULT) {
            throw new IllegalArgumentException(formatWithLocale(
                "Invalid parameter combination: %s combined with %s",
                TOP_N_KEY,
                BOTTOM_N_KEY
            ));
        }
        if (upperDegreeCutoff() < degreeCutoff()) {
            throw new IllegalArgumentException(formatWithLocale(
                "The value of upperDegreeCutoff cannot be smaller than degreeCutoff"
            ));
        }
    }

    @Configuration.GraphStoreValidationCheck
    default void validateComponentProperty(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        var componentsUsage = useComponents();
        if (componentsUsage.usePreComputedComponents()) {
            ConfigNodesValidations.validateNodePropertyExists(graphStore, selectedLabels, "Component property", componentsUsage.componentProperty());
        }
    }


    @Configuration.Ignore
    default NodeSimilarityParameters toParameters() {
        // topK and bottomK are exclusive, user cannot set both.
        // Any K value, top or bottom, is specified as a positive integer by user.
        // Internally we represent top/bottom by sign, so the normalized K value is either topK unchanged or bottomK but with sign flipped.
        // If the user set neither, we use topK.
        // Therefore, we check if bottomK changed from default. If it didn't either the user set topK or neither.
        // If bottomK changed, normalizedK is -bottomK. If it didn't, normalizedK is topK.
        var bottomKSetByUser = bottomK() != BOTTOM_K_DEFAULT;
        var normalizedK = bottomKSetByUser ? -bottomK() : topK();
        // Same for topN/bottomN
        var bottomNSetByUser = bottomN() != BOTTOM_N_DEFAULT;
        var normalizedN = bottomNSetByUser ? -bottomN() : topN();

        var componentUsage = useComponents();
        return new NodeSimilarityParameters(
            similarityMetric().build(similarityCutoff()),
            degreeCutoff(),
            upperDegreeCutoff(),
            normalizedK,
            normalizedN,
            computeToStream(),
            hasRelationshipWeightProperty(),
            componentUsage.useComponents(),
            componentUsage.componentProperty()
        );
    }

    @Configuration.Ignore
    default NodeSimilarityEstimateParameters toMemoryEstimateParameters() {
        return toParameters().memoryParameters();
    }

}
