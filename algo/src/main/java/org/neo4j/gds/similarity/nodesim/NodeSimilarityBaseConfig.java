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

import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.ConfigNodesValidations;
import org.neo4j.gds.config.RelationshipWeightConfig;

import java.util.Collection;

import static org.neo4j.gds.core.StringIdentifierValidations.emptyToNull;
import static org.neo4j.gds.core.StringIdentifierValidations.validateNoWhiteCharacter;
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

    String CONSIDER_COMPONENTS_KEY = "considerComponents";
    boolean CONSIDER_COMPONENTS = false;

    @Value.Default
    @Configuration.DoubleRange(min = 0, max = 1)
    default double similarityCutoff() {
        return 1E-42;
    }

    @Value.Default
    @Configuration.ConvertWith(method = "org.neo4j.gds.similarity.nodesim.MetricSimilarityComputer#parse")
    @Configuration.ToMapValue("org.neo4j.gds.similarity.nodesim.MetricSimilarityComputer#render")
    default MetricSimilarityComputer.MetricSimilarityComputerBuilder similarityMetric() {
        return new JaccardSimilarityComputer.Builder();
    }

    @Value.Default
    @Configuration.IntegerRange(min = 1)
    default int degreeCutoff() {
        return 1;
    }

    @Value.Default
    @Configuration.IntegerRange(min = 1)
    default int upperDegreeCutoff() {
        return Integer.MAX_VALUE;
    }

    @Value.Default
    @Configuration.Key(TOP_K_KEY)
    @Configuration.IntegerRange(min = 1)
    default int topK() {
        return TOP_K_DEFAULT;
    }

    @Value.Default
    @Configuration.Key(TOP_N_KEY)
    @Configuration.IntegerRange(min = 0)
    default int topN() {
        return TOP_N_DEFAULT;
    }

    @Value.Default
    @Configuration.Key(BOTTOM_K_KEY)
    @Configuration.IntegerRange(min = 1)
    default int bottomK() {
        return BOTTOM_K_DEFAULT;
    }

    @Value.Default
    @Configuration.Key(BOTTOM_N_KEY)
    @Configuration.IntegerRange(min = 0)
    default int bottomN() {
        return BOTTOM_N_DEFAULT;
    }

    @Value.Default
    @Configuration.ConvertWith(method = "validatePropertyName")
    @Configuration.Key(COMPONENT_PROPERTY_KEY)
    default @Nullable String componentProperty() { return null; }

    @Value.Default
    @Configuration.Key(CONSIDER_COMPONENTS_KEY)
    default boolean considerComponents() { return CONSIDER_COMPONENTS; }

    @Configuration.Ignore
    @Value.Derived
    default int normalizedK() {
        return bottomK() != BOTTOM_K_DEFAULT
            ? -bottomK()
            : topK();
    }

    @Configuration.Ignore
    @Value.Derived
    default int normalizedN() {
        return bottomN() != BOTTOM_N_DEFAULT
            ? -bottomN()
            : topN();
    }

    @Configuration.Ignore
    @Value.Derived
    default boolean isParallel() {
        return concurrency() > 1;
    }

    @Configuration.Ignore
    @Value.Derived
    default boolean hasTopK() {
        return normalizedK() != 0;
    }

    @Configuration.Ignore
    @Value.Derived
    default boolean hasTopN() {
        return normalizedN() != 0;
    }

    @Configuration.Ignore
    default boolean computeToStream() {
        return false;
    }

    @Configuration.Ignore
    @Value.Derived
    default boolean computeToGraph() {
        return !computeToStream();
    }

    @Value.Check
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

    static @Nullable String validatePropertyName(String input) {
        return validateNoWhiteCharacter(emptyToNull(input), COMPONENT_PROPERTY_KEY);
    }

    @Configuration.GraphStoreValidationCheck
    default void validateComponentProperty(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        String componentProperty = componentProperty();
        if (componentProperty != null) {
            ConfigNodesValidations.validateNodePropertyExists(graphStore, selectedLabels, "Component property", componentProperty);
        }
    }

    @Value.Derived
    default boolean runWCC() {
        return considerComponents() && componentProperty() == null;
    }

}
