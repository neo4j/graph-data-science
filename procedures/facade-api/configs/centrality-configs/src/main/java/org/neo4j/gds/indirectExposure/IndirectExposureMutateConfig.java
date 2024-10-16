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
package org.neo4j.gds.indirectExposure;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.MutateNodePropertyConfig;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Collection;

import static org.neo4j.gds.core.StringIdentifierValidations.emptyToNull;
import static org.neo4j.gds.core.StringIdentifierValidations.validateNoWhiteCharacter;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@Configuration
public interface IndirectExposureMutateConfig extends IndirectExposureConfig, MutateNodePropertyConfig {

    /**
     * Will take the value of the mutateProperty if not explicitly set.
     */
    default String exposureProperty() {
        return mutateProperty();
    }

    String hopProperty();

    String parentProperty();

    String rootProperty();

    @Configuration.Check
    default void validateMutateProperties() {
        validateNoWhiteCharacter(emptyToNull(exposureProperty()), "exposureProperty");
        validateNoWhiteCharacter(emptyToNull(hopProperty()), "hopProperty");
        validateNoWhiteCharacter(emptyToNull(parentProperty()), "parentProperty");
        validateNoWhiteCharacter(emptyToNull(rootProperty()), "Property");
    }

    @Override
    @Configuration.GraphStoreValidationCheck
    default void validateMutateProperty(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        validateMutateProperty(graphStore, selectedLabels, exposureProperty());
        validateMutateProperty(graphStore, selectedLabels, hopProperty());
        validateMutateProperty(graphStore, selectedLabels, parentProperty());
        validateMutateProperty(graphStore, selectedLabels, rootProperty());
    }

    static void validateMutateProperty(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        String mutateProperty
    ) {
        if (mutateProperty != null && graphStore.hasNodeProperty(selectedLabels, mutateProperty)) {
            throw new IllegalArgumentException(formatWithLocale(
                "Node property `%s` already exists in the in-memory graph.",
                mutateProperty
            ));
        }
    }

    static IndirectExposureMutateConfig of(CypherMapWrapper userInput) {
        return new IndirectExposureMutateConfigImpl(userInput);
    }
}
