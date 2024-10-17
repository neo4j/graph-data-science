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
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Collection;
import java.util.Map;

import static org.neo4j.gds.core.StringIdentifierValidations.emptyToNull;
import static org.neo4j.gds.core.StringIdentifierValidations.validateNoWhiteCharacter;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@Configuration
public interface IndirectExposureMutateConfig extends IndirectExposureConfig {

    @Configuration
    interface MutateProperties {
        String exposures();

        String hops();

        String parents();

        String roots();

        @SuppressWarnings("unchecked")
        static MutateProperties parse(Object o) {
            if (o instanceof MutateProperties m) {
                return m;
            } else if (o instanceof Map<?, ?> m) {
                var mapWrapper = CypherMapWrapper.create((Map<String, Object>) m);
                return new MutatePropertiesImpl(mapWrapper);
            }
            throw new IllegalArgumentException(formatWithLocale(
                "Expected MutateProperties or Map. Got %s.",
                o.getClass().getSimpleName()
            ));
        }

        static Map<String, Object> toMap(MutateProperties mutateProperties) {
            return Map.of(
                "exposures", mutateProperties.exposures(),
                "hops", mutateProperties.hops(),
                "parents", mutateProperties.parents(),
                "roots", mutateProperties.roots()
            );
        }
    }

    @Configuration.ConvertWith(method = "org.neo4j.gds.indirectExposure.IndirectExposureMutateConfig.MutateProperties#parse")
    @Configuration.ToMapValue("org.neo4j.gds.indirectExposure.IndirectExposureMutateConfig.MutateProperties#toMap")
    MutateProperties mutateProperties();

    @Configuration.Check
    default void validateMutateProperties() {
        validateNoWhiteCharacter(emptyToNull(mutateProperties().exposures()), "exposureMutateProperty");
        validateNoWhiteCharacter(emptyToNull(mutateProperties().hops()), "hopMutateProperty");
        validateNoWhiteCharacter(emptyToNull(mutateProperties().parents()), "parentMutateProperty");
        validateNoWhiteCharacter(emptyToNull(mutateProperties().roots()), "rootMutateProperty");
    }

    @Configuration.GraphStoreValidationCheck
    default void validateMutateProperty(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> ignored
    ) {
        validateMutateProperty(graphStore, selectedLabels, mutateProperties().exposures());
        validateMutateProperty(graphStore, selectedLabels, mutateProperties().hops());
        validateMutateProperty(graphStore, selectedLabels, mutateProperties().parents());
        validateMutateProperty(graphStore, selectedLabels, mutateProperties().roots());
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
