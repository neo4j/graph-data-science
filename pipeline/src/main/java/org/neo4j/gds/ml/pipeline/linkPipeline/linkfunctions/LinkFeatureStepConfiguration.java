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
package org.neo4j.gds.ml.pipeline.linkPipeline.linkfunctions;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.utils.StringJoining;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@Configuration
public interface LinkFeatureStepConfiguration {

    @Configuration.ConvertWith(method = "fromObject")
    List<String> nodeProperties();

    static List<String> fromObject(Object nodeProperties) {
        if (nodeProperties instanceof List) {
            List<?> nodePropertiesList = (List<?>) nodeProperties;

            if (nodePropertiesList.isEmpty()) {
                throw new IllegalArgumentException("`nodeProperties` must be non-empty.");
            }

            List<String> invalidProperties = nodePropertiesList
                .stream()
                .filter(property -> !(property instanceof String) || ((String) property).isBlank())
                .map(Object::toString)
                .collect(Collectors.toList());

            if (!invalidProperties.isEmpty()) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Invalid property names defined in `nodeProperties`: %s. Expecting a String with at least one non-white space character.",
                    StringJoining.join(invalidProperties)
                ));
            }
        } else {
            throw new IllegalArgumentException(formatWithLocale(
                "The value of `nodeProperties` must be of type `List` but was `%s`.",
                nodeProperties.getClass().getSimpleName()
            ));
        }

        return ((List<String>) nodeProperties);
    }

    @Configuration.CollectKeys
    @Value.Auxiliary
    @Value.Default
    @Value.Parameter(false)
    default Collection<String> configKeys() {
        return Collections.emptyList();
    }
}
