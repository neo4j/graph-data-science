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
package org.neo4j.gds.catalog;

import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.BaseConfig;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.utils.StringJoining;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@ValueClass
@Configuration
@SuppressWarnings("immutables:subtype")
public interface GraphRemoveNodePropertiesConfig extends BaseConfig, ConcurrencyConfig {
    @Configuration.Parameter
    Optional<String> graphName();

    @Configuration.Parameter
    @Configuration.ConvertWith("org.neo4j.gds.catalog.UserInputAsStringOrListOfString#parse")
    List<String> nodeProperties();


    static GraphRemoveNodePropertiesConfig of(
        String graphName,
        Object nodeProperties,
        CypherMapWrapper config
    ) {
        return new GraphRemoveNodePropertiesConfigImpl(
            Optional.of(graphName),
            nodeProperties,
            config
        );
    }

    @Configuration.Ignore
    default void validate(GraphStore graphStore) {
        List<String> invalidProperties = nodeProperties()
            .stream()
            .filter(nodeProperty -> !graphStore.hasNodeProperty(nodeProperty))
            .collect(Collectors.toList());

        if (!invalidProperties.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "Could not find property key(s) %s. Defined keys: %s.",
                StringJoining.join(invalidProperties),
                StringJoining.join(graphStore.nodePropertyKeys())
            ));
        }
    }
}
