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
package org.neo4j.gds.config;

import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.StringSimilarity;
import org.neo4j.gds.utils.StringJoining;

import java.util.List;
import java.util.Optional;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@ValueClass
@Configuration
@SuppressWarnings("immutables:subtype")
public interface GraphAccessGraphPropertyConfig extends BaseConfig {

    @Configuration.Parameter
    Optional<String> graphName();

    @Configuration.Parameter
    String graphProperty();

    @Configuration.Ignore
    default void validate(GraphStore graphStore) {
        if (!graphStore.hasGraphProperty(graphProperty())) {
            List<String> candidates = StringSimilarity.similarStringsIgnoreCase(
                graphProperty(),
                graphStore.graphPropertyKeys()
            );

            var message = !candidates.isEmpty()
                ? formatWithLocale("Did you mean: %s.", StringJoining.join(candidates))
                : formatWithLocale("The following properties exist in the graph %s.", StringJoining.join(graphStore.graphPropertyKeys()));

            throw new IllegalArgumentException(formatWithLocale(
                "The specified graph property '%s' does not exist. %s",
                graphProperty(),
                message
            ));
        }
    }

    static GraphAccessGraphPropertyConfig of(
        String graphName,
        String graphProperty,
        CypherMapWrapper config
    ) {
        return new GraphAccessGraphPropertyConfigImpl(
            Optional.of(graphName),
            graphProperty,
            config
        );
    }

}
