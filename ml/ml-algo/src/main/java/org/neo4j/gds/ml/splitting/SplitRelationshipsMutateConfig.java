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
package org.neo4j.gds.ml.splitting;

import org.immutables.value.Value;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.config.MutateConfig;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@Configuration
@SuppressWarnings("immutables:subtype")
public interface SplitRelationshipsMutateConfig extends SplitRelationshipsBaseConfig, MutateConfig {

    @Configuration.Ignore
    @Value.Derived
    default List<RelationshipType> superGraphTypes() {
        return Stream.concat(nonNegativeRelationshipTypes().stream(), relationshipTypes().stream())
            .map(RelationshipType::of)
            .collect(Collectors.toList());
    }

    static SplitRelationshipsMutateConfig of(CypherMapWrapper userInput) {
        return new SplitRelationshipsMutateConfigImpl(userInput);
    }
}
