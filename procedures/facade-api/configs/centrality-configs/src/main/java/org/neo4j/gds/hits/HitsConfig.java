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
package org.neo4j.gds.hits;


import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.beta.pregel.Partitioning;
import org.neo4j.gds.beta.pregel.PregelProcedureConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.StringIdentifierValidations;

import java.util.Collection;
import java.util.Locale;

@Configuration
public interface HitsConfig extends PregelProcedureConfig {

    default int hitsIterations() {
        return 20;
    }

    @Override
    @Configuration.Ignore
    default int maxIterations() {
        return hitsIterations() * 4;
    }

    @Override
    @Configuration.Ignore
    default boolean isAsynchronous() {
        return false;
    }

    @Configuration.ConvertWith(method = "validateHubProperty")
    default String hubProperty() {
        return "hub";
    }

    @Configuration.ConvertWith(method = "validateAuthProperty")
    default String authProperty() {
        return "auth";
    }

    @Override
    @Configuration.ConvertWith(method = "org.neo4j.gds.beta.pregel.Partitioning#parse")
    @Configuration.ToMapValue("org.neo4j.gds.beta.pregel.Partitioning#toString")
    default Partitioning partitioning() {
        return Partitioning.AUTO;
    }

    static @Nullable String validateHubProperty(String input) {
        return StringIdentifierValidations.validateNoWhiteCharacter(input, "hubProperty");
    }

    static @Nullable String validateAuthProperty(String input) {
        return StringIdentifierValidations.validateNoWhiteCharacter(input, "authProperty");
    }

    static HitsConfig of(CypherMapWrapper userConfig) {
        return new HitsConfigImpl(userConfig);
    }


    @Configuration.GraphStoreValidationCheck
    default void validateTargetRelIsUndirected(
        GraphStore graphStore,
        Collection<NodeLabel> ignored,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {

        var relationshipTypes = selectedRelationshipTypes;
        var relationshipSchema = graphStore.schema().relationshipSchema();

        var undirectedTypes = relationshipTypes
            .stream()
            .filter(relationshipSchema::isUndirected)
            .map(RelationshipType::name)
            .toList();

        if (!undirectedTypes.isEmpty()) {
            var stringBuilder= new StringBuilder("["+undirectedTypes.get(0));
            for (int index=1; index < undirectedTypes.size();++index){
                stringBuilder.append(",").append(undirectedTypes.get(index));
            }
            stringBuilder.append("]");
            throw new IllegalArgumentException(String.format(
                Locale.US,
                "This algorithm requires a directed graph, but the following configured relationship types are undirected: %s.",
                stringBuilder
            ));
        }
    }
}

