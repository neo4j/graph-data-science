/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

package org.neo4j.graphalgo.beta.generator;

import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.config.BaseConfig;

import java.util.Collections;
import java.util.Map;

@ValueClass
@Configuration("RandomGraphGeneratorConfigImpl")
@SuppressWarnings("immutables:subtype")
public interface RandomGraphGeneratorConfig extends BaseConfig {

    String RELATIONSHIP_SEED_KEY = "relationshipSeed";
    String RELATIONSHIP_PROPERTY_KEY = "relationshipProperty";
    String RELATIONSHIP_DISTRIBUTION_KEY = "relationshipDistribution";
    String RELATIONSHIP_PROPERTY_NAME_KEY = "name";
    String RELATIONSHIP_PROPERTY_TYPE_KEY = "type";
    String RELATIONSHIP_PROPERTY_MIN_KEY = "min";
    String RELATIONSHIP_PROPERTY_MAX_KEY = "max";
    String RELATIONSHIP_PROPERTY_VALUE_KEY = "value";

    @Configuration.Parameter
    String graphName();

    @Configuration.Parameter
    long nodeCount();

    @Configuration.Parameter
    long averageDegree();

    @Value.Default
    @Configuration.ConvertWith("org.neo4j.graphalgo.beta.generator.RelationshipDistribution#parse")
    default RelationshipDistribution relationshipDistribution() {
        return RelationshipDistribution.UNIFORM;
    }

    @Value.Default
    default @Nullable Long relationshipSeed() {
        return null;
    }

    // TODO: replace with type and parse from object
    default Map<String, Object> relationshipProperty() {
        return Collections.emptyMap();
    }

    static RandomGraphGeneratorConfig of(
        String username,
        String graphName,
        long nodeCount,
        long averageDegree,
        CypherMapWrapper config
    ) {
        return new RandomGraphGeneratorConfigImpl(graphName, nodeCount, averageDegree, username, config);
    }

}
