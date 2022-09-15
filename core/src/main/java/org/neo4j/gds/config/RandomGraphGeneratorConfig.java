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

import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.NodeProjection;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.GraphStoreFactory;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

@ValueClass
@Configuration
@SuppressWarnings("immutables:subtype")
public interface RandomGraphGeneratorConfig extends GraphProjectConfig {

    String RELATIONSHIP_SEED_KEY = "relationshipSeed";
    String RELATIONSHIP_PROPERTY_KEY = "relationshipProperty";
    String RELATIONSHIP_DISTRIBUTION_KEY = "relationshipDistribution";
    String RELATIONSHIP_PROPERTY_NAME_KEY = "name";
    String RELATIONSHIP_PROPERTY_TYPE_KEY = "type";
    String RELATIONSHIP_PROPERTY_MIN_KEY = "min";
    String RELATIONSHIP_PROPERTY_MAX_KEY = "max";
    String RELATIONSHIP_PROPERTY_VALUE_KEY = "value";

    @Configuration.Parameter
    long nodeCount();

    @Configuration.Parameter
    long averageDegree();

    @Value.Default
    @Configuration.ConvertWith(method = "org.neo4j.gds.core.Aggregation#parse")
    @Configuration.ToMapValue("org.neo4j.gds.core.Aggregation#toString")
    default Aggregation aggregation() {
        return Aggregation.NONE;
    }

    @Value.Default
    @Configuration.ConvertWith(method = "org.neo4j.gds.Orientation#parse")
    @Configuration.ToMapValue("org.neo4j.gds.Orientation#toString")
    default Orientation orientation() {
        return Orientation.NATURAL;
    }

    @Value.Default
    default boolean allowSelfLoops() {
        return false;
    }

    @Value.Default
    @Configuration.ConvertWith(method = "org.neo4j.gds.beta.generator.RelationshipDistribution#parse")
    @Configuration.ToMapValue("org.neo4j.gds.beta.generator.RelationshipDistribution#toString")
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

    @Value.Default
    @Configuration.ToMapValue("org.neo4j.gds.AbstractNodeProjections#toObject")
    default NodeProjections nodeProjections() {
        return NodeProjections.builder()
            .putProjection(
                NodeLabel.of(nodeCount() + "_Nodes"),
                NodeProjection.of(nodeCount() + "_Nodes"))
            .build();
    }

    @Value.Default
    @Configuration.Ignore
    default RelationshipType relationshipType() {
        return RelationshipType.of("REL");
    }

    @Value.Default
    @Configuration.ToMapValue("org.neo4j.gds.AbstractRelationshipProjections#toObject")
    default RelationshipProjections relationshipProjections() {
        return RelationshipProjections.builder()
            .putProjection(
                relationshipType(),
                RelationshipProjection.of(relationshipType().name, orientation(), aggregation())
            )
            .build();
    }

    @Configuration.Ignore
    @Override
    default GraphStoreFactory.Supplier graphStoreFactory() {
        // TODO: maybe we could introduce a RandomGraphFactory
        throw new UnsupportedOperationException("RandomGraphGeneratorConfig requires explicit graph generation.");
    }

    @Override
    @Configuration.Ignore
    default <R> R accept(Cases<R> visitor) {
        return visitor.random(this);
    }

    @Value.Derived
    @Configuration.Ignore
    default Set<String> outputFieldDenylist() {
        return Set.of(READ_CONCURRENCY_KEY,  NODE_COUNT_KEY, RELATIONSHIP_COUNT_KEY, "validateRelationships");
    }

    static RandomGraphGeneratorConfig of(
        String username,
        String graphName,
        long nodeCount,
        long averageDegree,
        CypherMapWrapper config
    ) {
        return new RandomGraphGeneratorConfigImpl(nodeCount, averageDegree, username, graphName, config);
    }

    enum AllowSelfLoops {
        YES(true), NO(false);

        private final boolean value;

        AllowSelfLoops(boolean value) {
            this.value = value;
        }

        public static AllowSelfLoops of(boolean value) {
            return value ? YES : NO;
        }

        public boolean value() {
            return value;
        }
    }
}
