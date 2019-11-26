/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.core.DeduplicationStrategy;

import java.util.Map;
import java.util.Objects;

public final class RelationshipProjection extends ElementProjection {

    private static final String TYPE_KEY = "type";
    private static final String PROJECTION_KEY = "projection";
    private static final String AGGREGATION_KEY = "aggregation";

    private static final String DEFAULT_PROJECTION = "NATURAL";
    private static final DeduplicationStrategy DEFAULT_AGGREGATION = DeduplicationStrategy.DEFAULT;

    private static final RelationshipProjection EMPTY = new RelationshipProjection("", "", DEFAULT_AGGREGATION, PropertyMappings.EMPTY);

    private final String type;
    private final String projection;
    private final DeduplicationStrategy aggregation;

    private RelationshipProjection(
        String type,
        String projection,
        DeduplicationStrategy aggregation,
        PropertyMappings properties
    ) {
        super(properties);
        this.type = type;
        this.aggregation = aggregation;
        this.projection = projection.isEmpty() ? DEFAULT_PROJECTION : projection;
    }

    public static RelationshipProjection of(@Nullable String type) {
        return of(type, DEFAULT_PROJECTION);
    }

    public static RelationshipProjection of(@Nullable String type, @NotNull String projection) {
        return of(type, projection, DEFAULT_AGGREGATION);
    }

    public static RelationshipProjection of(@Nullable String type, @NotNull String projection, @NotNull DeduplicationStrategy aggregation) {
        return of(type, projection, aggregation, PropertyMappings.EMPTY);
    }

    public static RelationshipProjection of(@Nullable String type, @NotNull DeduplicationStrategy aggregation) {
        return of(type, DEFAULT_PROJECTION, aggregation, PropertyMappings.EMPTY);
    }

    public static RelationshipProjection of(@Nullable String type, @NotNull String projection, @NotNull DeduplicationStrategy aggregation, PropertyMappings properties) {
        if (StringUtils.isEmpty(type)) {
            return EMPTY;
        }
        return new RelationshipProjection(type, projection, aggregation, properties);
    }

    public static RelationshipProjection of(Map<String, Object> map, ElementIdentifier identifier) {
        String type = map.containsKey(TYPE_KEY) ? nonEmptyString(map, TYPE_KEY): identifier.name;
        String projection = map.containsKey(PROJECTION_KEY) ? nonEmptyString(map, PROJECTION_KEY) : DEFAULT_PROJECTION;
        DeduplicationStrategy aggregation = map.containsKey(AGGREGATION_KEY) ? DeduplicationStrategy.valueOf(
            nonEmptyString(map, AGGREGATION_KEY)) : DEFAULT_AGGREGATION;
        return create(map, properties -> of(type, projection, aggregation, properties));
    }

    public static RelationshipProjection fromObject(Object object, ElementIdentifier identifier) {
        if (object == null) {
            return EMPTY;
        }
        if (object instanceof String) {
            return of((String) object);
        }
        if (object instanceof Map) {
            @SuppressWarnings("unchecked") Map<String, Object> map = (Map) object;
            return of(map, identifier);
        }
        throw new IllegalArgumentException(String.format(
            "Cannot construct a relationship filter out of a %s",
            object.getClass().getName()
        ));
    }

    public boolean hasMappings() {
        return properties().hasMappings();
    }

    public boolean isEmpty() {
        return this == EMPTY;
    }

    @Override
    boolean includeAggregation() {
        return true;
    }

    @Override
    void writeToObject(Map<String, Object> value) {
        value.put(TYPE_KEY, type);
        value.put(PROJECTION_KEY, projection);
        value.put(AGGREGATION_KEY, aggregation.name());
    }

    @Override
    public RelationshipProjection withAdditionalPropertyMappings(PropertyMappings mappings) {
        PropertyMappings newMappings = properties().mergeWith(mappings);
        if (newMappings == properties()) {
            return this;
        }
        return new RelationshipProjection(type, projection, aggregation, newMappings);
    }

    public RelationshipProjection withAggregation(DeduplicationStrategy aggregation) {
        return new RelationshipProjection(type, projection, aggregation, properties());
    }

    public static RelationshipProjection empty() {
        return EMPTY;
    }

    public String type() {
        return type;
    }

    public String projection() {
        return projection;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RelationshipProjection that = (RelationshipProjection) o;
        return type.equals(that.type) &&
               projection.equals(that.projection) &&
               aggregation == that.aggregation;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, projection, aggregation);
    }

    @Override
    public String toString() {
        return "RelationshipProjection{" +
               "type='" + type + '\'' +
               ", projection='" + projection + '\'' +
               ", aggregation=" + aggregation +
               '}';
    }

    public DeduplicationStrategy aggregation() {
        return aggregation;
    }
}
