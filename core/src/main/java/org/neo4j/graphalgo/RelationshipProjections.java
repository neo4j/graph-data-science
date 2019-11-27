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
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.stream.Streams;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toMap;

public final class RelationshipProjections {

    private static final RelationshipProjections EMPTY = new RelationshipProjections(emptyMap());

    public static RelationshipProjections of(@Nullable String type) {
        if (StringUtils.isEmpty(type)) {
            return EMPTY;
        }
        ElementIdentifier identifier = new ElementIdentifier(type);
        RelationshipProjection filter = RelationshipProjection.fromString(type);
        return create(singletonMap(identifier, filter));
    }

    public static RelationshipProjections of(Map<String, ?> map) {
        Map<ElementIdentifier, RelationshipProjection> projections = new LinkedHashMap<>();
        map.forEach((name, spec) -> {
            ElementIdentifier identifier = new ElementIdentifier(name);
            RelationshipProjection filter = RelationshipProjection.fromObject(spec, identifier);
            // sanity
            if (projections.put(identifier, filter) != null) {
                throw new IllegalStateException(String.format("Duplicate key: %s", name));
            }
        });
        return create(projections);
    }

    public static RelationshipProjections of(Iterable<?> items) {
        Map<ElementIdentifier, RelationshipProjection> filters = new LinkedHashMap<>();
        for (Object item : items) {
            RelationshipProjections relationshipProjections = fromObject(item);
            filters.putAll(relationshipProjections.projections);
        }
        return create(filters);
    }

    public static RelationshipProjections fromObject(Object object) {
        if (object == null) {
            return of(emptyMap());
        }
        if (object instanceof String) {
            return of((String) object);
        }
        if (object instanceof Map) {
            @SuppressWarnings("unchecked") Map<String, ?> map = (Map) object;
            return of(map);
        }
        if (object instanceof Iterable) {
            Iterable<?> list = (Iterable) object;
            return of(list);
        }
        throw new IllegalArgumentException(String.format(
            "Cannot construct a relationship filter out of a %s",
            object.getClass().getName()
        ));
    }

    public static RelationshipProjections single(ElementIdentifier identifier, RelationshipProjection projection) {
        return create(singletonMap(identifier, projection));
    }

    public static RelationshipProjections pair(
        ElementIdentifier identifier1,
        RelationshipProjection projection1,
        ElementIdentifier identifier2,
        RelationshipProjection projection2
    ) {
        return create(MapUtil.genericMap(new HashMap<>(), identifier1, projection1, identifier2, projection2));
    }

    private static RelationshipProjections create(Map<ElementIdentifier, RelationshipProjection> projections) {
        if (projections.values().stream().allMatch(RelationshipProjection::isMatchAll)) {
            return EMPTY;
        }
        return new RelationshipProjections(unmodifiableMap(projections));
    }

    private final Map<ElementIdentifier, RelationshipProjection> projections;

    private RelationshipProjections(Map<ElementIdentifier, RelationshipProjection> projections) {
        this.projections = projections;
    }

    public RelationshipProjection getFilter(ElementIdentifier identifier) {
        RelationshipProjection filter = projections.get(identifier);
        if (filter == null) {
            throw new IllegalArgumentException("Relationship type identifier does not exist: " + identifier);
        }
        return filter;
    }

    public Collection<RelationshipProjection> allFilters() {
        return projections.values();
    }

    public RelationshipProjections addAggregation(DeduplicationStrategy aggregation) {
        if (aggregation == DeduplicationStrategy.DEFAULT) {
            return this;
        }
        return modifyProjections(p -> p.withAggregation(aggregation));
    }

    public RelationshipProjections addPropertyMappings(PropertyMappings mappings) {
        if (!mappings.hasMappings()) {
            return this;
        }
        return modifyProjections(p -> p.withAdditionalPropertyMappings(mappings));
    }

    private RelationshipProjections modifyProjections(UnaryOperator<RelationshipProjection> operator) {
        Map<ElementIdentifier, RelationshipProjection> newProjections = projections.entrySet().stream().collect(toMap(
            Map.Entry::getKey,
            e -> operator.apply(e.getValue())
        ));
        if (newProjections.isEmpty()) {
            // TODO: special identifier for 'SELECT ALL'
            newProjections.put(
                new ElementIdentifier("*"),
                operator.apply(RelationshipProjection.empty())
            );
        }
        return create(newProjections);
    }

    public String typeFilter() {
        if (isEmpty()) {
            return "";
        }
        return projections
            .values()
            .stream()
            .map(RelationshipProjection::type)
            .flatMap(Streams::ofOptional)
            .distinct()
            .collect(Collectors.joining("|"));
    }

    public boolean isEmpty() {
        return this == EMPTY;
    }

    public static RelationshipProjections empty() {
        return EMPTY;
    }

    public Map<String, Object> toObject() {
        Map<String, Object> value = new LinkedHashMap<>();
        projections.forEach((identifier, projection) -> {
            value.put(identifier.name, projection.toObject());
        });
        return value;
    }
}
