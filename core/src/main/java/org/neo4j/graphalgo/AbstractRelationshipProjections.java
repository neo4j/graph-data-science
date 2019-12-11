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
import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.annotation.DataClass;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.stream.Streams;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toMap;

@DataClass
@Value.Immutable(singleton = true)
public abstract class AbstractRelationshipProjections {

    public abstract Map<ElementIdentifier, RelationshipProjection> projections();

    public static RelationshipProjections fromObject(Object object) {
        if (object == null) {
            return empty();
        }
        if (object instanceof RelationshipProjections) {
            return (RelationshipProjections) object;
        }
        if (object instanceof String) {
            return fromString((String) object);
        }
        if (object instanceof Map) {
            @SuppressWarnings("unchecked") Map<String, ?> map = (Map) object;
            return fromMap(map);
        }
        if (object instanceof Iterable) {
            Iterable<?> list = (Iterable) object;
            return fromList(list);
        }
        throw new IllegalArgumentException(String.format(
            "Cannot construct a relationship filter out of a %s",
            object.getClass().getName()
        ));
    }

    public static RelationshipProjections fromString(@Nullable String type) {
        if (StringUtils.isEmpty(type)) {
            return empty();
        }
        ElementIdentifier identifier = new ElementIdentifier(type);
        RelationshipProjection filter = RelationshipProjection.fromString(type);
        return create(singletonMap(identifier, filter));
    }

    public static RelationshipProjections fromMap(Map<String, ?> map) {
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

    public static RelationshipProjections fromList(Iterable<?> items) {
        Map<ElementIdentifier, RelationshipProjection> filters = new LinkedHashMap<>();
        for (Object item : items) {
            RelationshipProjections relationshipProjections = fromObject(item);
            filters.putAll(relationshipProjections.projections());
        }
        return create(filters);
    }

    public static RelationshipProjections single(ElementIdentifier identifier, RelationshipProjection projection) {
        return RelationshipProjections
            .builder()
            .putProjection(identifier, projection)
            .build();
    }

    public static RelationshipProjections pair(
        ElementIdentifier identifier1,
        RelationshipProjection projection1,
        ElementIdentifier identifier2,
        RelationshipProjection projection2
    ) {
        return RelationshipProjections
            .builder()
            .putProjection(identifier1, projection1)
            .putProjection(identifier2, projection2)
            .build();
    }

    private static RelationshipProjections create(Map<ElementIdentifier, RelationshipProjection> projections) {
        if (projections.values().stream().allMatch(RelationshipProjection::isMatchAll)) {
            return empty();
        }
        return RelationshipProjections.of(projections);
    }

    public RelationshipProjection getFilter(ElementIdentifier identifier) {
        RelationshipProjection filter = projections().get(identifier);
        if (filter == null) {
            throw new IllegalArgumentException("Relationship type identifier does not exist: " + identifier);
        }
        return filter;
    }

    public Collection<RelationshipProjection> allFilters() {
        return projections().values();
    }

    public RelationshipProjections addAggregation(DeduplicationStrategy aggregation) {
        if (aggregation == DeduplicationStrategy.DEFAULT) {
            return RelationshipProjections.copyOf(this);
        }
        return modifyProjections(p -> p.withAggregation(aggregation));
    }

    public RelationshipProjections addPropertyMappings(PropertyMappings mappings) {
        if (!mappings.hasMappings()) {
            return RelationshipProjections.copyOf(this);
        }
        return modifyProjections(p -> p.withAdditionalPropertyMappings(mappings));
    }

    private RelationshipProjections modifyProjections(UnaryOperator<RelationshipProjection> operator) {
        Map<ElementIdentifier, RelationshipProjection> newProjections = projections().entrySet().stream().collect(toMap(
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
        return projections()
            .values()
            .stream()
            .map(RelationshipProjection::type)
            .flatMap(Streams::ofOptional)
            .distinct()
            .collect(Collectors.joining("|"));
    }

    public boolean isEmpty() {
        return this == RelationshipProjections.of();
    }

    public static RelationshipProjections empty() {
        return RelationshipProjections.of();
    }

    public Map<String, Object> toObject() {
        Map<String, Object> value = new LinkedHashMap<>();
        projections().forEach((identifier, projection) -> {
            value.put(identifier.name, projection.toObject());
        });
        return value;
    }

    public Set<String> allRelationshipProperties() {
        return projections()
            .values()
            .stream()
            .flatMap(p -> p.properties().mappings().stream().map(PropertyMapping::propertyKey))
            .collect(Collectors.toSet());
    }
}
