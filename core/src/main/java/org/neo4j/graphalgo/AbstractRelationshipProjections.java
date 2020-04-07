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
package org.neo4j.graphalgo;

import org.apache.commons.lang3.StringUtils;
import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.annotation.DataClass;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toMap;
import static org.neo4j.graphalgo.RelationshipType.ALL_RELATIONSHIPS;

@DataClass
@Value.Immutable(singleton = true)
public abstract class AbstractRelationshipProjections extends AbstractProjections<RelationshipType, RelationshipProjection> {

    public abstract Map<RelationshipType, RelationshipProjection> projections();

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
            "Cannot construct a relationship projection out of a %s",
            object.getClass().getName()
        ));
    }

    public static RelationshipProjections fromString(@Nullable String typeString) {
        if (StringUtils.isEmpty(typeString)) {
            create(emptyMap());
        }
        if (typeString.equals(ALL_RELATIONSHIPS.name)) {
            return create(singletonMap(ALL_RELATIONSHIPS, RelationshipProjection.all()));
        }

        RelationshipType relationshipType = RelationshipType.of(typeString);
        RelationshipProjection filter = RelationshipProjection.fromString(typeString);
        return create(singletonMap(relationshipType, filter));
    }

    public static RelationshipProjections fromMap(Map<String, ?> map) {
        Map<RelationshipType, RelationshipProjection> projections = new LinkedHashMap<>();
        map.forEach((name, spec) -> {
            RelationshipType relationshipType = RelationshipType.of(name);
            RelationshipProjection filter = RelationshipProjection.fromObject(spec, relationshipType);
            // sanity
            if (projections.put(relationshipType, filter) != null) {
                throw new IllegalStateException(String.format("Duplicate key: %s", name));
            }
        });
        return create(projections);
    }

    public static RelationshipProjections fromList(Iterable<?> items) {
        Map<RelationshipType, RelationshipProjection> filters = new LinkedHashMap<>();
        for (Object item : items) {
            RelationshipProjections relationshipProjections = fromObject(item);
            filters.putAll(relationshipProjections.projections());
        }
        return create(filters);
    }

    public static RelationshipProjections single(RelationshipType relationshipType, RelationshipProjection projection) {
        return RelationshipProjections
            .builder()
            .putProjection(relationshipType, projection)
            .build();
    }

    public static RelationshipProjections pair(
        RelationshipType relationshipType1,
        RelationshipProjection projection1,
        RelationshipType relationshipType2,
        RelationshipProjection projection2
    ) {
        return RelationshipProjections
            .builder()
            .putProjection(relationshipType1, projection1)
            .putProjection(relationshipType2, projection2)
            .build();
    }

    private static RelationshipProjections create(Map<RelationshipType, RelationshipProjection> projections) {
        if (projections.isEmpty()) {
            throw new IllegalArgumentException(
                "An empty relationship projection was given; at least one relationship type must be projected.");
        }
        if (projections.size() > 1 && projections.containsKey(ALL_RELATIONSHIPS)) {
            throw new IllegalArgumentException(
                "A star projection (all relationships) cannot be combined with another projection.");
        }
        return RelationshipProjections.of(projections);
    }

    public RelationshipProjection getFilter(RelationshipType relationshipType) {
        RelationshipProjection filter = projections().get(relationshipType);
        if (filter == null) {
            throw new IllegalArgumentException("Relationship type does not exist: " + relationshipType);
        }
        return filter;
    }

    public RelationshipProjections addPropertyMappings(PropertyMappings mappings) {
        if (!mappings.hasMappings()) {
            return RelationshipProjections.copyOf(this);
        }
        return modifyProjections(p -> p.withAdditionalPropertyMappings(mappings));
    }

    private RelationshipProjections modifyProjections(UnaryOperator<RelationshipProjection> operator) {
        Map<RelationshipType, RelationshipProjection> newProjections = projections().entrySet().stream().collect(toMap(
            Map.Entry::getKey,
            e -> operator.apply(e.getValue())
        ));
        if (newProjections.isEmpty()) {
            newProjections.put(
                ALL_RELATIONSHIPS,
                operator.apply(RelationshipProjection.all())
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
}
