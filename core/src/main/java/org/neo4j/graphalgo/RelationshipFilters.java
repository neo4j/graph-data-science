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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;

public final class RelationshipFilters {

    private static final RelationshipFilters EMPTY = new RelationshipFilters(emptyMap());

    public static RelationshipFilters of(@Nullable String type) {
        if (StringUtils.isEmpty(type)) {
            return EMPTY;
        }
        ElementIdentifier identifier = new ElementIdentifier(type);
        RelationshipFilter filter = RelationshipFilter.of(type);
        return create(singletonMap(identifier, filter));
    }

    public static RelationshipFilters of(Map<String, ?> map) {
        Map<ElementIdentifier, RelationshipFilter> filters = new LinkedHashMap<>();
        map.forEach((name, spec) -> {
            ElementIdentifier identifier = new ElementIdentifier(name);
            RelationshipFilter filter = RelationshipFilter.fromObject(spec, identifier);
            // sanity
            if (filters.put(identifier, filter) != null) {
                throw new IllegalStateException(String.format("Duplicate key: %s", name));
            }
        });
        return create(filters);
    }

    public static RelationshipFilters of(Iterable<?> items) {
        Map<ElementIdentifier, RelationshipFilter> filters = new LinkedHashMap<>();
        for (Object item : items) {
            RelationshipFilters relationshipFilters = fromObject(item);
            filters.putAll(relationshipFilters.filters);
        }
        return create(filters);
    }

    public static RelationshipFilters fromObject(Object object) {
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

    private static RelationshipFilters create(Map<ElementIdentifier, RelationshipFilter> filters) {
        if (filters.values().stream().allMatch(RelationshipFilter::isEmpty)) {
            return EMPTY;
        }

        filters.values().stream()
            .collect(Collectors.collectingAndThen(
                Collectors.groupingBy(filter -> filter.type(), Collectors.counting()),
                map -> {
                    Stream<Map.Entry<String, Long>> duplicateTypes = map
                        .entrySet()
                        .stream()
                        .filter(entry -> entry.getValue() > 1);
                    duplicateTypes.forEach(entry -> {
                        throw new IllegalArgumentException(String.format(
                            "Duplicate relationship type: '%s'",
                            entry.getKey()
                        ));
                    });
                    return true;
                }
            ));

        return new RelationshipFilters(unmodifiableMap(filters));
    }

    private final Map<ElementIdentifier, RelationshipFilter> filters;

    private RelationshipFilters(Map<ElementIdentifier, RelationshipFilter> filters) {
        this.filters = filters;
    }

    public RelationshipFilter getFilter(ElementIdentifier identifier) {
        RelationshipFilter filter = filters.get(identifier);
        if (filter == null) {
            throw new IllegalArgumentException("Relationship type identifier does not exist: " + identifier);
        }
        return filter;
    }

    public Collection<RelationshipFilter> allFilters() {
        return filters.values();
    }

    public String typeFilter() {
        if (isEmpty()) {
            return "";
        }
        return filters.values().stream().map(f -> f.type()).collect(Collectors.joining("|"));
    }

    public boolean isEmpty() {
        return this == EMPTY;
    }

    public static RelationshipFilters empty() {
        return EMPTY;
    }

    public Map<String, Object> toObject() {
        Map<String, Object> value = new LinkedHashMap<>();
        filters.forEach((identifier, filter) -> {
            value.put(identifier.name, filter.toObject());
        });
        return value;
    }
}
