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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;

public final class NodeFilters {

    private static final NodeFilters EMPTY = new NodeFilters(emptyMap());

    public static NodeFilters of(@Nullable String label) {
        if (StringUtils.isEmpty(label)) {
            return EMPTY;
        }
        ElementIdentifier identifier = new ElementIdentifier(label);
        NodeFilter filter = NodeFilter.of(label);
        return create(singletonMap(identifier, filter));
    }

    public static NodeFilters of(Map<String, ?> map) {
        Map<ElementIdentifier, NodeFilter> filters = new LinkedHashMap<>();
        map.forEach((name, spec) -> {
            ElementIdentifier identifier = new ElementIdentifier(name);
            NodeFilter filter = NodeFilter.fromObject(spec, identifier);
            // sanity
            if (filters.put(identifier, filter) != null) {
                throw new IllegalStateException(String.format("Duplicate key: %s", name));
            }
        });
        return create(filters);
    }

    public static NodeFilters of(Iterable<?> items) {
        Map<ElementIdentifier, NodeFilter> filters = new LinkedHashMap<>();
        for (Object item : items) {
            NodeFilters nodeFilters = fromObject(item);
            filters.putAll(nodeFilters.filters);
        }
        return create(filters);
    }

    public static NodeFilters fromObject(Object object) {
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
            "Cannot construct a node filter out of a %s",
            object.getClass().getName()
        ));
    }

    private static NodeFilters create(Map<ElementIdentifier, NodeFilter> filters) {
        if (filters.values().stream().allMatch(NodeFilter::isEmpty)) {
            return EMPTY;
        }
        if (filters.size() != 1) {
            throw new IllegalArgumentException("Only one node filter is supported.");
        }
        return new NodeFilters(unmodifiableMap(filters));
    }

    private final Map<ElementIdentifier, NodeFilter> filters;

    private NodeFilters(Map<ElementIdentifier, NodeFilter> filters) {
        this.filters = filters;
    }

    public NodeFilter getFilter(ElementIdentifier identifier) {
        NodeFilter filter = filters.get(identifier);
        if (filter == null) {
            throw new IllegalArgumentException("Node label identifier does not exist: " + identifier);
        }
        return filter;
    }

    public Collection<NodeFilter> allFilters() {
        return filters.values();
    }

    public Optional<String> labelFilter() {
        if (isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(filters.values().stream().map(f -> f.label).collect(Collectors.joining("")));
    }

    public boolean isEmpty() {
        return this == EMPTY;
    }

    public static NodeFilters empty() {
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
