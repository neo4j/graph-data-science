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
import org.neo4j.stream.Streams;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

public final class NodeProjections extends AbstractProjections<NodeProjection> {

    private static final NodeProjections EMPTY = new NodeProjections(emptyMap());

    public static NodeProjections of(@Nullable String label) {
        if (StringUtils.isEmpty(label)) {
            return EMPTY;
        }
        ElementIdentifier identifier = new ElementIdentifier(label);
        NodeProjection projection = NodeProjection.fromString(label);
        return create(singletonMap(identifier, projection));
    }

    public static NodeProjections of(Map<String, ?> map) {
        Map<ElementIdentifier, NodeProjection> projections = new LinkedHashMap<>();
        map.forEach((name, spec) -> {
            ElementIdentifier identifier = new ElementIdentifier(name);
            NodeProjection projection = NodeProjection.fromObject(spec, identifier);
            // sanity
            if (projections.put(identifier, projection) != null) {
                throw new IllegalStateException(String.format("Duplicate key: %s", name));
            }
        });
        return create(projections);
    }

    public static NodeProjections of(Iterable<?> items) {
        Map<ElementIdentifier, NodeProjection> projections = new LinkedHashMap<>();
        for (Object item : items) {
            NodeProjections nodeProjections = fromObject(item);
            projections.putAll(nodeProjections.projections);
        }
        return create(projections);
    }

    public static NodeProjections fromObject(Object object) {
        if (object == null) {
            return of(emptyMap());
        }
        if (object instanceof NodeProjections) {
            return (NodeProjections) object;
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
            "Cannot construct a node projection out of a %s",
            object.getClass().getName()
        ));
    }

    public static NodeProjections create(Map<ElementIdentifier, NodeProjection> projections) {
        if (projections.values().stream().allMatch(NodeProjection::isMatchAll)) {
            return EMPTY;
        }
        if (projections.size() != 1) {
            throw new IllegalArgumentException("Only one node projection is supported.");
        }
        return new NodeProjections(unmodifiableMap(projections));
    }

    private final Map<ElementIdentifier, NodeProjection> projections;

    private NodeProjections(Map<ElementIdentifier, NodeProjection> projections) {
        this.projections = projections;
    }

    @Override
    public Map<ElementIdentifier, NodeProjection> projections() {
        return projections;
    }

    public NodeProjection getProjection(ElementIdentifier identifier) {
        NodeProjection projection = projections.get(identifier);
        if (projection == null) {
            throw new IllegalArgumentException("Node label identifier does not exist: " + identifier);
        }
        return projection;
    }

    public Collection<NodeProjection> allProjections() {
        return projections.values();
    }

    public NodeProjections addPropertyMappings(PropertyMappings mappings) {
        if (!mappings.hasMappings()) {
            return this;
        }
        Map<ElementIdentifier, NodeProjection> newProjections = projections.entrySet().stream().collect(toMap(
            Map.Entry::getKey,
            e -> e.getValue().withAdditionalPropertyMappings(mappings)
        ));
        if (newProjections.isEmpty()) {
            // TODO: special identifier for 'SELECT ALL'
            newProjections.put(new ElementIdentifier("*"), NodeProjection.empty().withAdditionalPropertyMappings(mappings));
        }
        return create(newProjections);
    }

    public Optional<String> labelProjection() {
        if (isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(projections
            .values()
            .stream()
            .map(NodeProjection::label)
            .flatMap(Streams::ofOptional)
            .collect(joining(""))
        );
    }

    public boolean isEmpty() {
        return this == EMPTY;
    }

    public static NodeProjections empty() {
        return EMPTY;
    }

    public Map<String, Object> toObject() {
        Map<String, Object> value = new LinkedHashMap<>();
        projections.forEach((identifier, projection) -> {
            value.put(identifier.name, projection.toObject());
        });
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeProjections that = (NodeProjections) o;
        return projections.equals(that.projections);
    }

    @Override
    public int hashCode() {
        return Objects.hash(projections);
    }

    @Override
    public String toString() {
        return "NodeProjections{" +
               "projections=" + projections +
               '}';
    }
}
