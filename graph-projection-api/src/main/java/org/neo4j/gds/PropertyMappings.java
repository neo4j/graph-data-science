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
package org.neo4j.gds;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singletonMap;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public record PropertyMappings(List<PropertyMapping> mappings) implements Iterable<PropertyMapping> {
    public PropertyMappings {
        checkForAggregationMixing(mappings);
    }
    public static final PropertyMappings EMPTY = new PropertyMappings(Collections.emptyList());

    public int count() {
        return mappings().size();
    }

    public static PropertyMappings of(PropertyMapping... mappings) {
        if (mappings == null || mappings.length == 0) {
            return EMPTY;
        }
        return new PropertyMappings(Arrays.asList(mappings));
    }

    public static PropertyMappings of(List<PropertyMapping> mappings) {
        if (mappings == null || mappings.isEmpty()) {
            return EMPTY;
        }
        return new PropertyMappings(mappings);
    }

    public static PropertyMappings of(List<PropertyMapping> mappings, Aggregation aggregation) {
        if (mappings == null || mappings.isEmpty()) {
            return EMPTY;
        }
        return new PropertyMappings(applyAggregation(mappings, aggregation));
    }


    public static PropertyMappings fromObject(Object propertyMappingInput) {
        return fromObject(propertyMappingInput, Aggregation.DEFAULT);
    }

    public static PropertyMappings fromObject(Object propertyMappingInput, Aggregation aggregation) {
        if (propertyMappingInput instanceof PropertyMappings properties) {
            List<PropertyMapping> newMappings = applyAggregation(properties.mappings, aggregation);
            return new PropertyMappings(newMappings);
        }
        if (propertyMappingInput instanceof String propertyMapping) {
            return fromObject(singletonMap(propertyMapping, propertyMapping), aggregation);
        } else if (propertyMappingInput instanceof List<?> inputList) {
            List<PropertyMapping> newMappings = new ArrayList<>();
            for (Object mapping : inputList) {
                List<PropertyMapping> propertyMappings = fromObject(mapping, aggregation).mappings();
                for (PropertyMapping propertyMapping : propertyMappings) {
                    if (newMappings.contains(propertyMapping)) {
                        throw new IllegalStateException(formatWithLocale(
                            "Duplicate property key `%s`",
                            propertyMapping.internalPropertyKey()
                        ));
                    }
                    newMappings.add(propertyMapping);
                }
            }
            return new PropertyMappings(applyAggregation(newMappings, aggregation));
        } else if (propertyMappingInput instanceof Map) {
            List<PropertyMapping> newMappings = new ArrayList<>();
            ((Map<String, Object>) propertyMappingInput).forEach((key, spec) -> {
                PropertyMapping propertyMapping = PropertyMapping.fromObject(key, spec);
                newMappings.add(propertyMapping);
            });
            return new PropertyMappings(applyAggregation(newMappings, aggregation));
        } else {
            throw new IllegalArgumentException(formatWithLocale(
                "Expected String or Map for property mappings. Got %s.",
                propertyMappingInput.getClass().getSimpleName()
            ));
        }
    }

    private static List<PropertyMapping> applyAggregation(List<PropertyMapping> mappings, Aggregation aggregation) {
        List<PropertyMapping> newMappings = new ArrayList<>(mappings.size());
        if (aggregation != Aggregation.DEFAULT && mappings != null) {
            for (PropertyMapping mapping : mappings) {
                newMappings.add(mapping.setNonDefaultAggregation(aggregation));
            }
        } else {
            newMappings.addAll(mappings);
        }
        return newMappings;
    }

    public static Map<String, Object> toObject(PropertyMappings propertyMappings) {
        return propertyMappings.toObject(true);
    }

    public Set<String> propertyKeys() {
        return stream().map(PropertyMapping::internalPropertyKey).collect(Collectors.toSet());
    }

    public Stream<PropertyMapping> stream() {
        return mappings().stream();
    }

    @Override
    public Iterator<PropertyMapping> iterator() {
        return mappings().iterator();
    }

    public boolean hasMappings() {
        return count() > 0;
    }

    public boolean isEmpty() {
        return mappings().isEmpty();
    }

    public Map<String, Object> toObject(boolean includeAggregation) {
        return stream()
            .map(mapping -> mapping.toObject(includeAggregation))
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (u, v) -> {throw new IllegalStateException(formatWithLocale("Duplicate key %s", u));},
                LinkedHashMap::new
            ));
    }

    public PropertyMappings mergeWith(PropertyMappings other) {
        if (!hasMappings()) {
            return other;
        }
        if (!other.hasMappings()) {
            var newMappings = new ArrayList<>(mappings);
            return new PropertyMappings(newMappings);
        }
        // TODO: what about aggregation in the merge case? what if they have different aggregations?
        var newMappings = Stream.concat(mappings().stream(), other.mappings().stream()).toList();
        return new PropertyMappings(newMappings);
    }

    private void checkForAggregationMixing(List<PropertyMapping> mappings) {
        long noneStrategyCount = mappings.stream()
            .filter(d -> d.aggregation() == Aggregation.NONE)
            .count();

        if (noneStrategyCount > 0 && noneStrategyCount < mappings.size()) {
            throw new IllegalArgumentException(
                "Conflicting relationship property aggregations, it is not allowed to mix `NONE` with aggregations.");
        }
    }
}
