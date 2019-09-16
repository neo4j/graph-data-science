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

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;

public final class PropertyMappings implements Iterable<PropertyMapping> {

    private final PropertyMapping[] mappings;

    public static PropertyMappings of(PropertyMapping... mappings) {
        if (mappings == null) {
            mappings = new PropertyMapping[0];
        }
        return new PropertyMappings(mappings);
    }

    private PropertyMappings(PropertyMapping... mappings) {
        this.mappings = mappings;
    }

    public Stream<PropertyMapping> stream() {
        return Arrays.stream(mappings);
    }

    public Stream<Pair<Integer, PropertyMapping>> enumerate() {
        return IntStream.range(0, mappings.length)
                .mapToObj(idx -> Pair.of(idx, mappings[idx]));
    }

    @Override
    public Iterator<PropertyMapping> iterator() {
        return stream().iterator();
    }

    @Deprecated
    public int weightId() {
        return mappings.length == 0 ? NO_SUCH_PROPERTY_KEY : mappings[0].propertyKeyId();
    }

    @Deprecated
    public double defaultWeight() {
        return mappings.length == 0 ? 0.0 : mappings[0].defaultValue();
    }

    public boolean hasMappings() {
        return mappings.length > 0;
    }

    public int numberOfMappings() {
        return mappings.length;
    }

    public boolean atLeastOneExists() {
        return stream().anyMatch(PropertyMapping::exists);
    }

    public int[] allPropertyKeyIds() {
        return stream()
                .mapToInt(PropertyMapping::propertyKeyId)
                .toArray();
    }

    public double[] allDefaultWeights() {
        return stream()
                .mapToDouble(PropertyMapping::defaultValue)
                .toArray();
    }

    public static final class Builder {
        private final List<PropertyMapping> mappings;

        public Builder() {
            mappings = new ArrayList<>();
        }

        public Builder addMapping(PropertyMapping mapping) {
            Objects.requireNonNull(mapping, "mapping");
            mappings.add(mapping);
            return this;
        }

        public Builder addOptionalMapping(PropertyMapping mapping) {
            Objects.requireNonNull(mapping, "mapping");
            if (mapping.hasValidName()) {
                mappings.add(mapping);
            }
            return this;
        }

        public Builder addAllMappings(PropertyMapping... propertyMappings) {
            addAllMappings(Arrays.stream(propertyMappings));
            return this;
        }

        public Builder addAllOptionalMappings(PropertyMapping... propertyMappings) {
            addAllOptionalMappings(Arrays.stream(propertyMappings));
            return this;
        }

        public Builder addAllMappings(Stream<PropertyMapping> propertyMappings) {
            propertyMappings.forEach(this::addMapping);
            return this;
        }

        public Builder addAllOptionalMappings(Stream<PropertyMapping> propertyMappings) {
            propertyMappings.forEach(this::addOptionalMapping);
            return this;
        }

        public PropertyMappings build() {
            PropertyMapping[] mappings = this.mappings.toArray(new PropertyMapping[0]);
            return of(mappings);
        }
    }
}
