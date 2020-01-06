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

import org.eclipse.collections.api.tuple.primitive.IntObjectPair;
import org.immutables.value.Value;
import org.neo4j.graphalgo.annotation.DataClass;
import org.neo4j.graphalgo.core.utils.CollectionUtil;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

@DataClass
@Value.Immutable(singleton = true)
public abstract class AbstractResolvedPropertyMappings implements Iterable<ResolvedPropertyMapping> {

    public static ResolvedPropertyMappings empty() {
        return ResolvedPropertyMappings.of();
    }

    public abstract List<ResolvedPropertyMapping> mappings();

    public Stream<ResolvedPropertyMapping> stream() {
        return mappings().stream();
    }

    public Stream<IntObjectPair<ResolvedPropertyMapping>> enumerate() {
        return CollectionUtil.enumerate(mappings());
    }

    @Override
    public Iterator<ResolvedPropertyMapping> iterator() {
        return mappings().iterator();
    }

    public boolean hasMappings() {
        return !mappings().isEmpty();
    }

    public int numberOfMappings() {
        return mappings().size();
    }

    public boolean atLeastOneExists() {
        return stream().anyMatch(ResolvedPropertyMapping::exists);
    }

    public int[] allPropertyKeyIds() {
        return stream()
            .mapToInt(ResolvedPropertyMapping::propertyKeyId)
            .toArray();
    }

    public double[] allDefaultWeights() {
        return stream()
            .mapToDouble(ResolvedPropertyMapping::defaultValue)
            .toArray();
    }
}
