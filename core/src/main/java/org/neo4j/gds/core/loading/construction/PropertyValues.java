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
package org.neo4j.gds.core.loading.construction;

import org.neo4j.gds.values.GdsValue;

import java.util.Map;
import java.util.function.BiConsumer;

public interface PropertyValues {

    void forEach(BiConsumer<String, GdsValue> consumer);

    boolean isEmpty();

    int size();

    Iterable<String> propertyKeys();

    GdsValue get(String key);

    /**
     * Returns the value of the single present property.
     * <p>
     * It is only valid to call this when {@link #size()} is exactly one; callers must
     * check the size beforehand.
     */
    GdsValue getSingle();

    static PropertyValues of(Map<String, GdsValue> map) {
        return new NativePropertyValues(map);
    }
}
