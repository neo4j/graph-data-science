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

public abstract class PropertyValues {

    public abstract void forEach(BiConsumer<String, GdsValue> consumer);

    public abstract boolean isEmpty();

    public abstract int size();

    public abstract Iterable<String> propertyKeys();

    public abstract GdsValue get(String key);

    public static PropertyValues of(Map<String, GdsValue> map) {
        return new NativePropertyValues(map);
    }
}
