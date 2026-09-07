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

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.values.GdsValue;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.BiConsumer;

/**
 * A {@link PropertyValues} implementation backed by one fixed array of property keys
 * and a parallel array of value slots. It is meant for import paths where the set of
 * property keys is known upfront, e.g. from a schema, so that keys can be shared and
 * only the value slots are provided per record.
 */
public final class ArrayPropertyValues implements PropertyValues {

    private final String[] keys;
    private final GdsValue[] values;

    public ArrayPropertyValues(String[] keys, GdsValue[] values) {
        if (keys.length != values.length) {
            throw new IllegalArgumentException(
                "The number of property keys (" + keys.length +
                    ") must match the number of property values (" + values.length + ")."
            );
        }
        this.keys = keys;
        this.values = values;
    }

    @Override
    public void forEach(BiConsumer<String, GdsValue> consumer) {
        for (int i = 0; i < keys.length; i++) {
            var value = values[i];
            if (value != null) {
                consumer.accept(keys[i], value);
            }
        }
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public int size() {
        int presentValues = 0;
        for (GdsValue value : values) {
            if (value != null) {
                presentValues++;
            }
        }
        return presentValues;
    }

    @Override
    public Iterable<String> propertyKeys() {
        return () -> new Iterator<>() {
            private int index = nextPresentIndex(0);

            @Override
            public boolean hasNext() {
                return index < keys.length;
            }

            @Override
            public String next() {
                if (index >= keys.length) {
                    throw new NoSuchElementException();
                }
                var key = keys[index];
                index = nextPresentIndex(index + 1);
                return key;
            }

            private int nextPresentIndex(int from) {
                while (from < keys.length && values[from] == null) {
                    from++;
                }
                return from;
            }
        };
    }

    @Override
    public @Nullable GdsValue get(String key) {
        for (int i = 0; i < keys.length; i++) {
            if (keys[i].equals(key)) {
                return values[i];
            }
        }
        return null;
    }

    @Override
    public GdsValue getSingle() {
        if (keys.length == 1) {
            return values[0];
        }
        for (GdsValue value : values) {
            if (value != null) {
                return value;
            }
        }
        throw new NoSuchElementException("There is no property value present.");
    }
}
