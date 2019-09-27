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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;

public final class PrivateLookup {

    private static final Lookup LOOKUP;

    static {
        try {
            Field implLookup = Lookup.class.getDeclaredField("IMPL_LOOKUP");
            implLookup.setAccessible(true);
            LOOKUP = (Lookup) implLookup.get(null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new Error(e);
        }
    }

    public static Lookup lookup() {
        return LOOKUP;
    }

    public static <T, U> MethodHandle field(
            Class<T> parentClass,
            Class<U> fieldClass,
            String name) {
        try {
            return LOOKUP.findGetter(
                    parentClass,
                    name,
                    fieldClass);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new Error(e);
        }
    }

    public static <T> MethodHandle method(
            Class<T> cls,
            String name,
            MethodType mt) {
        try {
            return LOOKUP.findVirtual(cls, name, mt);
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new Error(e);
        }
    }

    private PrivateLookup() {
    }
}
