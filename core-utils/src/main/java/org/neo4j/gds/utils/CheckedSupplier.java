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
package org.neo4j.gds.utils;

import java.util.function.Supplier;

@FunctionalInterface
public interface CheckedSupplier<T, E extends Exception> extends Supplier<T> {

    static <T, E extends Exception> CheckedSupplier<T, E> supplier(CheckedSupplier<T, E> supplier) {
        return supplier;
    }

    @Override
    default T get() {
        try {
            return checkedGet();
        } catch (Exception e) {
            ExceptionUtil.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    T checkedGet() throws E;
}
