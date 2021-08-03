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

import java.util.function.Function;

@FunctionalInterface
public interface CheckedFunction<T, R, E extends Exception> extends Function<T, R> {

    static <T, R, E extends Exception> CheckedFunction<T, R, E> function(CheckedFunction<T, R, E> function) {
        return function;
    }

    @Override
    default R apply(T t) {
        try {
            return checkedApply(t);
        } catch (Exception e) {
            ExceptionUtil.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    R checkedApply(T t) throws E;
}
