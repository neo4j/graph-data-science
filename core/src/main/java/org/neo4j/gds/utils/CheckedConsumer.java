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

import java.util.function.Consumer;

@FunctionalInterface
public interface CheckedConsumer<T, E extends Exception> extends Consumer<T> {

    static <T, E extends Exception> CheckedConsumer<T, E> consumer(CheckedConsumer<T, E> consumer) {
        return consumer;
    }

    @Override
    default void accept(T t) {
        try {
            checkedAccept(t);
        } catch (Exception e) {
            ExceptionUtil.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    void checkedAccept(T t) throws E;
}
