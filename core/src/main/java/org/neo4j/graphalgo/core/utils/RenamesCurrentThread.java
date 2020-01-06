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
package org.neo4j.graphalgo.core.utils;

public interface RenamesCurrentThread {

    // override AutoCloseable to remove checked throws declaration
    interface Revert extends AutoCloseable {
        @Override
        void close();
    }

    default String threadName() {
        return getClass().getSimpleName() + "-" + System.identityHashCode(this);
    }

    static Revert renameThread(final String newThreadName) {
        Thread currentThread = Thread.currentThread();
        String oldThreadName = currentThread.getName();

        boolean renamed = false;
        if (!oldThreadName.equals(newThreadName)) {
            try {
                currentThread.setName(newThreadName);
                renamed = true;
            } catch (SecurityException e) {
                // failed to rename thread, proceed as usual
            }
        }

        if (renamed) {
            return () -> currentThread.setName(oldThreadName);
        }
        return EMPTY;
    }

    Revert EMPTY = () -> {};
}
