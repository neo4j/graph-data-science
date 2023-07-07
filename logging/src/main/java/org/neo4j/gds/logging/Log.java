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
package org.neo4j.gds.logging;

/**
 * We have our own log interface. This enables us to evolve the interface, to run without Neo4j,
 * to not have dependencies on Neo4j everywhere.
 * <p>
 * As a start we mimic the name and methods from Neo4j in order to make migration easy;
 * you could imagine a big search+replace of the import statement, for example.
 * Later it might make sense to rename to GDSLog to make the conceptual split more obvious.
 */
public interface Log {
    void info(String message);

    void info(String format, Object... arguments);

    void warn(String message, Exception e);

    void warn(String format, Object... arguments);

    boolean isDebugEnabled();
}
