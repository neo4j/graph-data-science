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
package org.neo4j.gds.core;

/**
 * When operating GDS, it is very helpful to be able to trace requests accurately.
 * Without a request correlation id, you can sometimes get away with inference.
 * But having this construct allows us to, for example, correlate log messages down the GDS call stack.
 * Imagine you ran several Node2Vec runs concurrently:
 * how would you know which log lines represented output from which request?
 * The properties we need from this id are, it needs to be unique across requests,
 * and renderable as a human-readable string. A UUID would do, for example.
 * But leaving this opaque allows us to _also_ integrate more nicely with existing systems,
 * by wrapping the request id they supply.
 */
public interface RequestCorrelationId {
    /**
     * Please remember to override toString, logging for example will rely on that.
     */
    String toString();
}
