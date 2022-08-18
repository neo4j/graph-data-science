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
package org.neo4j.gds.configuration;

final class DefaultsAndLimitsConstants {
    /**
     * Neo4j's procedure framework has a notion of default values, and for us it means users can just leave out a
     * parameter in a procedure invocation.
     *
     * The form that it takes is, we _have to_ specify a default value - no nulls.
     *
     * So this is the unlikely-to-clash default value for the optional username when setting defaults.
     */
    static final String DummyUsername = "d81eb72e-c499-4f78-90c7-0c76123606a2";

    private DefaultsAndLimitsConstants() {}
}
