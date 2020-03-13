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
package org.neo4j.graphalgo.functions;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

public class IsFiniteFunc {

    @UserFunction("gds.util.isFinite")
    @Description("CALL gds.util.isFinite(value) - Return true iff the given argument is a finite value (not ±Infinity, NaN, or null).")
    public boolean isFinite(@Name(value = "value") Number value) {
        return value != null && Double.isFinite(value.doubleValue());
    }

    @UserFunction("gds.util.isInfinite")
    @Description("CALL gds.util.isInfinite(value) - Return true iff the given argument is not a finite value (not ±Infinity, NaN, or null).")
    public boolean isInfinite(@Name(value = "value") Number value) {
        return value == null || !Double.isFinite(value.doubleValue());
    }

    @UserFunction("gds.util.infinity")
    @Description("CALL gds.util.infinity() - Return infinity as a Cypher value.")
    public double Infinity() {
        return Double.POSITIVE_INFINITY;
    }

    @UserFunction("gds.util.NaN")
    @Description("CALL gds.util.NaN() - Returns NaN as a Cypher value.")
    public double NaN() {
        return Double.NaN;
    }
}
