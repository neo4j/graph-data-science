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
package org.neo4j.gds.values.primitive;

import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.values.IntegralValue;

import java.util.Objects;

public class LongValueImpl implements IntegralValue {
    private final long value;

    public LongValueImpl(long value) {
        this.value = value;
    }

    @Override
    public long longValue() {
        return value;
    }

    @Override
    public ValueType type() {
        return ValueType.LONG;
    }

    @Override
    public Long asObject() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof IntegralValue) {
            IntegralValue that = (IntegralValue) o;
            return value == that.longValue();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }
}
