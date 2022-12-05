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
package org.neo4j.gds.projection;

import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.BooleanArray;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.ByteArray;
import org.neo4j.values.storable.ByteValue;
import org.neo4j.values.storable.CharArray;
import org.neo4j.values.storable.CharValue;
import org.neo4j.values.storable.DateArray;
import org.neo4j.values.storable.DateTimeArray;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DoubleArray;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.DurationArray;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.FloatArray;
import org.neo4j.values.storable.FloatValue;
import org.neo4j.values.storable.FloatingPointArray;
import org.neo4j.values.storable.FloatingPointValue;
import org.neo4j.values.storable.IntArray;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.IntegralArray;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.LocalDateTimeArray;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeArray;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.LongArray;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.NoValue;
import org.neo4j.values.storable.NumberArray;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointArray;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.ShortArray;
import org.neo4j.values.storable.ShortValue;
import org.neo4j.values.storable.StringArray;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeArray;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualPathValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

interface PartialValueMapper<R> extends ValueMapper<R> {
    R unsupported(AnyValue value);

    @Override
    default R mapPath(VirtualPathValue value) {
        return unsupported(value);
    }

    @Override
    default R mapNode(VirtualNodeValue value) {
        return unsupported(value);
    }

    @Override
    default R mapRelationship(VirtualRelationshipValue value) {
        return unsupported(value);
    }

    @Override
    default R mapMap(MapValue value) {
        return unsupported(value);
    }

    @Override
    default R mapNoValue() {
        return unsupported(NoValue.NO_VALUE);
    }

    @Override
    R mapSequence(SequenceValue value);

    @Override
    default R mapText(TextValue value) {
        return unsupported(value);
    }

    @Override
    default R mapBoolean(BooleanValue value) {
        return unsupported(value);
    }

    @Override
    default R mapNumber(NumberValue value) {
        return unsupported(value);
    }

    @Override
    default R mapDateTime(DateTimeValue value) {
        return unsupported(value);
    }

    @Override
    default R mapLocalDateTime(LocalDateTimeValue value) {
        return unsupported(value);
    }

    @Override
    default R mapDate(DateValue value) {
        return unsupported(value);
    }

    @Override
    default R mapTime(TimeValue value) {
        return unsupported(value);
    }

    @Override
    default R mapLocalTime(LocalTimeValue value) {
        return unsupported(value);
    }

    @Override
    default R mapDuration(DurationValue value) {
        return unsupported(value);
    }

    @Override
    default R mapPoint(PointValue value) {
        return unsupported(value);
    }

    @Override
    default R mapString(StringValue value) {
        return unsupported(value);
    }

    @Override
    default R mapTextArray(TextArray value) {
        return unsupported(value);
    }

    @Override
    default R mapStringArray(StringArray value) {
        return unsupported(value);
    }

    @Override
    default R mapChar(CharValue value) {
        return unsupported(value);
    }

    @Override
    default R mapCharArray(CharArray value) {
        return unsupported(value);
    }

    @Override
    default R mapBooleanArray(BooleanArray value) {
        return unsupported(value);
    }

    @Override
    default R mapNumberArray(NumberArray value) {
        return unsupported(value);
    }

    @Override
    default R mapIntegral(IntegralValue value) {
        return unsupported(value);
    }

    @Override
    default R mapIntegralArray(IntegralArray value) {
        return unsupported(value);
    }

    @Override
    default R mapByte(ByteValue value) {
        return unsupported(value);
    }

    @Override
    default R mapByteArray(ByteArray value) {
        return unsupported(value);
    }

    @Override
    default R mapShort(ShortValue value) {
        return unsupported(value);
    }

    @Override
    default R mapShortArray(ShortArray value) {
        return unsupported(value);
    }

    @Override
    default R mapInt(IntValue value) {
        return unsupported(value);
    }

    @Override
    default R mapIntArray(IntArray value) {
        return unsupported(value);
    }

    @Override
    default R mapLong(LongValue value) {
        return unsupported(value);
    }

    @Override
    default R mapLongArray(LongArray value) {
        return unsupported(value);
    }

    @Override
    default R mapFloatingPoint(FloatingPointValue value) {
        return unsupported(value);
    }

    @Override
    default R mapFloatingPointArray(FloatingPointArray value) {
        return unsupported(value);
    }

    @Override
    default R mapDouble(DoubleValue value) {
        return unsupported(value);
    }

    @Override
    default R mapDoubleArray(DoubleArray value) {
        return unsupported(value);
    }

    @Override
    default R mapFloat(FloatValue value) {
        return unsupported(value);
    }

    @Override
    default R mapFloatArray(FloatArray value) {
        return unsupported(value);
    }

    @Override
    default R mapPointArray(PointArray value) {
        return unsupported(value);
    }

    @Override
    default R mapDateTimeArray(DateTimeArray value) {
        return unsupported(value);
    }

    @Override
    default R mapLocalDateTimeArray(LocalDateTimeArray value) {
        return unsupported(value);
    }

    @Override
    default R mapLocalTimeArray(LocalTimeArray value) {
        return unsupported(value);
    }

    @Override
    default R mapTimeArray(TimeArray value) {
        return unsupported(value);
    }

    @Override
    default R mapDateArray(DateArray value) {
        return unsupported(value);
    }

    @Override
    default R mapDurationArray(DurationArray value) {
        return unsupported(value);
    }
}
