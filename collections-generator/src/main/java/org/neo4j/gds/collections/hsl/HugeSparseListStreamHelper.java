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
package org.neo4j.gds.collections.hsl;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;

import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

final class HugeSparseListStreamHelper {

    private HugeSparseListStreamHelper() {}

    static TypeName getStreamTypeName(TypeName valueType) {
        if (valueType == TypeName.LONG) {
            return ClassName.get(LongStream.class);
        }
        if (valueType == TypeName.INT) {
            return ClassName.get(IntStream.class);
        }
        if (valueType == TypeName.DOUBLE) {
            return ClassName.get(DoubleStream.class);
        }
        return ParameterizedTypeName.get(ClassName.get(Stream.class), valueType);
    }

    static String flatMapFunction(TypeName valueType) {
        if (valueType == TypeName.LONG) {
            return "flatMapToLong";
        }
        if (valueType == TypeName.INT) {
            return "flatMapToInt";
        }
        if (valueType == TypeName.DOUBLE) {
            return "flatMapToDouble";
        }
        return "flatMap";
    }
}
