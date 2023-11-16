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
package org.neo4j.gds.core.io.file.csv;

import org.intellij.lang.annotations.Subst;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.nodeproperties.ValueType;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class DefaultValueIOHelperTest {

    private static Stream<Arguments> defaultValuesAndSerializedFormat() {
        return Stream.of(
            Arguments.of(DefaultValue.of(42.0, ValueType.DOUBLE, true), "DefaultValue(42.0)", ValueType.DOUBLE),
            Arguments.of(DefaultValue.of(1337, ValueType.LONG, true), "DefaultValue(1337)", ValueType.LONG),
            Arguments.of(DefaultValue.of(new Float[]{ 0.1f, 0.2f }, ValueType.FLOAT_ARRAY, true), "DefaultValue([0.1,0.2])", ValueType.FLOAT_ARRAY),
            Arguments.of(DefaultValue.of(new Double[]{ 1.1, 2.2, 3.3 }, ValueType.DOUBLE_ARRAY, true), "DefaultValue([1.1,2.2,3.3])", ValueType.DOUBLE_ARRAY),
            Arguments.of(DefaultValue.of(new Long[]{ 42L, 43L }, ValueType.LONG_ARRAY, true), "DefaultValue([42,43])", ValueType.LONG_ARRAY)
        );
    }

    @ParameterizedTest
    @MethodSource("defaultValuesAndSerializedFormat")
    void shouldSerializeDefaultValues(DefaultValue defaultValue, String expected, ValueType __) {
        assertThat(DefaultValueIOHelper.serialize(defaultValue)).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = { "DefaultValue(%s)", "%s" })
    void shouldDeserializedDefaultValues(@Subst("") String defaultValueTemplate) {
        assertThat(DefaultValueIOHelper
            .deserialize(formatWithLocale(defaultValueTemplate, "[42,43]"), ValueType.LONG_ARRAY, true)
            .longArrayValue()
        ).contains(42, 43);

        assertThat(DefaultValueIOHelper
            .deserialize(formatWithLocale(defaultValueTemplate, "[42.0,13.37]"), ValueType.FLOAT_ARRAY, true)
            .floatArrayValue()
        ).contains(42.0f, 13.37f);

        assertThat(DefaultValueIOHelper
            .deserialize(formatWithLocale(defaultValueTemplate, "[42.0,13.37]"), ValueType.DOUBLE_ARRAY, true)
            .doubleArrayValue()
        ).contains(42.0D, 13.37D);

        assertThat(DefaultValueIOHelper
            .deserialize(formatWithLocale(defaultValueTemplate, "42.0"), ValueType.DOUBLE, true)
            .doubleValue()
        ).isEqualTo(42.0D);

        assertThat(DefaultValueIOHelper
            .deserialize(formatWithLocale(defaultValueTemplate, "1337"), ValueType.LONG, true)
            .longValue()
        ).isEqualTo(1337L);
    }
}
