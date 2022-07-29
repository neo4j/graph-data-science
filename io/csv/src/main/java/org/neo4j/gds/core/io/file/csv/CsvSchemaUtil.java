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

import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.core.io.file.FileHeader;
import org.neo4j.gds.core.io.file.HeaderProperty;

import java.util.Comparator;
import java.util.Map;

final class CsvSchemaUtil {

    private CsvSchemaUtil() {}

    static <P extends PropertySchema, HEADER extends FileHeader<?, P>> CsvSchema fromElementSchema(
        Map<String, P> propertySchemas,
        HEADER header,
        String... elementColumns
    ) {
        var schemaBuilder = CsvSchema.builder();
        for (String elementColumn : elementColumns) {
            schemaBuilder.addColumn(elementColumn, CsvSchema.ColumnType.NUMBER);
        }

        // We need to construct a csv schema that keeps the correct order
        // of columns. The file header stores column positions that we can
        // leverage to achieve this.
        header
            .propertyMappings()
            .stream()
            .sorted(Comparator.comparingInt(HeaderProperty::position))
            .forEach(headerProperty -> schemaBuilder.addColumn(
                headerProperty.propertyKey(),
                csvTypeFromValueType(propertySchemas.get(headerProperty.propertyKey()).valueType())
            ));

        return schemaBuilder.build();
    }

    static CsvSchema.ColumnType csvTypeFromValueType(ValueType valueType) {
        switch (valueType) {
            case DOUBLE:
            case LONG:
                return CsvSchema.ColumnType.NUMBER;
            case LONG_ARRAY:
            case DOUBLE_ARRAY:
            case FLOAT_ARRAY:
                return CsvSchema.ColumnType.ARRAY;
            default:
                throw new UnsupportedOperationException();
        }
    }
}
