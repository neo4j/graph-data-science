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
package org.neo4j.gds.core.utils.io.file;

import java.util.List;

import static org.neo4j.gds.core.utils.io.file.csv.CsvNodeSchemaVisitor.DEFAULT_VALUE_COLUMN_NAME;
import static org.neo4j.gds.core.utils.io.file.csv.CsvNodeSchemaVisitor.LABEL_COLUMN_NAME;
import static org.neo4j.gds.core.utils.io.file.csv.CsvNodeSchemaVisitor.PROPERTY_KEY_COLUMN_NAME;
import static org.neo4j.gds.core.utils.io.file.csv.CsvNodeSchemaVisitor.STATE_COLUMN_NAME;
import static org.neo4j.gds.core.utils.io.file.csv.CsvNodeSchemaVisitor.VALUE_TYPE_COLUMN_NAME;

public final class CsvSchemaConstants {

    public static final List<String> NODE_SCHEMA_COLUMNS = List.of(
        LABEL_COLUMN_NAME,
        PROPERTY_KEY_COLUMN_NAME,
        VALUE_TYPE_COLUMN_NAME,
        DEFAULT_VALUE_COLUMN_NAME,
        STATE_COLUMN_NAME
    );

    public static final List<String> GRAPH_PROPERTY_SCHEMA_COLUMNS = List.of(
        PROPERTY_KEY_COLUMN_NAME,
        VALUE_TYPE_COLUMN_NAME,
        DEFAULT_VALUE_COLUMN_NAME,
        STATE_COLUMN_NAME
    );

    private CsvSchemaConstants() {}
}
