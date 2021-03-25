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
package org.neo4j.graphalgo.core.utils.export.file;

import com.fasterxml.jackson.databind.util.StdConverter;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;

import java.util.Locale;

public class JacksonConverters {

    static class NodeLabelConverter extends StdConverter<String, NodeLabel> {
        @Override
        public NodeLabel convert(String value) {
            return NodeLabel.of(value);
        }
    }

    static class RelationshipTypeConverter extends StdConverter<String, RelationshipType> {
        @Override
        public RelationshipType convert(String value) {
            return RelationshipType.of(value);
        }
    }

    static class ValueTypeConverter extends StdConverter<String, ValueType> {
        @Override
        public ValueType convert(String value) {
            return ValueType.valueOf(value.toUpperCase(Locale.ENGLISH));
        }
    }

    static class DefaultValueConverter extends StdConverter<String, DefaultValue> {
        @Override
        public DefaultValue convert(String value) {
            return DefaultValue.of(value.replaceAll("DefaultValue\\(|\\)", ""));
        }
    }
}
