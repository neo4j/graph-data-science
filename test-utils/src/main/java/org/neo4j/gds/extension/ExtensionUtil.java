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
package org.neo4j.gds.extension;

import org.junit.jupiter.api.extension.ExtensionConfigurationException;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Locale;

public final class ExtensionUtil {

    private ExtensionUtil() {}

    static void setField(Object testInstance, Field field, Object db) {
        field.setAccessible(true);
        try {
            field.set(testInstance, db);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    static String getStringValueOfField(Field field) {
        if (field.getType() != String.class) {
            throw new ExtensionConfigurationException(String.format(
                Locale.ENGLISH,
                "Field %s.%s must be of type %s.",
                field.getDeclaringClass().getTypeName(),
                field.getName(),
                String.class.getTypeName()
            ));
        }

        // read field value
        if (!Modifier.isStatic(field.getModifiers())) {
            throw new ExtensionConfigurationException(String.format(
                Locale.ENGLISH,
                "Field %s.%s must be static.",
                field.getDeclaringClass().getTypeName(),
                field.getName()
            ));
        }

        field.setAccessible(true);

        String value;
        try {
            value = field.get(null).toString();
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        return value;
    }
}
