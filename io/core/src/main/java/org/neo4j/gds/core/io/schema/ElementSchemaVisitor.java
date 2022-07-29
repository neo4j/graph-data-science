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
package org.neo4j.gds.core.io.schema;

import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.PropertySchema;

public abstract class ElementSchemaVisitor extends InputSchemaVisitor.Adapter implements PropertySchema {

    String key;
    ValueType valueType;
    DefaultValue defaultValue;
    PropertyState state;

    protected abstract void export();

    @Override
    public String key() {
        return key;
    }

    @Override
    public ValueType valueType() {
        return valueType;
    }

    @Override
    public DefaultValue defaultValue() {
        return defaultValue;
    }

    @Override
    public PropertyState state() {
        return state;
    }

    @Override
    public boolean key(String key) {
        this.key = key;
        return true;
    }

    @Override
    public boolean valueType(ValueType valueType) {
        this.valueType = valueType;
        return true;
    }

    @Override
    public boolean defaultValue(DefaultValue defaultValue) {
        this.defaultValue = defaultValue;
        return true;
    }

    @Override
    public boolean state(PropertyState state) {
        this.state = state;
        return true;
    }

    @Override
    public void endOfEntity() {
        export();
        reset();
    }

    protected void reset() {
        key(null);
        valueType(null);
        defaultValue(null);
        state(null);
    }
}
