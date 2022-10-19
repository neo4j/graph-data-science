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
package org.neo4j.gds.compat._51;

import org.neo4j.gds.compat.InMemoryPropertySelection;
import org.neo4j.storageengine.api.PropertySelection;

public class InMemoryPropertySelectionImpl implements InMemoryPropertySelection {

    private final PropertySelection propertySelection;

    public InMemoryPropertySelectionImpl(PropertySelection propertySelection) {this.propertySelection = propertySelection;}

    @Override
    public boolean isLimited() {
        return propertySelection.isLimited();
    }

    @Override
    public int numberOfKeys() {
        return propertySelection.numberOfKeys();
    }

    @Override
    public int key(int index) {
        return propertySelection.key(index);
    }

    @Override
    public boolean test(int key) {
        return propertySelection.test(key);
    }

    @Override
    public boolean isKeysOnly() {
        return propertySelection.isKeysOnly();
    }
}
