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
package org.neo4j.gds.kcore;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.Optional;

final class KCoreCompanion {
    private KCoreCompanion() {}

    static NodePropertyValues nodePropertyValues(Optional<KCoreDecompositionResult> result) {
        return result
            .map(KCoreDecompositionResult::coreValues)
            .map(IntNodeProperties::new)
            .orElseGet(() -> new IntNodeProperties(HugeIntArray.newArray(0)));
    }

    private static final class IntNodeProperties implements NodePropertyValues {

        private final HugeIntArray array;

        private IntNodeProperties(HugeIntArray array) {this.array = array;}

        @Override
        public ValueType valueType() {
            return ValueType.LONG;
        }

        @Override
        public @Nullable Object getObject(long nodeId) {
            return array.get(nodeId);
        }

        @Override
        public Value value(long nodeId) {
            return Values.longValue(array.get(nodeId));
        }

        @Override
        public long nodeCount() {
            return array.size();
        }

        @Override
        public long longValue(long nodeId) {
            return array.get(nodeId);
        }

        @Override
        public Optional<Integer> dimension() {
            return Optional.empty();
        }
    }

}
