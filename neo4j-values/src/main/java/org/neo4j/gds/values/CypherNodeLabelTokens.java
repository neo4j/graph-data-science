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
package org.neo4j.gds.values;

import org.eclipse.collections.api.block.function.primitive.ObjectIntToObjectFunction;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.core.loading.construction.NodeLabelToken;
import org.neo4j.gds.core.loading.construction.NodeLabelTokens;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;

import java.util.Arrays;
import java.util.Objects;

public class CypherNodeLabelTokens {
    public static NodeLabelToken of(TextValue textValue) {
        return new NodeLabelTokens.Singleton<>(textValue, tv -> NodeLabelTokens.labelOf(tv.stringValue()));
    }

    public static NodeLabelToken of(TextArray textArray) {
        return new Sequence<>(textArray, TextArray::stringValue);
    }

    public static @NotNull NodeLabelToken of(SequenceValue nodeLabels) {
        return new Sequence<>(nodeLabels, (s, i) -> ((TextValue) s.value((long) i)).stringValue());
    }

    private static final class Sequence<T extends SequenceValue> implements NodeLabelTokens.ValidNodeLabelToken {
        private final T sequence;
        private final ObjectIntToObjectFunction<T, String> toString;

        private Sequence(T sequence, ObjectIntToObjectFunction<T, String> toString) {
            this.sequence = sequence;
            this.toString = toString;
        }

        @Override
        public boolean isEmpty() { return sequence.isEmpty(); }

        @Override
        public int size() {
            return sequence.intSize();
        }

        @Override
        public @NotNull NodeLabel get(int index) {
            return new NodeLabel(toString.valueOf(sequence, index));
        }

        @Override
        public String[] getStrings() {
            var result = new String[size()];
            Arrays.setAll(result, i -> toString.valueOf(sequence, i));
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Sequence<?> sequence1 = (Sequence<?>) o;
            return sequence.equals(sequence1.sequence);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sequence);
        }
    }
    private CypherNodeLabelTokens() {}
}
