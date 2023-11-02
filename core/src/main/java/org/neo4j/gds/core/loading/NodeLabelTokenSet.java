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
package org.neo4j.gds.core.loading;

import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.core.GraphDimensions;

import java.util.Arrays;

public interface NodeLabelTokenSet {

    NodeLabelTokenSet ANY_LABEL = new NodeLabelTokenSet() {
        @Override
        public int length() {
            return 0;
        }

        @Override
        public int get(int index) {
            throw new IndexOutOfBoundsException();
        }

        @Override
        public void ignore(int index) {
            throw new IndexOutOfBoundsException();
        }

        @Override
        public int[] asIntArray() {
            return new int[0];
        }
    };

    static NodeLabelTokenSet from(int... tokens) {
        return new IntNodeLabelTokenSet(tokens);
    }

    static NodeLabelTokenSet from(long... tokens) {
        return new LongNodeLabelTokenSet(tokens);
    }

    int length();

    int get(int index);

    void ignore(int index);

    @TestOnly
    int[] asIntArray();

    class IntNodeLabelTokenSet implements NodeLabelTokenSet {
        private final int[] tokens;

        public IntNodeLabelTokenSet(int[] tokens) {
            this.tokens = tokens;
        }

        @Override
        public int length() {
            return tokens.length;
        }

        @Override
        public int get(int index) {
            return tokens[index];
        }

        @Override
        public void ignore(int index) {
            tokens[index] = GraphDimensions.IGNORE;
        }

        @Override
        public int[] asIntArray() {
            return tokens;
        }
    }

    class LongNodeLabelTokenSet implements NodeLabelTokenSet {
        private final long[] tokens;

        LongNodeLabelTokenSet(long[] tokens) {
            this.tokens = tokens;
        }

        @Override
        public int length() {
            return tokens.length;
        }

        @Override
        public int get(int index) {
            return (int) tokens[index];
        }

        @Override
        public void ignore(int index) {
            tokens[index] = GraphDimensions.IGNORE;
        }

        @Override
        public int[] asIntArray() {
            return Arrays.stream(tokens).mapToInt(Math::toIntExact).toArray();
        }
    }
}
