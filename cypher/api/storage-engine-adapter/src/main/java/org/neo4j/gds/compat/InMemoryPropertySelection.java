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
package org.neo4j.gds.compat;

public interface InMemoryPropertySelection {

    /**
     * @return {@code true} if this selection limits which keys will be selected, otherwise {@code false} if all will be selected.
     */
    boolean isLimited();

    /**
     * @return the number of keys in this selection. If the selection is not limited 1 is returned {@code 1} and with the key {@link org.neo4j.token.api.TokenConstants#ANY_PROPERTY_KEY}
     * from a call to {@code key(0)}.
     */
    int numberOfKeys();

    /**
     * @param index the selection index. A selection can have multiple keys.
     * @return the key for the given selection index.
     */
    int key( int index );

    /**
     * @param key the key to tests whether or not it fits the criteria of this selection.
     * @return {@code true} if the given {@code key} is part of this selection, otherwise {@code false}.
     */
    boolean test( int key );

    /**
     * A hint that the creator of this selection isn't interested in the actual values, only the existence of the keys.
     * @return {@code true} if only keys will be extracted where this selection is used, otherwise {@code false} if also values will be extracted.
     */
    boolean isKeysOnly();

    InMemoryPropertySelection SELECT_ALL = new InMemoryPropertySelection() {
        @Override
        public boolean isLimited() {
            return false;
        }

        @Override
        public int numberOfKeys() {
            return 1;
        }

        @Override
        public int key(int index) {
            return org.neo4j.token.api.TokenConstants.ANY_PROPERTY_KEY;
        }

        @Override
        public boolean test(int key) {
            return true;
        }

        @Override
        public boolean isKeysOnly() {
            return false;
        }
    };
}
