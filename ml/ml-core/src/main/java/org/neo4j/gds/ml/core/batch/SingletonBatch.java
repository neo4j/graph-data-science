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
package org.neo4j.gds.ml.core.batch;

import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;

public class SingletonBatch implements Batch {
    private final long id;

    public SingletonBatch(long id) {this.id = id;}

    @Override
    public PrimitiveIterator.OfLong elementIds() {
        return new PrimitiveIterator.OfLong() {
            private boolean hasNext = true;

            @Override
            public boolean hasNext() {
                return hasNext;
            }

            @Override
            public long nextLong() {
                if (!hasNext) {
                    throw new NoSuchElementException();
                }

                hasNext = false;
                return id;
            }
        };
    }

    @Override
    public int size() {
        return 1;
    }
}
