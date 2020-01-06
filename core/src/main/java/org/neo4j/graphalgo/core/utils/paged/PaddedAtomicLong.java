/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils.paged;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @see <a href="http://mechanical-sympathy.blogspot.ch/2011/08/false-sharing-java-7.html">http://mechanical-sympathy.blogspot.ch/2011/08/false-sharing-java-7.html</a>
 */
@SuppressWarnings({"serial", "WeakerAccess"})
public final class PaddedAtomicLong extends AtomicLong {
    public volatile long p1 = 1, p2 = 2, p3 = 3, p4 = 4, p5 = 5, p6 = 6, p7 = 7;

    public long sum() { // prevents optimizing away the fields above
        return p1 + p2 + p3 + p4 + p5 + p6 + p7;
    }

    public PaddedAtomicLong() {
        super();
    }

    public PaddedAtomicLong(long initialValue) {
        super(initialValue);
    }
}
