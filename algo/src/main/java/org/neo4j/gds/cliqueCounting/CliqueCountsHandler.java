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
package org.neo4j.gds.cliqueCounting;

import java.util.ArrayList;
import java.util.List;


class CliqueCountsHandler {
    private final long nodeCount;
    List<SizeFrequencies> free;

    CliqueCountsHandler(long nodeCount) {
        this.nodeCount = nodeCount;
        this.free = new ArrayList<>();
    }

    public SizeFrequencies createAtIdx(int idx) {
        synchronized (this) {
            while (free.size() <= idx) {
                free.add(new SizeFrequencies(nodeCount));
            }
            return this.free.get(idx);
        }
    }

    SizeFrequencies takeOrCreate() {
        synchronized (this) {
            return free.isEmpty() ? new SizeFrequencies(nodeCount) : free.removeLast();
        }
    }

    void giveBack(SizeFrequencies sizeFrequencies) {
        synchronized (this) {
            free.add(sizeFrequencies);
        }
    }

    public SizeFrequencies merge() {
        SizeFrequencies sizeFrequencies = free.removeLast();
        while (!free.isEmpty()) {
            sizeFrequencies.merge(free.removeLast());
        }
        return sizeFrequencies;
    }
}
