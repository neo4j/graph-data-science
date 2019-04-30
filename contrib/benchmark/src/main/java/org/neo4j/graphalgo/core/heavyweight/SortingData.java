/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.core.heavyweight;

import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.Random;

@State(Scope.Benchmark)
public class SortingData {

    @Param({"100", "10000"})
    int size;

    int[] values;
    float[] sidecar;

    @Setup
    public void setup() {
        values = createValues(size);
        sidecar = createSidecar(size);
    }

    private static int[] createValues(int size) {
        Random rand = new Random(0);
        int[] array = new int[size];
        for (int i = 0; i < size; ++i) {
            array[i] = randomInt(rand);
        }
        return array;
    }

    private static float[] createSidecar(int size) {
        Random rand = new Random(42);
        float[] array = new float[size];
        for (int i = 0; i < size; ++i) {
            array[i] = randomFloat(rand);
        }
        return array;
    }

    private static int randomInt(Random random) {
        return random.nextInt() & Integer.MAX_VALUE;
    }

    private static float randomFloat(Random random) {
        return (float)(random.nextDouble() * 0x1p53);
    }
}
