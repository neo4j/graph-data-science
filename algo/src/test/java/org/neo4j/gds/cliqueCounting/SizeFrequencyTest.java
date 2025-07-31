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

import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SizeFrequencyTest {

    @Test
    void shouldKeepCountsWithoutOptionals(){

        var sizeFrequency1 = new SizeFrequency();
        var sizeFrequency2 = new SizeFrequency();
        sizeFrequency1.add(4,0);
        assertThat(sizeFrequency1.toLongArray()).containsExactly(0,1);
        sizeFrequency1.add(4,0);
        assertThat(sizeFrequency1.toLongArray()).containsExactly(0,2);
        sizeFrequency1.add(6,0);
        assertThat(sizeFrequency1.toLongArray()).containsExactly(0,2,0,1);
        var list = List.of(sizeFrequency1,sizeFrequency2);
        SizeFrequency.add(4,0,list);
        assertThat(sizeFrequency1.toLongArray()).containsExactly(0,3,0,1);
        assertThat(sizeFrequency2.toLongArray()).containsExactly(0,1);

    }

    @Test
    void shouldKeepCountsWithOptionals(){

        var sizeFrequency = new SizeFrequency();
        sizeFrequency.add(4,2);
        assertThat(sizeFrequency.toLongArray()).containsExactly(0,1,2,1);
        sizeFrequency.add(4,0);
        assertThat(sizeFrequency.toLongArray()).containsExactly(0,2,2,1);
        sizeFrequency.add(6,3);
        assertThat(sizeFrequency.toLongArray()).containsExactly(0,2,2,2,3,3,1);
    }



    @Test
    void shouldMege(){
        var sizeFrequency1 = new SizeFrequency();
        var sizeFrequency2 = new SizeFrequency();
        var sizeFrequency3 = new SizeFrequency();

        sizeFrequency1.data = new BigInteger[]{BigInteger.ONE, BigInteger.TEN, BigInteger.ZERO};
        sizeFrequency2.data = new BigInteger[]{BigInteger.ONE, BigInteger.ZERO, BigInteger.TEN, BigInteger.TEN};
        sizeFrequency3.data = new BigInteger[]{BigInteger.ONE};

        sizeFrequency1.merge(sizeFrequency2);
        assertThat(sizeFrequency1.toLongArray()).containsExactly(2,10,10,10);

        sizeFrequency1.merge(sizeFrequency3);
        assertThat(sizeFrequency1.toLongArray()).containsExactly(3,10,10,10);




    }

}
