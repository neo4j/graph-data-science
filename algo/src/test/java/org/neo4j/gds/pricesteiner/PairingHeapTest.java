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
package org.neo4j.gds.pricesteiner;

import com.carrotsearch.hppc.ObjectArrayList;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PairingHeapTest {

    @Test
    void shouldPopAndAddEvents(){

        PairingHeap heap1 = new PairingHeap(new ObjectArrayList<>());
        heap1.add(1,10);
        heap1.add(2,20);
        heap1.add(3,30);
        heap1.add(4,40);
        assertThat(heap1.minValue()).isEqualTo(10);
        heap1.pop();
        assertThat(heap1.minValue()).isEqualTo(20);
        heap1.pop();
        assertThat(heap1.minValue()).isEqualTo(30);
        heap1.pop();
        assertThat(heap1.minValue()).isEqualTo(40);
        heap1.add(5,5);
        assertThat(heap1.minValue()).isEqualTo(5);
        heap1.pop();
        assertThat(heap1.minValue()).isEqualTo(40);
    }

    @Test
    void shouldBehaveLikeAQueue(){
        PairingHeap heap = new PairingHeap(new ObjectArrayList<>());
        assertThat(heap.empty()).isTrue();

        heap.add(1,10);
        assertThat(heap.empty()).isFalse();

        heap.add(2,11);
        assertThat(heap.minValue()).isEqualTo(10);
        assertThat(heap.minElement()).isEqualTo(1);

        heap.add(3,9);
        assertThat(heap.minValue()).isEqualTo(9);
        assertThat(heap.minElement()).isEqualTo(3);

        heap.pop();
        assertThat(heap.minValue()).isEqualTo(10);
        assertThat(heap.minElement()).isEqualTo(1);

        heap.pop();
        assertThat(heap.minValue()).isEqualTo(11);
        assertThat(heap.minElement()).isEqualTo(2);

        heap.pop();
        assertThat(heap.empty()).isTrue();


    }
    @Test
    void shouldMergeQueues(){
        PairingHeap heap1 = new PairingHeap(new ObjectArrayList<>());
        PairingHeap heap2 = new PairingHeap(new ObjectArrayList<>());


        heap1.add(0,0);
        heap1.add(2,2);
        heap1.add(4,4);

        heap2.add(1,1);
        heap2.add(3,3);
        heap2.add(5,5);

        heap2.join(heap1);

        for (int i=0;i<=5;++i){
            assertThat(heap2.minValue()).isEqualTo(i);
            assertThat(heap2.minElement()).isEqualTo(i);
            heap2.pop();
        }
        assertThat(heap2.empty()).isTrue();
    }

    @Test
    void shouldWorkWithOffsets(){
        PairingHeap heap1 = new PairingHeap(new ObjectArrayList<>());
        heap1.add(1,10);
        heap1.add(2,20);
        heap1.add(3,30);
        heap1.add(4,40);
        heap1.increaseValues(1);
        assertThat(heap1.minValue()).isEqualTo(11);
        heap1.pop();
        heap1.increaseValues(1);
        assertThat(heap1.minValue()).isEqualTo(22);
        heap1.pop();
        heap1.increaseValues(1);
        assertThat(heap1.minValue()).isEqualTo(33);
        heap1.pop();
        heap1.increaseValues(1);
        assertThat(heap1.minValue()).isEqualTo(44);

    }

    @Test
    void shouldWorkWithOffsetsOnMeldedHeaps(){
        ObjectArrayList<PairingHeapElement> helpingArray = new ObjectArrayList<>();

        PairingHeap heap1 = new PairingHeap(helpingArray);
        heap1.add(1,10);
        heap1.add(2,30);
        heap1.add(3,50);
        heap1.add(4,70);
       heap1.increaseValues(1);

        PairingHeap heap2 = new PairingHeap(helpingArray);
        heap2.add(1,20);
        heap2.add(2,40);
        heap2.add(3,60);
        heap2.add(4,80);
        heap2.increaseValues(2);

        heap2.join(heap1);

        assertThat(heap2.minValue()).isEqualTo(11); heap2.pop();
        assertThat(heap2.minValue()).isEqualTo(22); heap2.pop();
        assertThat(heap2.minValue()).isEqualTo(31); heap2.pop();
        assertThat(heap2.minValue()).isEqualTo(42); heap2.pop();
        assertThat(heap2.minValue()).isEqualTo(51); heap2.pop();
        assertThat(heap2.minValue()).isEqualTo(62); heap2.pop();
        assertThat(heap2.minValue()).isEqualTo(71); heap2.pop();
        assertThat(heap2.minValue()).isEqualTo(82);

    }


    @Test
    void shouldWorkWithOffsetsOnManyMeldedHeaps(){
        ObjectArrayList<PairingHeapElement> helpingArray = new ObjectArrayList<>();

        PairingHeap heap1 = new PairingHeap(helpingArray);
        heap1.add(1,10);
        heap1.add(2,30);
        heap1.add(3,50);
        heap1.add(4,70);
        heap1.increaseValues(1);

        PairingHeap heap2 = new PairingHeap(helpingArray);
        heap2.add(1,20);
        heap2.add(2,40);
        heap2.add(3,60);
        heap2.add(4,80);
        heap2.increaseValues(2);

        heap2.join(heap1);

        heap2.increaseValues(1);

        PairingHeap heap3 = new PairingHeap(helpingArray);
        heap3.add(1,0);
        heap3.add(2,200);
        heap3.add(3,300);
        heap3.add(4,400);
        heap3.increaseValues(4);
        heap3.join(heap2);

        assertThat(heap3.minValue()).isEqualTo(4); heap3.pop();
        assertThat(heap3.minValue()).isEqualTo(12); heap3.pop();
        assertThat(heap3.minValue()).isEqualTo(23); heap3.pop();
        assertThat(heap3.minValue()).isEqualTo(32); heap3.pop();
        assertThat(heap3.minValue()).isEqualTo(43); heap3.pop();
        assertThat(heap3.minValue()).isEqualTo(52); heap3.pop();
        assertThat(heap3.minValue()).isEqualTo(63); heap3.pop();
        assertThat(heap3.minValue()).isEqualTo(72); heap3.pop();
        assertThat(heap3.minValue()).isEqualTo(83); heap3.pop();
        assertThat(heap3.minValue()).isEqualTo(204); heap3.pop();
        assertThat(heap3.minValue()).isEqualTo(304); heap3.pop();
        assertThat(heap3.minValue()).isEqualTo(404);

    }

}
