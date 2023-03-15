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
package org.neo4j.gds.steiner;

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;


class ReroutingChildrenManagerTest {


    @Test
    void shouldWorkInTree() {
         //         0
         //   1            2        12
         //3    4      5     (6)    13
        //(7) 8  (9)  (10)         (14)  (15)
        //  (11)

        var terminalBitSet=new BitSet(16);
        terminalBitSet.set(6,8);
        terminalBitSet.set(9,12);
        terminalBitSet.set(14,16);

        var manager=new ReroutingChildrenManager(16,terminalBitSet,0);
        manager.link(1,0);
        manager.link(2,0);
        manager.link(3,1);
        manager.link(4,1);
        manager.link(5,2);
        manager.link(6,2);
        manager.link(7,3);
        manager.link(8,3);
        manager.link(9,4);
        manager.link(10,5);
        manager.link(11,8);

        manager.link(12,0);
        manager.link(13,12);
        manager.link(14,13);
        manager.link(15,13);


        assertThat(manager.prunable(0)).isFalse(); // source cannot be pruneds
        assertThat(manager.prunable(1)).isFalse(); // more than one  children
        assertThat(manager.prunable(2)).isFalse(); // more than one children
        assertThat(manager.prunable(3)).isFalse(); //  more than one children
        assertThat(manager.prunable(4)).isTrue(); //  one descendant terminals
        assertThat(manager.prunable(5)).isTrue(); //  one descendant terminals
        assertThat(manager.prunable(6)).isFalse(); //  terminal
        assertThat(manager.prunable(7)).isFalse(); //  terminal
        assertThat(manager.prunable(8)).isTrue(); //  one descendant terminals
        assertThat(manager.prunable(9)).isFalse(); //  terminal
        assertThat(manager.prunable(10)).isFalse(); //  terminal
        assertThat(manager.prunable(11)).isFalse(); //  terminal
        assertThat(manager.prunable(12)).isTrue(); //  two desendant terminals but only one child which would be transferred
        assertThat(manager.prunable(13)).isFalse();  // more than two terminal descendants
        assertThat(manager.prunable(14)).isFalse(); //  terminal
        assertThat(manager.prunable(15)).isFalse(); //  terminal

        manager.cut(12);
        manager.link(13,5);
        assertThat(manager.prunable(5)).isFalse(); //  two descendants now
        manager.cut(14);
        manager.link(14,0);
        assertThat(manager.prunable(13)).isTrue(); //  one descendant
        manager.cut(11);
        manager.cut(8);
        manager.link(11,0);
        assertThat(manager.prunable(3)).isTrue(); //

    }

}
