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
package org.neo4j.gds.modularityoptimization;

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

import static org.assertj.core.api.Assertions.assertThat;


class ModularityColorArrayTest {

    @Test
    void shouldComputeCorrectly() {
        HugeLongArray colors = HugeLongArray.of(0, 1, 3, 3, 3, 1, 0, 0, 0);
        BitSet usedColors = new BitSet(colors.size());
        usedColors.set(0);
        usedColors.set(1);
        usedColors.set(3);


        ModularityColorArray modularityColorArray = ModularityColorArray.create(
            colors,
            usedColors
        );

        assertThat(modularityColorArray.numberOfColors()).isEqualTo(3);

        assertThat(modularityColorArray.nodeAtPosition(0)).isEqualTo(0);
        assertThat(modularityColorArray.nodeAtPosition(1)).isEqualTo(6);
        assertThat(modularityColorArray.nodeAtPosition(2)).isEqualTo(7);
        assertThat(modularityColorArray.nodeAtPosition(3)).isEqualTo(8);

        assertThat(modularityColorArray.nodeAtPosition(4)).isEqualTo(1);
        assertThat(modularityColorArray.nodeAtPosition(5)).isEqualTo(5);

        assertThat(modularityColorArray.nodeAtPosition(6)).isEqualTo(2);
        assertThat(modularityColorArray.nodeAtPosition(7)).isEqualTo(3);
        assertThat(modularityColorArray.nodeAtPosition(8)).isEqualTo(4);

        assertThat(modularityColorArray.nextStartingCoordinate(0)).isEqualTo(4);
        assertThat(modularityColorArray.nextStartingCoordinate(4)).isEqualTo(6);
        assertThat(modularityColorArray.nextStartingCoordinate(6)).isEqualTo(9);


    }
}
