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
package org.neo4j.gds.ml.metrics.classification;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.collections.ha.HugeIntArray;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;

import static org.assertj.core.api.Assertions.assertThat;

class F1MacroTest {

    private HugeIntArray targets;
    private HugeIntArray predictions;

    private LocalIdMap localIdMap;

    @BeforeEach
    void setup() {
        predictions = HugeIntArray.of(3, 4, 6, 6, 7, 9, 8, 1, 1, 2, 3, 3, 3, 4, 4);
        targets = HugeIntArray.of(4, 4, 5, 5, 5, 8, 9, 1, 1, 2, 2, 3, 3, 4, 5);
        localIdMap = new LocalIdMap();
        for (long target : targets.toArray()) {
            localIdMap.toMapped(target);
        }
    }

    @Test
    void shouldComputeF1AllCorrectMultiple() {
        var totalF1 = 1.0 + 2.0/3.0 + 2.0/3.0 + 2.0/3.0;
        var totalClasses = 7;
        assertThat(new F1Macro(localIdMap).compute(targets, predictions))
            .isCloseTo(totalF1 / totalClasses, Offset.offset(1e-8));
    }
}
