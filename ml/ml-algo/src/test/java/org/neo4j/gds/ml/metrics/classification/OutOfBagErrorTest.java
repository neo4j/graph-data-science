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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ParalleLongPageCreator;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;

import static org.assertj.core.api.Assertions.assertThat;

class OutOfBagErrorTest {

    @Test
    void handleZeroOutOfAnyBagVectors() {
        double outOfBagError = OutOfBagError.evaluate(
            ReadOnlyHugeLongArray.of(HugeLongArray.of(1L)),
            1,
            HugeIntArray.of(0),
            4,
            HugeAtomicLongArray.of(1, ParalleLongPageCreator.identity(1))
        );

        assertThat(outOfBagError).isEqualTo(0D);
    }

}
