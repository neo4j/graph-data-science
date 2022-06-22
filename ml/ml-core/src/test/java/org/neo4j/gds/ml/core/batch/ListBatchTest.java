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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.ml.core.batch.PrimitiveIteratorTestUtil.iteratorToArray;

class ListBatchTest {

    @Test
    void nonEvenBatching() {
        var batch = new ListBatch(new long[] {42L, 43L, 44L});

        assertThat(iteratorToArray(batch.elementIds())).containsExactly(42L, 43L, 44L);
        assertThat(iteratorToArray(batch.elementIds())).containsExactly(42L, 43L, 44L);
    }

}
