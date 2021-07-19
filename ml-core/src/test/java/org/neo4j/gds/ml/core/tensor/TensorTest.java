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
package org.neo4j.gds.ml.core.tensor;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.core.Dimensions;

import static org.assertj.core.api.Assertions.assertThat;

class TensorTest {

    @Test
    void elementWiseProduct() {
        var dimensions = Dimensions.matrix(2, 2);
        var a = TensorFactory.constant(2, dimensions);

        assertThat(a.elementwiseProduct(a))
            .isNotSameAs(a)
            .isEqualTo(TensorFactory.constant(4, dimensions));
    }

    @Test
    void elementWiseProductMutate() {
        var dimensions = Dimensions.matrix(2, 2);
        var a = TensorFactory.constant(2, dimensions);

        assertThat(a.elementwiseProductMutate(a))
            .isSameAs(a)
            .isEqualTo(TensorFactory.constant(4, dimensions));
    }

}
