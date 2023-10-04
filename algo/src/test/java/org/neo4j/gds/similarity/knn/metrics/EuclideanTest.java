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
package org.neo4j.gds.similarity.knn.metrics;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EuclideanTest {

    @Test
    void doubleArrays() {
        var left = new double[] {0.008706967509313894d, 0.0d, 0.004919643078839634d, 0.007029592654453866d, 0.0d, 0.0d, 0.1257682851778286d, 0.016530303738513153d, 0.012745441170181648d, 0.0d};
        var right = new double[] {0.008706967509313894d, 0.0d, 0.0041533258820686736d, 0.003698167604532474d, 0.0d, 0.0d, 0.14438303839785882d, 0.0017742178454893931d, 0.008013536500447603d, 0.0d};

        var metric = Euclidean.doubleMetric(left, right);

        assertThat(metric).isCloseTo(0.976123304363789d, Offset.offset(1e-5));
    }

    @Test
    void floatArrays() {
        var left = new float[] {0.008706967509313894f, 0.0f, 0.004919643078839634f, 0.007029592654453866f, 0.0f, 0.0f, 0.1257682851778286f, 0.016530303738513153f, 0.012745441170181648f, 0.0f};
        var right = new float[] {0.008706967509313894f, 0.0f, 0.0041533258820686736f, 0.003698167604532474f, 0.0f, 0.0f, 0.14438303839785882f, 0.0017742178454893931f, 0.008013536500447603f, 0.0f};

        var metric = Euclidean.floatMetric(left, right);

        assertThat(metric).isCloseTo(0.976123304363789d, Offset.offset(1e-5));
    }
}
