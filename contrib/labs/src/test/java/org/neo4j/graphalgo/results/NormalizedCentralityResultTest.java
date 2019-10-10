/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.results;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.impl.results.CentralityResult;
import org.neo4j.graphalgo.impl.results.HugeDoubleArrayResult;
import org.neo4j.graphalgo.impl.results.PartitionedDoubleArrayResult;
import org.neo4j.graphalgo.impl.utils.Normalization;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NormalizedCentralityResultTest {

    @Test
    public void maxNormalization() {
        CentralityResult centralityResult = mock(CentralityResult.class);
        when(centralityResult.score(0)).thenReturn(1.0);
        when(centralityResult.score(1)).thenReturn(2.0);
        when(centralityResult.score(2)).thenReturn(3.0);
        when(centralityResult.score(3)).thenReturn(4.0);
        when(centralityResult.computeMax()).thenReturn(4.0);

        CentralityResult normalizedResult = Normalization.MAX.apply(centralityResult);

        assertEquals(0.25, normalizedResult.score(0), 0.01);
        assertEquals(0.5, normalizedResult.score(1), 0.01);
        assertEquals(0.75, normalizedResult.score(2), 0.01);
        assertEquals(1.0, normalizedResult.score(3), 0.01);
    }

    @Test
    public void noNormalization() {
        CentralityResult centralityResult = mock(CentralityResult.class);
        when(centralityResult.score(0)).thenReturn(1.0);
        when(centralityResult.score(1)).thenReturn(2.0);
        when(centralityResult.score(2)).thenReturn(3.0);
        when(centralityResult.score(3)).thenReturn(4.0);
        when(centralityResult.computeMax()).thenReturn(4.0);

        CentralityResult normalizedResult = Normalization.NONE.apply(centralityResult);

        assertEquals(1.0, normalizedResult.score(0), 0.01);
        assertEquals(2.0, normalizedResult.score(1), 0.01);
        assertEquals(3.0, normalizedResult.score(2), 0.01);
        assertEquals(4.0, normalizedResult.score(3), 0.01);
    }

    @Test
    public void l2Norm() {
        CentralityResult centralityResult = mock(CentralityResult.class);
        when(centralityResult.score(0)).thenReturn(1.0);
        when(centralityResult.score(1)).thenReturn(2.0);
        when(centralityResult.score(2)).thenReturn(3.0);
        when(centralityResult.score(3)).thenReturn(4.0);
        when(centralityResult.computeL2Norm()).thenReturn(4.0);

        CentralityResult normalizedResult = Normalization.L2NORM.apply(centralityResult);

        assertEquals(0.25, normalizedResult.score(0), 0.01);
        assertEquals(0.5, normalizedResult.score(1), 0.01);
        assertEquals(0.75, normalizedResult.score(2), 0.01);
        assertEquals(1.0, normalizedResult.score(3), 0.01);
    }

    @Test
    public void l1Norm() {
        CentralityResult centralityResult = mock(CentralityResult.class);
        when(centralityResult.score(0)).thenReturn(1.0);
        when(centralityResult.score(1)).thenReturn(2.0);
        when(centralityResult.score(2)).thenReturn(3.0);
        when(centralityResult.score(3)).thenReturn(4.0);
        when(centralityResult.computeL1Norm()).thenReturn(4.0);

        CentralityResult normalizedResult = Normalization.L1NORM.apply(centralityResult);

        assertEquals(0.25, normalizedResult.score(0), 0.01);
        assertEquals(0.5, normalizedResult.score(1), 0.01);
        assertEquals(0.75, normalizedResult.score(2), 0.01);
        assertEquals(1.0, normalizedResult.score(3), 0.01);
    }

    @Test
    void doubleArrayResultExport() {
        String property = "eigenvector";
        final HugeDoubleArray given = HugeDoubleArray.of(1, 2, 3, 4);
        HugeDoubleArrayResult result = new HugeDoubleArrayResult(given);

        Exporter exporter = mock(Exporter.class);
        ArgumentCaptor<HugeDoubleArrayResult.MapTranslator> arg = ArgumentCaptor
                .forClass(HugeDoubleArrayResult.MapTranslator.class);

        Normalization.MAX.apply(result).export(property, exporter);

        verify(exporter).write(eq(property), eq(given), arg.capture());
        HugeDoubleArrayResult.MapTranslator provided = arg.getValue();

        assertEquals(0.25D, provided.toDouble(given, 0), 1e-4);
        assertEquals(0.5D, provided.toDouble(given, 1), 1e-4);
        assertEquals(0.75D, provided.toDouble(given, 2), 1e-4);
        assertEquals(1.0D, provided.toDouble(given, 3), 1e-4);
    }

    @Test
    void partitionedPrimitiveDoubleArrayResultExport() {
        String property = "eigenvector";
        double[][] partitions = new double[][]{{1.0, 2.0}, {3.0, 4.0}};
        long[] starts = new long[]{0, 2};
        PartitionedDoubleArrayResult result = new PartitionedDoubleArrayResult(partitions, starts);

        Exporter exporter = mock(Exporter.class);
        Normalization.MAX.apply(result).export(property, exporter);

        verify(exporter).write(eq(property), argThat(arrayEq(new double[][]{{0.25, 0.5}, {0.75, 1.0}})), eq(result));
    }

    @Test
    void partitionedDoubleArrayResultExport() {
        String property = "eigenvector";
        double[][] partitions = new double[][]{{1.0, 2.0}, {3.0, 4.0}};
        long[] starts = new long[]{0, 2};
        PartitionedDoubleArrayResult result = new PartitionedDoubleArrayResult(partitions, starts);

        Exporter exporter = mock(Exporter.class);
        Normalization.MAX.apply(result).export(property, exporter);

        verify(exporter).write(eq(property), argThat(arrayEq(new double[][]{{0.25, 0.5}, {0.75, 1.0}})), eq(result));
    }

    private ArrayMatcher arrayEq(double[][] expected) {
        return new ArrayMatcher(expected);
    }

    class ArrayMatcher implements ArgumentMatcher<double[][]> {
        private double[][] expected;

        ArrayMatcher(double[][] expected) {
            this.expected = expected;
        }

        @Override
        public boolean matches(double[][] actual) {
            return Arrays.deepEquals(expected, actual);
        }
    }
}
