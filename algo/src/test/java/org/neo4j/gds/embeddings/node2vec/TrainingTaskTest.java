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
package org.neo4j.gds.embeddings.node2vec;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.tensor.FloatVector;

import java.util.List;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TrainingTaskTest {

    @Test
    void shouldComputeGradientForPositive(){
        var task = new TrainingTask(
            null,
            null,
            null,
            null,
            0.5f,
            0,
            3,
            ProgressTracker.NULL_TRACKER
        );
        var center = new FloatVector(new float[]{0f,  1f, 2f});
        var context = new FloatVector(new float[]{0f, 2f, 3f});

        //affinity  = 1*2 + 2*3 = 8
        // pos = 0.99966464987
        // neg = 1-pos = 0.00033535013
        //gradient =  -0.00033535013
        //scaled = -gradient * neg  = 00033535013 * 0.5  = 0.00016767506;
        assertThat(task.computeGradient(center,context,true)).isCloseTo(0.00016767506f, Offset.offset(1e-5f));

    }

    @Test
    void shouldComputeGradientForNegatively(){
        var task = new TrainingTask(
            null,
            null,
            null,
            null,
            0.5f,
            0,
            3,
            ProgressTracker.NULL_TRACKER
        );
        var center = new FloatVector(new float[]{0f,  1f, 2f});
        var context = new FloatVector(new float[]{0f, 2f, 3f});

        //affinity  = 1*2 + 2*3 = 8
        // pos = 0.99966464987
        //gradient =  0.99966464987
        //scaled = -gradient * pos  = 0.99966464987 * 0.5  = -0.49983232493;
        assertThat(task.computeGradient(center,context,false)).isCloseTo(-0.49983232493f, Offset.offset(1e-5f));
    }

    @Test
    void shouldUpdateEmbeddings(){
        var task = new TrainingTask(
            null,
            null,
            null,
            null,
            0.5f,
            0,
            3,
            ProgressTracker.NULL_TRACKER
        );
        var center = new FloatVector(new float[]{0f,  1f, 2f});
        var context = new FloatVector(new float[]{1f, 2f, 3f});

        var centerBuffer = new FloatVector(3);
        var contextBuffer = new FloatVector(3);

        task.updateEmbeddings(center, context,5, centerBuffer, contextBuffer);
        //centerbuffer = [5, 10, 15]
        //contextbuffer= [0, 5, 10 ]
        assertThat(centerBuffer.data()).containsExactly(5f,  10f, 15f);
        assertThat(contextBuffer.data()).containsExactly(0f, 5f,  10f);
        //center = [5,11,17]
        //context = [1,7,13]
        assertThat(center.data()).containsExactly(5f,11f,17f);
        assertThat(context.data()).containsExactly(1f,7f,13f);
    }

    @Test
    void shouldTrainNegatively(){
        HugeObjectArray<FloatVector> centerEmbeddings = HugeObjectArray.newArray(FloatVector.class, 2);
        centerEmbeddings.set(0, new FloatVector(new float[]{0f,1f,2f}));
        centerEmbeddings.set(1, new FloatVector(3));

        HugeObjectArray<FloatVector> contextEmbeddings = HugeObjectArray.newArray(FloatVector.class, 2);
        contextEmbeddings.set(0, new FloatVector(3));
        contextEmbeddings.set(1, new FloatVector(new float[]{0f,2f,3f}));

        var task = new TrainingTask(
            centerEmbeddings,
            contextEmbeddings,
            null,
            null,
            0.5f,
            0,
            3,
            ProgressTracker.NULL_TRACKER
        );

        task.trainSample(0,1,false);

        //centerbuffer = [0, -0.99966464986, -1.49949697479]
        //contextbuffer= [0, -0.4998323249, -0.99966464986 ]

        var center = centerEmbeddings.get(0).data();
        var context = contextEmbeddings.get(1).data();

        assertThat(center[0]).isEqualTo(0f);
        assertThat(center[1]).isCloseTo(0.00033535014f,Offset.offset(1e-5f));
        assertThat(center[2]).isCloseTo(0.50050302521f,Offset.offset(1e-5f));

        assertThat(context[0]).isEqualTo(0f);
        assertThat(context[1]).isCloseTo(1.5001676751f,Offset.offset(1e-5f));
        assertThat(context[2]).isCloseTo(2.00033535014f,Offset.offset(1e-5f));

    }

    @Test
    void shouldTrainPositively(){
        HugeObjectArray<FloatVector> centerEmbeddings = HugeObjectArray.newArray(FloatVector.class, 2);
        centerEmbeddings.set(0, new FloatVector(new float[]{0f,1f,2f}));
        centerEmbeddings.set(1, new FloatVector(3));

        HugeObjectArray<FloatVector> contextEmbeddings = HugeObjectArray.newArray(FloatVector.class, 2);
        contextEmbeddings.set(0, new FloatVector(3));
        contextEmbeddings.set(1, new FloatVector(new float[]{0f,2f,3f}));

        var task = new TrainingTask(
            centerEmbeddings,
            contextEmbeddings,
            null,
            null,
            0.5f,
            0,
            3,
            ProgressTracker.NULL_TRACKER
        );

        task.trainSample(0,1,true);

        //centerbuffer = [0, 0.00033535012, 0.00050302518]
        //contextbuffer= [0, 0.00016767506f, 0.00033535012 ]

        var center = centerEmbeddings.get(0).data();
        var context = contextEmbeddings.get(1).data();

        assertThat(center[0]).isEqualTo(0f);
        assertThat(center[1]).isCloseTo(1.00033535012f,Offset.offset(1e-5f));
        assertThat(center[2]).isCloseTo(2.00050302518f,Offset.offset(1e-5f));

        assertThat(context[0]).isEqualTo(0f);
        assertThat(context[1]).isCloseTo(2.00016767506f,Offset.offset(1e-5f));
        assertThat(context[2]).isCloseTo(3.00033535012f,Offset.offset(1e-5f));
    }

    @Test
    void trainShouldCallRightThings(){
        PositiveSampleProducer positiveSampleProducer = new PositiveSampleProducer(
            List.of(new long[]{0,1,2}, new long[]{0,3,4}).iterator(),
            HugeDoubleArray.of(1d,1d,1d,-11d,1d),
            5,
            42 //seed is irrelevant because probability values is rigged to always pick everything except 3
        );
        //positiveSampleProducer returns

        NegativeSampleProducer negativeSampleProducer = mock(NegativeSampleProducer.class);
        when(negativeSampleProducer.next()).thenReturn(3L);

        HugeObjectArray<FloatVector> centerEmbeddings = HugeObjectArray.newArray(FloatVector.class, 5);
        HugeObjectArray<FloatVector> contextEmbeddings = HugeObjectArray.newArray(FloatVector.class, 5);

        for (int i=0;i<5;++i) {
            centerEmbeddings.set(i, new FloatVector(3));
            contextEmbeddings.set(i, new FloatVector(3));
        }

        var task = spy(new TrainingTask(
            centerEmbeddings,
            contextEmbeddings,
            positiveSampleProducer,
            negativeSampleProducer,
            0.5f,
            42,
            3,
            ProgressTracker.NULL_TRACKER
        ));

        task.run();

        //for each buffer pair: 1 positive call, 42 negative
        verify(task, times(8*43)).trainSample(anyLong(), anyLong(),anyBoolean());

        //negative call check:  0 has three buffer[0] appearances, 4 has one,  1 and 2 two, multiply by 42
        verify(task, times(42*3)).trainSample(
            ArgumentMatchers.same(0L),
            ArgumentMatchers.same(3L),
            ArgumentMatchers.same(false)
        );
        verify(task, times(42*2)).trainSample(
            ArgumentMatchers.same(1L),
            ArgumentMatchers.same(3L),
            ArgumentMatchers.same(false)
        );
        verify(task, times(42*2)).trainSample(
            ArgumentMatchers.same(2L),
            ArgumentMatchers.same(3L),
            ArgumentMatchers.same(false)
        );
        verify(task, times(42)).trainSample(
            ArgumentMatchers.same(4L),
            ArgumentMatchers.same(3L),
            ArgumentMatchers.same(false)
        );
        //positive

        BiConsumer<Long,Long> positiveSamplingVerify = (a,b) -> {
            verify(task, times(1)).trainSample(
                ArgumentMatchers.same(a),
                ArgumentMatchers.same(b),
                ArgumentMatchers.same(true)
            );
            verify(task, times(1)).trainSample(
                ArgumentMatchers.same(b),
                ArgumentMatchers.same(a),
                ArgumentMatchers.same(true)
            );
        };
        positiveSamplingVerify.accept(0L,1L);
        positiveSamplingVerify.accept(0L,2L);
        positiveSamplingVerify.accept(1L,2L);
        positiveSamplingVerify.accept(0L,4L);

    }

    @Test
    void trainE2ETest(){
        PositiveSampleProducer positiveSampleProducer = new PositiveSampleProducer(
            List.of(new long[]{0,1}, new long[]{0,2}).iterator(),
            HugeDoubleArray.of(1d,1d,1d,1d),
            5,
            42 //seed is irrelevant because probability values is rigged to always pick everything except 3
        );

        NegativeSampleProducer negativeSampleProducer = mock(NegativeSampleProducer.class);
        when(negativeSampleProducer.next()).thenReturn(3L);

        HugeObjectArray<FloatVector> centerEmbeddings = HugeObjectArray.newArray(FloatVector.class, 4);
        HugeObjectArray<FloatVector> contextEmbeddings = HugeObjectArray.newArray(FloatVector.class, 4);

        for (int i=0;i<4;++i) {
            centerEmbeddings.set(i, new FloatVector(new float[]{i}));
            contextEmbeddings.set(i, new FloatVector(new float[]{10+i}));
        }

        var task = new TrainingTask(
            centerEmbeddings,
            contextEmbeddings,
            positiveSampleProducer,
            negativeSampleProducer,
            0.5f,
            1,
            1,
            ProgressTracker.NULL_TRACKER
        );

        task.run();

        assertThat(centerEmbeddings.get(0).data()[0]).isCloseTo(-3.3124456f,Offset.offset(1e-5f));
        assertThat(centerEmbeddings.get(1).data()[0]).isCloseTo( -4.812221f,Offset.offset(1e-5f));
        assertThat(centerEmbeddings.get(2).data()[0]).isCloseTo(-2.9999456f,Offset.offset(1e-5f));
        assertThat(centerEmbeddings.get(3).data()[0]).isCloseTo(3.0f,Offset.offset(1e-5f));

        assertThat(contextEmbeddings.get(0).data()[0]).isCloseTo(10.000023f,Offset.offset(1e-5f));
        assertThat(contextEmbeddings.get(1).data()[0]).isCloseTo( 11f,Offset.offset(1e-5f));
        assertThat(contextEmbeddings.get(2).data()[0]).isCloseTo(10.125f,Offset.offset(1e-5f));
        assertThat(contextEmbeddings.get(3).data()[0]).isCloseTo(8.999891f,Offset.offset(1e-5f));

    }

}
