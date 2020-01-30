/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.core.utils.Intersections;
import org.neo4j.graphalgo.impl.similarity.RleDecoder;
import org.neo4j.graphalgo.impl.similarity.Weights;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms8g", "-Xmx8g", "-XX:+UseG1GC"})
@Warmup(iterations = 1000)
@Measurement(iterations = 1000, time = 2)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class PearsonSimilarityBenchmark {

    public static final int SIZE = 10_000;

    private static double[] initial = generate(SIZE,-42);
    private static double[][] data = data(100);

    private static double[] initialRle = generateRle(SIZE,-42);
    private static double[][] dataRle = dataRle(100);

    private static double[][] data(int size) {
        double[][] result = new double[size][SIZE];
        for (int i=0;i<size;i++) {
            result[i] = generate(SIZE,i);
        }
        return result;
    }

    private static double[][] dataRle(int size) {
        double[][] result = new double[size][SIZE];
        for (int i=0;i<size;i++) {
            result[i] = generateRle(SIZE,i);
        }
        return result;
    }

    private static double[] generate(int size, long seed) {
        double[] result = new double[size];
        double increment = 2*Math.PI/360.0;
        double angle=new Random(seed).nextDouble()*10;
        for (int i=0;i<size;i++) {
            result[i] = Math.sin(angle);
            angle  += increment;
        }
        return result;
    }

    private static double[] generateRle(int size, long seed) {
        List<Number> values = new ArrayList<>(size);
        double[] result = new double[size];
        double increment = 2*Math.PI/360.0;
        double angle=new Random(seed).nextDouble()*10;
        for (int i=0;i<size;i++) {
            values.add(i, Math.sin(angle));
            angle  += increment;
        }

        return Weights.buildRleWeights(values, 3);
    }

    @Benchmark
    public void pearsonSquares(Blackhole bh) throws Exception {
        for (double[] datum : data) {
            bh.consume(Intersections.pearson(initial, datum,SIZE));
        }
    }

    @Benchmark
    public void pearsonSquaresSkip(Blackhole bh) throws Exception {
        for (double[] datum : data) {
            bh.consume(Intersections.pearsonSkip(initial, datum,SIZE, 0));
        }
    }

    @Benchmark
    public void pearsonSquaresRle(Blackhole bh) throws Exception {
        RleDecoder rleDecoder = new RleDecoder(SIZE);
        for (double[] datum : dataRle) {
            rleDecoder.reset(initialRle, datum);
            double[] initialVector = rleDecoder.item1();
            double[] vector = rleDecoder.item2();
            bh.consume(Intersections.pearson(initialVector, vector, SIZE));
        }
    }

    @Setup
    public void setup() throws IOException {
    }

    @TearDown
    public void shutdown() {
    }
}
