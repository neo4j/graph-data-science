//package org.neo4j.graphalgo.core.heavyweight;
//
//import com.carrotsearch.hppc.sorting.IndirectComparator;
//import com.carrotsearch.hppc.sorting.IndirectSort;
//import org.openjdk.jmh.annotations.Benchmark;
//import org.openjdk.jmh.annotations.BenchmarkMode;
//import org.openjdk.jmh.annotations.Fork;
//import org.openjdk.jmh.annotations.Measurement;
//import org.openjdk.jmh.annotations.Mode;
//import org.openjdk.jmh.annotations.OutputTimeUnit;
//import org.openjdk.jmh.annotations.Threads;
//import org.openjdk.jmh.annotations.Warmup;
//
//import java.util.AbstractMap;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Comparator;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.TimeUnit;
//
//@Threads(1)
//@Fork(value = 1, jvmArgs = {"-Xms8g", "-Xmx8g", "-XX:+UseG1GC"})
//@Warmup(iterations = 5, time = 1)
//@Measurement(iterations = 10, time = 1)
//@BenchmarkMode(Mode.Throughput)
//@OutputTimeUnit(TimeUnit.SECONDS)
//public class IndirectSortBenchmark {
//
//    @Benchmark
//    public SortingData jdk_api(SortingData data) {
//        int size = data.size;
//        IntFloatPair[] pairs = new IntFloatPair[size];
//        Arrays.setAll(pairs, i -> new IntFloatPair(data.values[i], data.sidecar[i]));
//        Arrays.sort(pairs, 0, size);
//        for (int i = 0; i < pairs.length; i++) {
//            IntFloatPair pair = pairs[i];
//            data.values[i] = pair.left;
//            data.sidecar[i] = pair.right;
//        }
//
//        return data;
//    }
//
//    @Benchmark
//    public SortingData jdk_api_boxed(SortingData data) {
//        int size = data.size;
//        List<Map.Entry<Integer, Float>> pairs = new ArrayList<>(size);
//        for (int i = 0; i < size; i++) {
//            pairs.add(new AbstractMap.SimpleEntry<>(data.values[i], data.sidecar[i]));
//        }
//        pairs.sort(Comparator.comparingInt(Map.Entry::getKey));
//        for (int i = 0; i < size; i++) {
//            Map.Entry<Integer, Float> pair = pairs.get(i);
//            data.values[i] = pair.getKey();
//            data.sidecar[i] = pair.getValue();
//        }
//        return data;
//    }
//
//    @Benchmark
//    public SortingData hppcIndirectSort(SortingData data) {
//        int[] values = data.values.clone();
//        float[] sidecar = data.sidecar.clone();
//        int[] order = IndirectSort.mergesort(0, data.size, new IndirectComparator.AscendingIntComparator(values));
//        for (int i = 0; i < order.length; i++) {
//            int pos = order[i];
//            data.values[i] = values[pos];
//            data.sidecar[i] = sidecar[pos];
//        }
//        return data;
//    }
//
//    @Benchmark
//    public SortingData indirectIntSort(SortingData data) {
//        IndirectIntSort.sort(data.values, data.sidecar, new long[data.size], data.size);
//        return data;
//    }
//
//    @Benchmark
//    public SortingData indirectIntSortNoDedup(SortingData data) {
//        IndirectIntSort.sortWithoutDeduplication(data.values, data.sidecar, new long[data.size], data.size);
//        return data;
//    }
//
//    private static final class IntFloatPair implements Comparable<IntFloatPair> {
//        final int left;
//        final float right;
//
//        private IntFloatPair(final int left, final float right) {
//            this.left = left;
//            this.right = right;
//        }
//
//        @Override
//        public int compareTo(final IntFloatPair o) {
//            return Integer.compare(left, o.left);
//        }
//    }
//}
