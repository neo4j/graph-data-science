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
package org.neo4j.graphalgo.impl.similarity;

import org.junit.Assume;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class TopKConsumerTest {

    private static final Item ITEM1 = new Item(null, 1);
    private static final Item ITEM3 = new Item(null, 3);
    private static final Item ITEM2 = new Item(null, 2);
    private static final Item ITEM4 = new Item(null, 4);
    private static final Item ITEM5 = new Item(null, 5);
    private static final Item ITEM6 = new Item(null, 6);
    private static final Item ITEM7 = new Item(null, 7);


    static class Item implements Comparable<Item> {
        String name;
        int value;

        Item(String name, int value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return value == ((Item) o).value;

        }

        @Override
        public int hashCode() {
            return value;
        }

        @Override
        public int compareTo(Item o) {
            return Integer.compare(o.value,value);
        }
    }
    
    private static final int RUNS = 10000;
    private static final int COUNT = 50000;
    public static final int WINDOW_SIZE = 20;

    @Test
    public void testFindTopKHeap4() throws Exception {
        Collection<Item> topItems = TopKConsumer.topK(asList(ITEM1, ITEM3, ITEM2, ITEM4), 4, Item::compareTo);
        assertEquals(asList(ITEM4,ITEM3,ITEM2,ITEM1),topItems);
        for (Item topItem : topItems) {
            System.out.println(topItem);
        }
    }

    @Test
    public void testFindTopKHeap2of4() throws Exception {
        Collection<Item> topItems = TopKConsumer.topK(asList(ITEM2, ITEM4), 4, Item::compareTo);
        assertEquals(asList(ITEM4,ITEM2),topItems);
        for (Item topItem : topItems) {
            System.out.println(topItem);
        }
    }
    @Test
    public void testFindTopKHeap4of3() throws Exception {
        Collection<Item> topItems = TopKConsumer.topK(asList(ITEM2, ITEM1, ITEM4, ITEM3), 3, Item::compareTo);
        assertEquals(asList(ITEM4,ITEM3,ITEM2),topItems);
        for (Item topItem : topItems) {
            System.out.println(topItem);
        }
    }
    @Test
    public void testFindTopKHeap() throws Exception {
        Collection<Item> topItems = TopKConsumer.topK(asList(ITEM1, ITEM3, ITEM2, ITEM4), 2, Item::compareTo);
        assertEquals(asList(ITEM4,ITEM3),topItems);
        for (Item topItem : topItems) {
            System.out.println(topItem);
        }
    }

    @Test
    public void testFindTopKHeap2() throws Exception {
        List<Item> topItems = TopKConsumer.topK(asList(ITEM1, ITEM3, ITEM2, ITEM4), 2, Item::compareTo);
        assertEquals(asList(ITEM4,ITEM3),topItems);
        for (Item topItem : topItems) {
            System.out.println(topItem);
        }
    }

    @Test
    public void testFindTopKHeapPerf() throws Exception {
        Assume.assumeTrue("benchmark disabled",System.getProperty("neo4j.graphalgo.benchmark")!=null);
        List<Item> items = createItems(COUNT);
        List<Item> topItems = null;
        for (int i = 0; i < RUNS/10; i++) {
            topItems = TopKConsumer.topK(items, WINDOW_SIZE, Item::compareTo);
        }
        long time = System.currentTimeMillis();
        for (int i = 0; i < RUNS; i++) {
            topItems = TopKConsumer.topK(items, WINDOW_SIZE, Item::compareTo);
        }
        time = System.currentTimeMillis() - time;
        System.out.println("array based time = " + time+" "+RUNS+" runs with "+COUNT+" items avg "+1.0*time/RUNS);
        for (Item topItem : topItems) {
            System.out.println(topItem);
        }
    }

    @Test
    public void mergeNoOverlapConsumers() {
        TopKConsumer<Item> rootConsumer = new TopKConsumer<>(3, Item::compareTo);
        TopKConsumer<Item> comparator1 = new TopKConsumer<>(3, Item::compareTo);
        TopKConsumer<Item> comparator2 = new TopKConsumer<>(3, Item::compareTo);

        asList(ITEM4, ITEM5, ITEM7).forEach(comparator1::apply);
        asList(ITEM1, ITEM2, ITEM3).forEach(comparator2::apply);

        rootConsumer.apply(comparator1);
        rootConsumer.apply(comparator2);

        assertThat(rootConsumer.list(), contains(ITEM7, ITEM5, ITEM4));
    }

    @Test
    public void mergeOverlappingConsumers() {
        TopKConsumer<Item> rootConsumer = new TopKConsumer<>(3, Item::compareTo);
        TopKConsumer<Item> comparator1 = new TopKConsumer<>(3, Item::compareTo);
        TopKConsumer<Item> comparator2 = new TopKConsumer<>(3, Item::compareTo);

        asList(ITEM3, ITEM4, ITEM5).forEach(comparator1::apply);
        asList(ITEM1, ITEM6, ITEM7).forEach(comparator2::apply);

        rootConsumer.apply(comparator1);
        rootConsumer.apply(comparator2);

        assertThat(rootConsumer.list(), contains(ITEM7, ITEM6, ITEM5));
    }

    @Test
    public void mergeNotFullConsumer() {
        TopKConsumer<Item> rootConsumer = new TopKConsumer<>(3, Item::compareTo);
        TopKConsumer<Item> comparator1 = new TopKConsumer<>(3, Item::compareTo);
        TopKConsumer<Item> comparator2 = new TopKConsumer<>(3, Item::compareTo);

        asList(ITEM6).forEach(comparator1::apply);
        asList(ITEM3, ITEM4, ITEM5).forEach(comparator2::apply);

        rootConsumer.apply(comparator1);
        rootConsumer.apply(comparator2);

        assertThat(rootConsumer.list(), contains(ITEM6, ITEM5, ITEM4));
    }

    @Test
    public void addingDuplicateItems() {
        TopKConsumer<Item> consumer = new TopKConsumer<>(3, Item::compareTo);
        asList(ITEM3, ITEM3, ITEM3).forEach(consumer::apply);
        assertThat(consumer.list(), contains(ITEM3));
    }

    private List<Item> createItems(int count) {
        List<Item> items = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            items.add(new Item(null, i));
        }
        return items;
    }
}