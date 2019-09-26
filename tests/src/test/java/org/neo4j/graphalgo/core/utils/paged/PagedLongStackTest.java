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
package org.neo4j.graphalgo.core.utils.paged;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.qala.datagen.RandomValue.between;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class PagedLongStackTest {

    @Test
    void shouldCreateEmptyStack() {
        PagedLongStack stack = new PagedLongStack(between(0L, 10L).Long(), AllocationTracker.EMPTY);
        assertEmpty(stack);
    }

    @Test
    void shouldPopValuesInLIFOOrder() {
        PagedLongStack stack = new PagedLongStack(between(0L, 10L).Long(), AllocationTracker.EMPTY);
        long values[] = IntStream.range(0, between(1, 42).integer())
                .mapToLong(i -> between(42L, 1337L).Long())
                .toArray();

        for (long value : values) {
            stack.push(value);
        }

        assertAll(
                IntStream.iterate(values.length - 1, i -> i - 1)
                .limit(values.length)
                .mapToObj(i -> {
                    System.out.println("i is " + i);
                    long value = values[i];
                    long actual = stack.pop();
                    return () -> assertEquals(actual, value, "Mismatch at index " + i);
                })
        );
        assertEmpty(stack);
    }

    @Test
    void shouldPeekLastAddedValue() {
        PagedLongStack stack = new PagedLongStack(between(0L, 10L).Long(), AllocationTracker.EMPTY);
        int repetitions = between(1, 42).integer();
        List<Executable> assertions = new ArrayList<>();
        for (int i = 0; i < repetitions; i++) {
            long value = between(0L, 1337L).Long();
            stack.push(value);
            long actual = stack.peek();
            final String reason = "Mismatch at index " + i;
            assertions.add(() -> assertThat(reason, actual, is(value)));
        }
        assertAll(assertions);
    }

    @Test
    void shouldClearToAnEmptyStack() {
        PagedLongStack stack = new PagedLongStack(between(0L, 10L).Long(), AllocationTracker.EMPTY);
        IntStream.range(0, between(13, 37).integer())
                .mapToLong(i -> between(42L, 1337L).Long())
                .forEach(stack::push);
        stack.clear();
        assertEmpty(stack);
    }

    @Test
    void shouldGrowAsNecessary() {
        PagedLongStack stack = new PagedLongStack(0L, AllocationTracker.EMPTY);
        // something large enough to spill over one page
        int valuesToAdd = between(10_000, 20_000).integer();
        long[] values = IntStream.range(0, valuesToAdd)
                .mapToLong(i -> between(42L, 1337L).Long())
                .toArray();
        for (long value : values) {
            stack.push(value);
        }
        assertAll(
                Stream.concat(
                        Stream.of(() -> assertEquals((long) valuesToAdd, stack.size())),
                        IntStream.iterate(values.length - 1, i -> i - 1)
                                .limit(values.length - 1)
                                .mapToObj(i -> {
                                    long value = values[i];
                                    long actual = stack.pop();
                                    return () -> assertEquals(actual, value, "Mismatch at index " + i);
                                })

                )
        );
    }

    @Test
    void shouldReleaseMemory() {
        int valuesToAdd = between(10_000, 20_000).integer();
        AllocationTracker tracker = AllocationTracker.create();
        PagedLongStack stack = new PagedLongStack(valuesToAdd, tracker);
        long tracked = tracker.tracked();
        List<Executable> assertions = new ArrayList<>();
        assertions.add(() -> assertEquals(stack.release(), tracked));
        assertions.add(() -> assertTrue(stack.isEmpty(), "released stack is empty"));
        assertions.add(() -> assertEquals(0L, stack.size(), "released stack has size 0"));
        assertions.add(() -> assertThrows(NullPointerException.class, stack::pop, "pop on released stack shouldn't succeed"));
        assertions.add(() -> assertThrows(NullPointerException.class, stack::peek, "peek on released stack shouldn't succeed"));
        assertAll(assertions);
    }

    private void assertEmpty(final PagedLongStack stack) {
        List<Executable> assertions = new ArrayList<>();
        assertions.add(() -> assertTrue(stack.isEmpty(), "empty stack is empty"));
        assertions.add(() -> assertEquals(0L, stack.size(), "empty stack has size 0"));
        assertions.add(() -> assertThrows(ArrayIndexOutOfBoundsException.class, stack::pop, "pop on empty stack shouldn't succeed"));
        assertions.add(() -> assertThrows(ArrayIndexOutOfBoundsException.class, stack::peek, "peek on empty stack shouldn't succeed"));
        assertAll(assertions);
    }
}
