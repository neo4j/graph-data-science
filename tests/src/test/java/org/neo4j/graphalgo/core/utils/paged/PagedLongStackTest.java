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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

import java.util.stream.IntStream;

import static org.hamcrest.Matchers.is;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public final class PagedLongStackTest extends RandomizedTest {

    @Rule
    public ErrorCollector collector = new ErrorCollector();

    @Test
    public void shouldCreateEmptyStack() {
        PagedLongStack stack = new PagedLongStack(between(0L, 10L), AllocationTracker.EMPTY);
        assertEmpty(stack);
    }

    @Test
    public void shouldPopValuesInLIFOOrder() {
        PagedLongStack stack = new PagedLongStack(between(0L, 10L), AllocationTracker.EMPTY);
        long values[] = IntStream.range(0, between(1, 42))
                .mapToLong(i -> between(42L, 1337L))
                .toArray();

        for (long value : values) {
            stack.push(value);
        }

        for (int i = values.length - 1; i >= 0; i--) {
            long value = values[i];
            long actual = stack.pop();
            collector.checkThat("Mismatch at index " + i, actual, is(value));
        }
        assertEmpty(stack);
    }

    @Test
    public void shouldPeekLastAddedValue() {
        PagedLongStack stack = new PagedLongStack(between(0L, 10L), AllocationTracker.EMPTY);
        int repetitions = between(1, 42);
        for (int i = 0; i < repetitions; i++) {
            long value = between(0L, 1337L);
            stack.push(value);
            long actual = stack.peek();
            collector.checkThat("Mismatch at index " + i, actual, is(value));
        }
    }

    @Test
    public void shouldClearToAnEmptyStack() {
        PagedLongStack stack = new PagedLongStack(between(0L, 10L), AllocationTracker.EMPTY);
        IntStream.range(0, between(13, 37))
                .mapToLong(i -> between(42L, 1337L))
                .forEach(stack::push);
        stack.clear();
        assertEmpty(stack);
    }

    @Test
    public void shouldGrowAsNecessary() {
        PagedLongStack stack = new PagedLongStack(0L, AllocationTracker.EMPTY);
        // something large enough to spill over one page
        int valuesToAdd = between(10_000, 20_000);
        long[] values = IntStream.range(0, valuesToAdd)
                .mapToLong(i -> between(42L, 1337L))
                .toArray();
        for (long value : values) {
            stack.push(value);
        }
        collector.checkThat(stack.size(), is((long) valuesToAdd));
        for (int i = values.length - 1; i >= 0; i--) {
            long value = values[i];
            long actual = stack.pop();
            collector.checkThat("Mismatch at index " + i, actual, is(value));
        }
    }

    @Test
    public void shouldReleaseMemory() {
        int valuesToAdd = between(10_000, 20_000);
        AllocationTracker tracker = AllocationTracker.create();
        PagedLongStack stack = new PagedLongStack(valuesToAdd, tracker);
        long tracked = tracker.tracked();
        collector.checkThat(stack.release(), is(tracked));
        collector.checkThat("released stack is empty", stack.isEmpty(), is(true));
        collector.checkThat("released stack has size 0", stack.size(), is(0L));
        try {
            stack.pop();
            collector.addError(new AssertionError("pop on released stack shouldn't succeed"));
        } catch (NullPointerException ignored) {
        }
        try {
            stack.peek();
            collector.addError(new AssertionError("pop on released stack shouldn't succeed"));
        } catch (NullPointerException ignored) {
        }
    }

    private void assertEmpty(final PagedLongStack stack) {
        collector.checkThat("empty stack is empty", stack.isEmpty(), is(true));
        collector.checkThat("empty stack has size 0", stack.size(), is(0L));
        try {
            stack.pop();
            collector.addError(new AssertionError("pop on empty stack shouldn't succeed"));
        } catch (ArrayIndexOutOfBoundsException e) {
            collector.checkThat(e.getMessage(), Matchers.containsString("-1"));
        }
        try {
            stack.peek();
            collector.addError(new AssertionError("pop on empty stack shouldn't succeed"));
        } catch (ArrayIndexOutOfBoundsException e) {
            collector.checkThat(e.getMessage(), Matchers.containsString("-1"));
        }
    }
}
