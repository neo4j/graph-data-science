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
package org.neo4j.gds.core.utils.warnings;

import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.progress.tasks.Task;

import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

class BetterUserLogStoreTest {
    @Test
    void shouldDealWithNoEntries() {
        var userLogStore = new BetterUserLogStore();

        assertThat(userLogStore.query("user 1")).isEmpty();
    }

    @Test
    void shouldStoreAndRetrieveUserLogEntry() {
        var userLogStore = new BetterUserLogStore();

        userLogStore.addUserLogMessage("user 1", new Task("task description 1", null), "log message 1");

        assertThat(userLogStore.query("user 1")).map(mapToSomethingUseful()).containsExactly(
            Pair.of("log message 1", "task description 1")
        );
    }

    @Test
    void shouldStoreAndRetrieveUserLogEntries() {
        var userLogStore = new BetterUserLogStore();

        userLogStore.addUserLogMessage("user 1", new Task("task description 1", null), "log message 01");
        userLogStore.addUserLogMessage("user 1", new Task("task description 2", null), "log message 02");
        userLogStore.addUserLogMessage("user 1", new Task("task description 2", null), "log message 03");
        userLogStore.addUserLogMessage("user 2", new Task("task description 1", null), "log message 04");
        userLogStore.addUserLogMessage("user 3", new Task("task description 1", null), "log message 05");
        userLogStore.addUserLogMessage("user 3", new Task("task description 1", null), "log message 06");

        assertThat(userLogStore.query("user 1")).map(mapToSomethingUseful()).containsExactly(
            Pair.of("log message 01", "task description 1"),
            Pair.of("log message 02", "task description 2"),
            Pair.of("log message 03", "task description 2")
        );

        assertThat(userLogStore.query("user 2")).map(mapToSomethingUseful()).containsExactly(
            Pair.of("log message 04", "task description 1")
        );

        assertThat(userLogStore.query("user 3")).map(mapToSomethingUseful()).containsExactly(
            Pair.of("log message 05", "task description 1"),
            Pair.of("log message 06", "task description 1")
        );

        userLogStore.addUserLogMessage("user 1", new Task("task description 1", null), "log message 07");
        userLogStore.addUserLogMessage("user 1", new Task("task description 2", null), "log message 08");
        userLogStore.addUserLogMessage("user 2", new Task("task description 3", null), "log message 09");
        userLogStore.addUserLogMessage("user 3", new Task("task description 1", null), "log message 10");
        userLogStore.addUserLogMessage("user 3", new Task("task description 3", null), "log message 11");
        userLogStore.addUserLogMessage("user 3", new Task("task description 3", null), "log message 12");

        assertThat(userLogStore.query("user 1")).map(mapToSomethingUseful()).containsExactly(
            Pair.of("log message 01", "task description 1"),
            Pair.of("log message 07", "task description 1"),
            Pair.of("log message 02", "task description 2"),
            Pair.of("log message 03", "task description 2"),
            Pair.of("log message 08", "task description 2")
        );

        assertThat(userLogStore.query("user 2")).map(mapToSomethingUseful()).containsExactly(
            Pair.of("log message 04", "task description 1"),
            Pair.of("log message 09", "task description 3")
        );

        assertThat(userLogStore.query("user 3")).map(mapToSomethingUseful()).containsExactly(
            Pair.of("log message 05", "task description 1"),
            Pair.of("log message 06", "task description 1"),
            Pair.of("log message 10", "task description 1"),
            Pair.of("log message 11", "task description 3"),
            Pair.of("log message 12", "task description 3")
        );
    }

    @NotNull
    private static Function<UserLogEntry, Pair<String, String>> mapToSomethingUseful() {
        return ule -> Pair.of(ule.message, ule.taskName);
    }
}
