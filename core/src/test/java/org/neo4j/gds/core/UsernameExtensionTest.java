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
package org.neo4j.gds.core;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.annotation.SuppressForbidden;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.utils.CheckedRunnable;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

class UsernameExtensionTest {

    @Test
    void shouldNotInvokeCompatLayerJustBecauseTheExtensionIsRunning() throws Exception {
        CheckedRunnable<Exception> runnable = () -> {
            var dbms = new TestDatabaseManagementServiceBuilder()
                .impermanent()
                .noOpSystemGraphInitializer()
                // make sure that we 1) have our extension under test and 2) have it only once
                .removeExtensions(ex -> ex instanceof UsernameExtension)
                .addExtension(new UsernameExtension())
                .build();
            var db = dbms.database(DEFAULT_DATABASE_NAME);
            var ignored = db.executeTransactionally("CALL dbms.procedures()", Map.of(), Result::resultAsString);
            dbms.shutdown();
        };
        assertThat(runCapturingStdOut(runnable)).isEmpty();
    }

    @Test
    void shouldNotInvokeCompatLayerWhenUsernameIsContextInjected() throws Exception {
        CheckedRunnable<Exception> runnable = () -> {
            var dbms = new TestDatabaseManagementServiceBuilder()
                .impermanent()
                .noOpSystemGraphInitializer()
                // make sure that we 1) have our extension under test and 2) have it only once
                .removeExtensions(ex -> ex instanceof UsernameExtension)
                .addExtension(new UsernameExtension())
                .build();
            var db = dbms.database(DEFAULT_DATABASE_NAME);
            GraphDatabaseApiProxy.registerProcedures(db, UsernameProc.class);
            var ignored = db.executeTransactionally("CALL dbms.procedures()", Map.of(), Result::resultAsString);
            dbms.shutdown();
        };
        assertThat(runCapturingStdOut(runnable)).isEmpty();
    }

    @Test
    void shouldInvokeCompatLayerWhenTheUsernameIsInteractedWith() throws Exception {
        // This test checks that we initialize the Neo4jProxy in the procedure invocation
        // Since we have no control over test execution, it might be that the class is already initialized.
        // There is also no good way of checking if a class is initialized without also initializing it.
        // So we will do the not-so-good way for now

        // UnsafeUtil does not expose the actual unsafe (for good reasons)
        var theUnsafeField = UnsafeUtil.class.getDeclaredField("unsafe");
        // This might trigger a warning of illegal reflective access
        theUnsafeField.setAccessible(true);
        var theUnsafe = (sun.misc.Unsafe) theUnsafeField.get(null);

        Assumptions.assumeFalse(theUnsafe == null, "There is no Unsafe available on this platform, skipping test");

        Class<?> neoProxyClass = Class.forName(
            "org.neo4j.gds.compat.Neo4jProxy",
            /* initialize class = */ false,
            getClass().getClassLoader()
        );

        // The unsafe method is (other than being unsafe) deprecated since JDK15 and scheduled for removal with no replacement.
        // The implementation is apparently not thread-safe and one should use `ensureInitialized` in the
        // MethodHandles API, but that does not give us what we want.
        Assumptions.assumeTrue(
            theUnsafe.shouldBeInitialized(neoProxyClass),
            "The Neo4jProxy is already initialized, this test cannot succeed"
        );

        CheckedRunnable<Exception> runnable = () -> {
            var dbms = new TestDatabaseManagementServiceBuilder()
                .impermanent()
                .noOpSystemGraphInitializer()
                // make sure that we 1) have our extension under test and 2) have it only once
                .removeExtensions(ex -> ex instanceof UsernameExtension)
                .addExtension(new UsernameExtension())
                .build();
            var db = dbms.database(DEFAULT_DATABASE_NAME);
            GraphDatabaseApiProxy.registerProcedures(db, UsernameProc.class);
            var ignored = db.executeTransactionally("CALL gds.test.username()", Map.of(), Result::resultAsString);
            dbms.shutdown();
        };
        assertThat(runCapturingStdOut(runnable)).contains(" Loaded compatibility layer:");
    }

    @SuppressForbidden(reason = "System.out is used")
    private <E extends Exception> String runCapturingStdOut(CheckedRunnable<E> runnable) throws E {
        var stdout = new ByteArrayOutputStream(8192);
        var originalOut = System.out;

        try {
            System.setOut(new PrintStream(stdout, true, StandardCharsets.UTF_8));
            runnable.checkedRun();
        } finally {
            System.setOut(originalOut);
        }
        return stdout.toString(StandardCharsets.UTF_8).strip();
    }

    public static class UsernameProc {
        @Context
        public Username username;

        @Procedure(name = "gds.test.username")
        public Stream<UsernameProcResult> run() {
            return Stream.of(new UsernameProcResult(username.username()));
        }
    }

    public static class UsernameProcResult {
        public String username;

        UsernameProcResult(String username) {
            this.username = username;
        }
    }
}
