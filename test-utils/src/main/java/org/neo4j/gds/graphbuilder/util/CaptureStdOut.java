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
package org.neo4j.gds.graphbuilder.util;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.annotation.SuppressForbidden;
import org.neo4j.gds.annotation.ValueClass;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

public final class CaptureStdOut {

    private CaptureStdOut() {}

    @SuppressForbidden(reason = "We capture System.out here")
    public static <T> Output<T> run(Supplier<T> task) {
        var stdout = new ByteArrayOutputStream(8192);
        var stderr = new ByteArrayOutputStream(8192);
        var originalOut = System.out;
        var originalErr = System.err;

        try {

            System.setOut(new PrintStream(stdout, true, StandardCharsets.UTF_8));
            System.setErr(new PrintStream(stderr, true, StandardCharsets.UTF_8));

            var output = task.get();
            return ImmutableOutput.<T>builder()
                .stdout(stdout.toString(StandardCharsets.UTF_8).strip())
                .stderr(stderr.toString(StandardCharsets.UTF_8).strip())
                .output(output)
                .build();
        } finally {
            System.setErr(originalErr);
            System.setOut(originalOut);
        }
    }

    @ValueClass
    public interface Output<T> {
        String stdout();

        String stderr();

        @Nullable T output();
    }
}
