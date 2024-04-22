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
package org.neo4j.gds.mem;

import org.neo4j.gds.annotation.SuppressForbidden;
import org.openjdk.jol.info.GraphWalker;
import org.openjdk.jol.vm.VM;

import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.OptionalLong;

public final class MemoryUsage {

    private MemoryUsage() {}

    public static long sizeOf(Object thing) {
        if (!VmInfoHolder.VM_INFO_AVAILABLE) {
            return -1L;
        }

        try {
            return new GraphWalker().walk(thing).totalSize();
        } catch (RuntimeException e) {
            return -1;
        }
    }

    public static OptionalLong sizeOfObject(Object thing) {
        long size = sizeOf(thing);
        return size == -1 ? OptionalLong.empty() : OptionalLong.of(size);
    }

    // nested class so we initialize the VM object lazily when we actually need it
    private static final class VmInfoHolder {

        private static final boolean VM_INFO_AVAILABLE = isVmInfoAvailable();

        /*
         * Try to initialize JOL without having it print warnings or throw errors because
         * we run on an unsupported VM
         */
        @SuppressForbidden(reason = "we want to use system.out here")
        private static boolean isVmInfoAvailable() {
            macWorkaround();

            var sysOut = System.out;
            try {
                var swallowSysOut = new PrintStream(NullOutputStream.NULL_OUTPUT_STREAM, true, StandardCharsets.UTF_8);
                System.setOut(swallowSysOut);
                VM.current();
                swallowSysOut.flush();
                return true;
            } catch (Exception unavailable) {
                return false;
            } finally {
                System.setOut(sysOut);
            }
        }

        /**
         * JOL currently kills the JVM on Mac OS X when trying to attach to the Hotspot SA.
         * This happens with several JVMs (Java.net, Azul) and on both platforms (x86, ARM).
         * <p>
         * We can work around this by skipping the Hotspot SA attach.
         * See <a href="https://bugs.openjdk.org/browse/CODETOOLS-7903447">OpenJDK issue</a>
         * </p>
         */
        @Deprecated(forRemoval = true)
        private static void macWorkaround() {
            if (System.getProperty("os.name").contains("Mac")) {
                System.setProperty("jol.skipHotspotSAAttach", "true");
            }
        }

        private static final class NullOutputStream extends OutputStream {

            static final OutputStream NULL_OUTPUT_STREAM = new NullOutputStream();

            private NullOutputStream() {
            }

            @Override
            public void write(int b) {
                //nothing
            }

            @Override
            public void write(byte[] b) {
                // nothing
            }

            @Override
            public void write(byte[] b, int off, int len) {
                // nothing
            }

        }
    }
}
