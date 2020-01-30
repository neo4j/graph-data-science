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

import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.logging.FormattedLog;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.management.ManagementFactory;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

abstract class BaseMain {

    private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();

    private static final MethodType STRING_TO_VOID = MethodType.methodType(void.class, String.class);
    private static final MethodType THROWABLE_TO_THROWABLE = MethodType.methodType(Throwable.class, Throwable.class);
    private static final MethodType STRING_THROWABLE_TO_VOID = MethodType.methodType(void.class, String.class, Throwable.class);

    private static final boolean HAS_PROFILER;
    private static final MethodHandle ADD_BOOKMARK;

    static {
        boolean hasProfiler = false;
        MethodHandle addBookmark = MethodHandles.constant(Void.class, null);
        try {
            Class.forName("com.jprofiler.agent.ControllerImpl");
            Class<?> apiClass = Class.forName("com.jprofiler.api.agent.Controller");
            addBookmark = LOOKUP.findStatic(
                    apiClass,
                    "addBookmark",
                    MethodType.methodType(void.class, String.class));
            hasProfiler = true;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException ignored) {
        }
        HAS_PROFILER = hasProfiler;
        ADD_BOOKMARK = addBookmark;
    }

    abstract void init(Collection<String> args);

    abstract Iterable<String> run(String graphToLoad, Log log) throws Throwable;

    static void run(final Class<? extends BaseMain> cls, final String... args) throws Throwable {
        String jvmArgs = ManagementFactory
                .getRuntimeMXBean()
                .getInputArguments()
                .stream()
                .filter(s -> s.startsWith("-X"))
                .collect(Collectors.joining(" "));

        Log log = FormattedLog
                .withLogLevel(Level.DEBUG)
                .withZoneId(ZoneId.systemDefault())
                .withDateTimeFormatter(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                .toOutputStream(System.out);
        log.info("Started with JVM args %s", jvmArgs);

        Collection<String> argv = new HashSet<>(Arrays.asList(args));
        boolean skipBarrier = argv.remove("-skipBarrier");

        AtomicReference<String> graphConfig = new AtomicReference<>("L10");
        argv.removeIf(arg -> {
            if (arg.startsWith("-db=")) {
                String dbToLoad = arg.split("=")[1].trim();
                graphConfig.set(dbToLoad);
                return true;
            }
            return false;
        });
        String graphToLoad = graphConfig.get();

        BaseMain main = cls.getDeclaredConstructor().newInstance();
        main.init(argv);

        if (!skipBarrier) {
            System.out.println("Press [Enter] to start");
            @SuppressWarnings("unused") int read = System.in.read();
            System.out.println("Starting...");
        }

        System.gc();

        Iterable<String> messages = Collections.emptyList();

        try {
            messages = main.run(graphToLoad, log);
        } finally {
            log.info("before shutdown");
            System.gc();

            jprofBookmark("shutdown");
            Pools.DEFAULT.shutdownNow();
            System.gc();

            for (String message : messages) {
                log.info(message);
            }
        }
    }

    static void jprofBookmark(final String start) {
        if (HAS_PROFILER) {
            try {
                ADD_BOOKMARK.invokeExact(start);
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        }
    }

    static List<Throwable> flattenSuppressed(Exception e) {
        final Stream<Throwable> subExceptions = flattenSuppressed0(e);
        return Stream.concat(Stream.of(e), subExceptions).map(BaseMain::cleanException).collect(Collectors.toList());
    }

    private static Throwable cleanException(Throwable e) {
        final Throwable[] suppressed = e.getSuppressed();
        if (suppressed == null || suppressed.length == 0) {
            return e;
        }

        MethodHandle constructor = constructor(e);
        try {
            Throwable newInstance = (Throwable) constructor.invoke(e.getMessage(), e.getCause());
            newInstance.setStackTrace(e.getStackTrace());
            return newInstance;
        } catch (Throwable e1) {
            throw toRE(e1, e);
        }
    }

    private static MethodHandle constructor(Throwable e) {
        try {
            /* MH form:
             * Throwable construct(String message, Throwable cause) {
             *   return <new e.getClass()>(message, cause);
             * } */
            return LOOKUP.findConstructor(e.getClass(), STRING_THROWABLE_TO_VOID);
        } catch (NoSuchMethodException | IllegalAccessException ignored) {
        }
        try {
            /* MH form:
             * Throwable construct(String message, Throwable cause) {
             *   Throwable exception = <new e.getClass()>(message);
             *   return exception.initCause(cause)
             * } */
            MethodHandle constructor = LOOKUP.findConstructor(e.getClass(), STRING_TO_VOID);
            MethodHandle initCause = LOOKUP.findVirtual(e.getClass(), "initCause", THROWABLE_TO_THROWABLE);
            return MethodHandles.collectArguments(initCause, 0, constructor);
        } catch (NoSuchMethodException | IllegalAccessException ignored) {
        }

        try {
            /* MH form:
             * Throwable construct(String message, Throwable cause) {
             *   return new RuntimeException(message, cause);
             * } */
            return LOOKUP.findConstructor(RuntimeException.class, STRING_THROWABLE_TO_VOID);
        } catch (NoSuchMethodException | IllegalAccessException e1) {
            throw toRE(e1, e);
        }
    }

    private static Stream<Throwable> flattenSuppressed0(Throwable e) {
        Throwable[] suppressed = e.getSuppressed();
        if (suppressed == null) {
            return Stream.empty();
        }
        return Arrays.stream(suppressed).flatMap(BaseMain::flattenSuppressed0);
    }

    private static RuntimeException toRE(Throwable ex, Throwable supressed) {
        RuntimeException re = new RuntimeException(ex);
        if (supressed != null) {
            re.addSuppressed(supressed);
        }
        return re;
    }
}
