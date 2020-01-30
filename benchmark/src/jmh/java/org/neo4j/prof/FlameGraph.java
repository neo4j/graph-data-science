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
package org.neo4j.prof;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.neo4j.graphalgo.helper.ldbc.LdbcDownloader;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.ExternalProfiler;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.profile.ProfilerException;
import org.openjdk.jmh.results.BenchmarkResult;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.runner.IterationType;
import org.openjdk.jmh.util.Utils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Runs the <a href="https://github.com/jvm-profiling-tools/async-profiler">async-profiler</a>
 * alongside the profiled benchmark and produces a flame graph of the profiling result.
 *
 * Run the benchmarks with {@code -prof org.neo4j.prof.FlameGraph} to enable this profiler.
 *
 * You have to <a href="https://github.com/jvm-profiling-tools/async-profiler/releases">download the async-profiler</a>
 * and point the profiler to the profiler directory. This can be done either by setting
 * the ENV variable ASYNC_PROFILER_DIR to the /path/to/async-profiler or by passing the
 * directory to the {@code asyncProfiler} option, e.g. {@code -prof org.neo4j.prof.FlameGraph:asyncProfiler=/path/to/async-profiler}.
 * The profiler must be at least version 1.2 with the added SVG support.
 *
 * Flame graph files are by default put into the {@code $TMP/org.neo4j/bench/$BENCHMARK_CLASS/} folder
 * where {@code $TMP} is defined as the {@link System#getProperty(String) SystemProperty} {@code java.io.tmp}
 * and {@code $BENCHMARK_CLASS} is the unqualified class name of the benchmark under profiling.
 * This directory can be override by providing the {@code dir} option.
 *
 * The file name is derived from the benchmark method and its parameters.
 *
 * Run with {@code -prof org.neo4j.prof.FlameGraph:help} to see all possible options.
 *
 * @see <a href="https://github.com/jvm-profiling-tools/async-profiler">async-profiler</a>
 */
public final class FlameGraph implements InternalProfiler, ExternalProfiler {

    private static final String ASYNC_PROFILER_DIR = "ASYNC_PROFILER_DIR";
    private static final String DEFAULT_EVENT = "cpu";

    private final String event;
    private final Path asyncProfilerDir;
    private final boolean threads;
    private final OptionalLong frameBufferSize;
    private final OptionalLong interval;
    private final Optional<String> title;
    private final Collection<String> profilerArgs;
    private final Collection<Path> generated = new ArrayList<>();
    private Path outputDir;
    private boolean started;
    private int measurementIterationCount;

    public FlameGraph(final String initLine) throws ProfilerException {
        Path asyncDir = null;
        String env = System.getenv(ASYNC_PROFILER_DIR);
        if (env != null) {
            asyncDir = Paths.get(env).toAbsolutePath();
        }

        OptionParser parser = new OptionParser();

        OptionSpec<String> outputDir = parser
                .accepts("dir", "Output directory")
                .withRequiredArg()
                .describedAs("directory")
                .ofType(String.class);

        ArgumentAcceptingOptionSpec<String> asyncProfiler = parser
                .accepts(
                        "asyncProfiler",
                        "Location of https://github.com/jvm-profiling-tools/async-profiler unless provided by $" + ASYNC_PROFILER_DIR)
                .withRequiredArg()
                .ofType(String.class)
                .describedAs("directory");
        if (asyncDir == null) {
            asyncProfiler.required();
        } else {
            asyncProfiler.defaultsTo(asyncDir.toString());
        }

        OptionSpec<String> event = parser
                .accepts("event", "Event to sample: cpu, alloc, lock, cache-misses etc.")
                .withRequiredArg()
                .ofType(String.class)
                .defaultsTo("cpu");
        OptionSpec<Long> frameBufferSize = parser
                .accepts("framebuf", "Size of profiler framebuffer")
                .withRequiredArg()
                .ofType(Long.class);
        OptionSpec<Long> interval = parser
                .accepts("interval", "Profiling interval, in nanoseconds")
                .withRequiredArg()
                .ofType(Long.class);
        OptionSpec<Boolean> threads = parser
                .accepts("threads", "Profile threads separately")
                .withRequiredArg()
                .ofType(Boolean.class)
                .defaultsTo(false, true);
        OptionSpec<String> title = parser
                .accepts("title", "Title of the generated SVG file")
                .withRequiredArg()
                .ofType(String.class);
        OptionSpec<String> profilerArgs = parser
                .accepts("profilerArgs", "Additional arguments that are passed directly to the Profiler, separated by comma (`,`). See https://github.com/jvm-profiling-tools/async-profiler/blob/master/README.md#profiler-options for an overview. Arguments of interest might be -s or --reverse.")
                .withRequiredArg()
                .withValuesSeparatedBy(',')
                .ofType(String.class);

        OptionSet options = parseInitLine(initLine, parser);
        if (options.has(event)) {
            this.event = options.valueOf(event);
        } else {
            this.event = DEFAULT_EVENT;
        }
        if (options.has(frameBufferSize)) {
            this.frameBufferSize = OptionalLong.of(options.valueOf(frameBufferSize));
        } else {
            this.frameBufferSize = OptionalLong.empty();
        }
        if (options.has(interval)) {
            this.interval = OptionalLong.of(options.valueOf(interval));
        } else {
            this.interval = OptionalLong.empty();
        }
        if (options.has(outputDir)) {
            this.outputDir = Paths.get(options.valueOf(outputDir));
            createOutputDirectories();
        }
        if (options.has(threads)) {
            this.threads = options.valueOf(threads);
        } else {
            this.threads = false;
        }
        this.title = Optional.ofNullable(options.valueOf(title));
        this.profilerArgs = options.valuesOf(profilerArgs);

        if (options.has(asyncProfiler)) {
            this.asyncProfilerDir = Paths.get(options.valueOf(asyncProfiler));
        } else {
            this.asyncProfilerDir = Objects.requireNonNull(asyncDir, "directory of async-profiler missing");
        }
    }

    public FlameGraph() {
        Path asyncDir = null;
        String env = System.getenv(ASYNC_PROFILER_DIR);
        if (env != null) {
            asyncDir = Paths.get(env).toAbsolutePath();
        }
        this.event = DEFAULT_EVENT;
        this.frameBufferSize = OptionalLong.empty();
        this.interval = OptionalLong.empty();
        this.threads = false;
        this.title = Optional.empty();
        this.profilerArgs = Collections.emptyList();
        this.asyncProfilerDir = asyncDir;
    }

    @Override
    public Collection<String> addJVMInvokeOptions(final BenchmarkParams params) {
        return Collections.emptyList();
    }

    @Override
    public Collection<String> addJVMOptions(final BenchmarkParams params) {
        return Arrays.asList(
                "-XX:+UnlockDiagnosticVMOptions",
                "-XX:+DebugNonSafepoints"
        );
    }

    @Override
    public String getDescription() {
        return "Generate flame graphs using async-profiler";
    }

    @Override
    public void beforeTrial(final BenchmarkParams benchmarkParams) {
    }

    @Override
    public Collection<? extends Result> afterTrial(
            final BenchmarkResult br,
            final long pid,
            final File stdOut,
            final File stdErr) {
        return Collections.emptyList();
    }

    @Override
    public boolean allowPrintOut() {
        return true;
    }

    @Override
    public boolean allowPrintErr() {
        return true;
    }

    @Override
    public void beforeIteration(final BenchmarkParams benchmarkParams, final IterationParams iterationParams) {
        if (iterationParams.getType() != IterationType.MEASUREMENT || started || asyncProfilerDir == null) {
            return;
        }

        if (outputDir == null) {
            outputDir = createTempDir(benchmarkParams);
        }

        Collection<String> commands = new ArrayList<>();
        commands.add("-e");
        commands.add(event);
        if (threads) {
            commands.add("-t");
        }
        frameBufferSize.ifPresent(b -> {
            commands.add("-b");
            commands.add(String.valueOf(b));
        });
        interval.ifPresent(i -> {
            commands.add("-i");
            commands.add(String.valueOf(i));
        });
        title.ifPresent(t -> {
            commands.add("--title");
            commands.add(t);
        });
        commands.addAll(profilerArgs);

        commands.add("-f");
        commands.add(outputFile(benchmarkParams).toString());

        profilerCommand("start", commands);
        started = true;
    }

    @Override
    public Collection<? extends Result> afterIteration(
            final BenchmarkParams benchmarkParams,
            final IterationParams iterationParams,
            final IterationResult result) {
        if (iterationParams.getType() != IterationType.MEASUREMENT) {
            return Collections.emptyList();
        }

        if (++measurementIterationCount == iterationParams.getCount()) {
            Path profileFile = outputFile(benchmarkParams);
            profilerCommand("stop", Arrays.asList("-f", profileFile.toString()));
            generated.add(profileFile);
        }

        if (generated.isEmpty()) {
            return Collections.emptyList();
        }

        return generated
                .stream().map(Path::toAbsolutePath)
                .map(Path::toString)
                .map(f -> new NoResult("flame-graph", f))
                .collect(Collectors.toList());
    }

    private Path createTempDir(final BenchmarkParams benchmarkParams) {
        String benchmark = benchmarkParams.getBenchmark();
        int i = benchmark.lastIndexOf('.');
        if (i > 0) {
            benchmark = benchmark.substring(0, i);
        }
        try {
            return LdbcDownloader.tempDirFor("org.neo4j", "bench", benchmark);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static final Pattern SLASH = Pattern.compile("/");

    private Path outputFile(final BenchmarkParams benchmarkParams) {
        return outputDir.resolve(outputFileName(benchmarkParams)).toAbsolutePath();
    }

    private String outputFileName(final BenchmarkParams benchmarkParams) {
        String benchmark = benchmarkParams.getBenchmark();
        int i = benchmark.lastIndexOf('.');
        StringBuilder sb = new StringBuilder("profile-");
        if (i > 0) {
            sb.append(benchmark, i + 1, benchmark.length()).append("-");
        }
        sb.append(event).append("-").append(benchmarkParams.getMode());
        for (String key : benchmarkParams.getParamsKeys()) {
            sb
                    .append("-")
                    .append(key)
                    .append("-")
                    .append(benchmarkParams.getParam(key));
        }
        sb.append(".jfr");
        return SLASH.matcher(sb).replaceAll("-");
    }

    private void profilerCommand(final String action, final Collection<String> options) {
        long pid = Utils.getPid();

        List<String> cmd = new ArrayList<>();
        cmd.add("./profiler.sh");
        cmd.add(action);
        cmd.addAll(options);
        cmd.add(String.valueOf(pid));

        ProcessBuilder processBuilder = new ProcessBuilder(cmd);
        processBuilder.directory(asyncProfilerDir.toFile());
        startAndWait(processBuilder);
    }

    private void createOutputDirectories() {
        createDirectories(outputDir);
    }

    private static OptionSet parseInitLine(final String initLine, final OptionParser parser) throws ProfilerException {
        try {
            Method method = Class
                    .forName("org.openjdk.jmh.profile.ProfilerUtils")
                    .getDeclaredMethod("parseInitLine", String.class, OptionParser.class);
            method.setAccessible(true);
            return (OptionSet) method.invoke(null, initLine, parser);
        } catch (InvocationTargetException ex) {
            throw (ProfilerException) ex.getCause();
        } catch (NoSuchMethodException | IllegalArgumentException | IllegalAccessException | ClassNotFoundException | SecurityException e) {
            throw new ProfilerException(e);
        }
    }

    private static void startAndWait(final ProcessBuilder processBuilder) {
        String commandLine = String.join(" ", processBuilder.command());
        try {
            processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
            Process process = processBuilder.start();
            if (process.waitFor() != 0) {
                throw new RuntimeException("Non zero exit code from: " + commandLine);
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Error running " + commandLine, e);
        }
    }

    private static void createDirectories(final Path outputDir) {
        try {
            Files.createDirectories(outputDir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
