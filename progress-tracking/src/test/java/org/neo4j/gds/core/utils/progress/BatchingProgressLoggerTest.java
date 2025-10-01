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
package org.neo4j.gds.core.utils.progress;

import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.eclipse.collections.impl.utility.ListIterate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.gds.assertj.Extractors;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.progress.tasks.LeafTask;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.logging.GdsTestLog;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.BitUtil;
import org.neo4j.gds.utils.CloseableThreadLocal;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@ExtendWith(SoftAssertionsExtension.class)
class BatchingProgressLoggerTest {

    @Test
    void mustLogProgressOnlyAfterBatchSizeInvocations() {
        var log = new GdsTestLog();
        var taskVolume = 42;
        var batchSize = 8;
        var concurrency = new Concurrency(1);
        var logger = new BatchingProgressLogger(
            log,
            new JobId("some job id"),
            Tasks.leaf("foo", taskVolume),
            batchSize,
            concurrency
        );

        for (int i = 0; i < taskVolume; i++) {
            int currentProgress = i;
            logger.logProgress(() -> String.valueOf(currentProgress));
        }

        var threadName = Thread.currentThread().getName();
        var messageTemplate = "[%s] [%s] foo %d%% %d";
        var expectedMessages = List.of(
            formatWithLocale(messageTemplate, "some job id", threadName, 1 * batchSize * 100 / taskVolume, 1 * batchSize - 1),
            formatWithLocale(messageTemplate, "some job id", threadName, 2 * batchSize * 100 / taskVolume, 2 * batchSize - 1),
            formatWithLocale(messageTemplate, "some job id", threadName, 3 * batchSize * 100 / taskVolume, 3 * batchSize - 1),
            formatWithLocale(messageTemplate, "some job id", threadName, 4 * batchSize * 100 / taskVolume, 4 * batchSize - 1),
            formatWithLocale(messageTemplate, "some job id", threadName, 5 * batchSize * 100 / taskVolume, 5 * batchSize - 1)
        );

        var messages = log.getMessages("info");
        assertEquals(expectedMessages, messages);
    }

    @Test
    void mustLogProgressOnlyAfterHittingOrExceedingBatchSize() {
        var log = new GdsTestLog();
        var taskVolume = 1337;
        var progressStep = 5;
        var batchSize = 16;
        var concurrency = new Concurrency(1);
        var logger = new BatchingProgressLogger(
            log,
            new JobId("a job id"),
            Tasks.leaf("foo", taskVolume),
            batchSize,
            concurrency
        );

        for (int i = 0; i < taskVolume; i += progressStep) {
            logger.logProgress(progressStep);
        }

        var threadName = Thread.currentThread().getName();
        var messageTemplate = "[%s] [%s] foo %d%%";

        var progressSteps = IntStream
            .iterate(0, i -> i < taskVolume, i -> i + progressStep)
            .boxed()
            .collect(Collectors.toList());
        var loggedProgressSteps = ListIterate.distinctBy(progressSteps, i -> i / batchSize);

        var expectedMessages = loggedProgressSteps.stream()
            .skip(1)
            .map(i -> formatWithLocale(messageTemplate, "a job id", threadName, i * 100 / taskVolume))
            .collect(Collectors.toList());

        var messages = log.getMessages("info");
        assertEquals(expectedMessages, messages);
    }

    @Test
    void shouldLogEveryPercentageOnlyOnce() {
        var loggedPercentages = performLogging(400, new Concurrency(4));
        var expected = loggedPercentages.stream().distinct().collect(Collectors.toList());

        assertEquals(expected.size(), loggedPercentages.size());
    }

    @Test
    void shouldLogPercentagesSequentially() {
        var loggedPercentages = performLogging(400, new Concurrency(4));
        var expected = loggedPercentages.stream().sorted().collect(Collectors.toList());

        assertEquals(expected, loggedPercentages);
    }

    @Test
    void shouldLogEveryPercentageUnderHeavyContention() {
        var loggedPercentages = performLogging(20, new Concurrency(4));
        var expected = IntStream.rangeClosed(1, 20).map(p -> p * 5).boxed().collect(Collectors.toList());

        assertEquals(expected, loggedPercentages);
    }

    @Test
    void shouldLogAfterResetWhereACallCountHigherThanBatchSizeIsLeftBehind() {
        var concurrency = new Concurrency(1);
        var taskVolume = 1337;

        var log = new GdsTestLog();
        var logger = new BatchingProgressLogger(log, new JobId(), Tasks.leaf("Test", taskVolume), concurrency); // batchSize is 13
        logger.reset(taskVolume);
        logger.logProgress(20); // callCount is 20, call count after logging == 20 - 13 = 7
        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .containsExactly("Test 1%");
        logger.reset(420); // batchSize is now 4, which is smaller than the callCount 7
        logger.logProgress(10);
        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .containsExactly("Test 1%", "Test 2%"); // regardless of previous callCount, this should log an additional message
    }

    @Test
    void log100Percent() {
        var log = new GdsTestLog();
        var concurrency = new Concurrency(1);
        var testProgressLogger = new BatchingProgressLogger(log, new JobId(), Tasks.leaf("Test"), concurrency);
        testProgressLogger.reset(1337);
        testProgressLogger.logFinishPercentage();
        assertThat(log.getMessages(TestLog.INFO))
            .extracting(Extractors.removingThreadId())
            .containsExactly("Test 100%");
    }

    @Test
    void shouldLog100OnlyOnce() {
        var log = new GdsTestLog();
        var concurrency = new Concurrency(1);
        var testProgressLogger = new BatchingProgressLogger(log, new JobId(), Tasks.leaf("Test"), concurrency);
        testProgressLogger.reset(1);
        testProgressLogger.logProgress(1);
        testProgressLogger.logFinishPercentage();
        assertThat(log.getMessages(TestLog.INFO))
            .extracting(Extractors.removingThreadId())
            .containsExactly("Test 100%");
    }

    @Test
    void shouldNotExceed100Percent() {
        var log = new GdsTestLog();
        var concurrency = new Concurrency(1);
        var testProgressLogger = new BatchingProgressLogger(log, new JobId(), Tasks.leaf("Test"), concurrency);
        testProgressLogger.reset(1);
        testProgressLogger.logProgress(1); // reaches 100 %
        testProgressLogger.logProgress(1); // exceeds 100 %
        assertThat(log.getMessages(TestLog.INFO))
            .extracting(Extractors.removingThreadId())
            .containsExactly("Test 100%");
    }

    @Test
    void closesThreadLocal() {
        var logger = new BatchingProgressLogger(
            Log.noOpLog(),
            new JobId(),
            Tasks.leaf("foo", 42),
            new Concurrency(1)
        );

        logger.release();

        CloseableThreadLocal<Random> threadLocal = null;
        try {
            //noinspection unchecked
            threadLocal = (CloseableThreadLocal<Random>) MethodHandles
                .privateLookupIn(BatchingProgressLogger.class, MethodHandles.lookup())
                .findGetter(BatchingProgressLogger.class, "callCounter", CloseableThreadLocal.class)
                .invoke(logger);
        } catch (Throwable e) {
            fail("couldn't inspect the field", e);
        }
        assertThatThrownBy(threadLocal::get).isInstanceOf(NullPointerException.class);
    }

    private static List<Integer> performLogging(long taskVolume, Concurrency concurrency) {
        var log = new GdsTestLog();
        var logger = new BatchingProgressLogger(log, new JobId("the_job_id"), Tasks.leaf("Test", taskVolume), concurrency);
        logger.reset(taskVolume);

        var batchSize = (int) BitUtil.ceilDiv(taskVolume, concurrency.value());

        var tasks = IntStream
            .range(0, concurrency.value())
            .mapToObj(i -> (Runnable) () ->
                IntStream.range(0, batchSize).forEach(ignore -> {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    logger.logProgress();
                }))
            .collect(Collectors.toList());

        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(tasks)
            .run();

        return log
            .getMessages(TestLog.INFO)
            .stream()
            .map(progress -> progress.split(" ")[3].replace("%", ""))
            .map(Integer::parseInt)
            .collect(Collectors.toList());
    }

    @Test
    void shouldPrependCorrelationIdToInfoLogMessages() {
        var log = mock(Log.class);
        var batchingProgressLogger = new BatchingProgressLogger(
            log,
            JobId.parse("my job id"),
            new LeafTask("Monsieur Alfonse", 42),
            new Concurrency(87)
        );

        batchingProgressLogger.logMessage("Swiftly, and with style");

        verify(log).info("[%s] [%s] %s %s", "my job id", "Test worker", "Monsieur Alfonse", "Swiftly, and with style");
    }

    @Test
    void shouldPrependCorrelationIdToDebugLogMessages() {
        var log = mock(Log.class);
        var batchingProgressLogger = new BatchingProgressLogger(
            log,
            JobId.parse("my job id"),
            new LeafTask("Monsieur Alfonse", 42),
            new Concurrency(87)
        );

        when(log.isDebugEnabled()).thenReturn(true);
        batchingProgressLogger.logDebug("Swiftly, and with style");

        verify(log).debug("[%s] [%s] %s %s", "my job id", "Test worker", "Monsieur Alfonse", "Swiftly, and with style");
    }

    @Test
    void shouldPrependCorrelationIdToWarningLogMessages() {
        var log = mock(Log.class);
        var batchingProgressLogger = new BatchingProgressLogger(
            log,
            JobId.parse("my job id"),
            new LeafTask("Monsieur Alfonse", 42),
            new Concurrency(87)
        );

        batchingProgressLogger.logWarning("Swiftly, and with style");

        verify(log).warn("[%s] [%s] %s %s", "my job id", "Test worker", "Monsieur Alfonse", "Swiftly, and with style");
    }

    @Test
    void shouldPrependCorrelationIdToErrorLogMessages() {
        var log = mock(Log.class);
        var batchingProgressLogger = new BatchingProgressLogger(
            log,
            JobId.parse("my job id"),
            new LeafTask("Monsieur Alfonse", 42),
            new Concurrency(87)
        );

        batchingProgressLogger.logError("Swiftly, and with style");

        verify(log).error("[%s] [%s] %s %s", "my job id", "Test worker", "Monsieur Alfonse", "Swiftly, and with style");
    }
}
