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
package org.neo4j.gds.core.utils;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.eclipse.collections.impl.utility.ListIterate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.mem.BitUtil;
import org.neo4j.gds.utils.CloseableThreadLocal;

import java.lang.reflect.Field;
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
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@ExtendWith(SoftAssertionsExtension.class)
class BatchingProgressLoggerTest {

    @Test
    void mustLogProgressOnlyAfterBatchSizeInvocations() {
        var log = Neo4jProxy.testLog();
        var taskVolume = 42;
        var batchSize = 8;
        var logger = new BatchingProgressLogger(
            log,
            Tasks.leaf("foo", taskVolume),
            batchSize,
            /* concurrency */0
        );

        for (int i = 0; i < taskVolume; i++) {
            int currentProgress = i;
            logger.logProgress(() -> String.valueOf(currentProgress));
        }

        var threadName = Thread.currentThread().getName();
        var messageTemplate = "[%s] foo %d%% %d";
        var expectedMessages = List.of(
            formatWithLocale(messageTemplate, threadName, 1 * batchSize * 100 / taskVolume, 1 * batchSize - 1),
            formatWithLocale(messageTemplate, threadName, 2 * batchSize * 100 / taskVolume, 2 * batchSize - 1),
            formatWithLocale(messageTemplate, threadName, 3 * batchSize * 100 / taskVolume, 3 * batchSize - 1),
            formatWithLocale(messageTemplate, threadName, 4 * batchSize * 100 / taskVolume, 4 * batchSize - 1),
            formatWithLocale(messageTemplate, threadName, 5 * batchSize * 100 / taskVolume, 5 * batchSize - 1)
        );

        var messages = log.getMessages("info");
        assertEquals(expectedMessages, messages);
    }

    @Test
    void mustLogProgressOnlyAfterHittingOrExceedingBatchSize() {
        var log = Neo4jProxy.testLog();
        var taskVolume = 1337;
        var progressStep = 5;
        var batchSize = 16;
        var logger = new BatchingProgressLogger(
            log,
            Tasks.leaf("foo", taskVolume),
            batchSize,
            /* concurrency */1
        );

        for (int i = 0; i < taskVolume; i += progressStep) {
            logger.logProgress(progressStep);
        }

        var threadName = Thread.currentThread().getName();
        var messageTemplate = "[%s] foo %d%%";

        var progressSteps = IntStream
            .iterate(0, i -> i < taskVolume, i -> i + progressStep)
            .boxed()
            .collect(Collectors.toList());
        var loggedProgressSteps = ListIterate.distinctBy(progressSteps, i -> i / batchSize);

        var expectedMessages = loggedProgressSteps.stream()
            .skip(1)
            .map(i -> formatWithLocale(messageTemplate, threadName, i * 100 / taskVolume))
            .collect(Collectors.toList());

        var messages = log.getMessages("info");
        assertEquals(expectedMessages, messages);
    }

    @Test
    void shouldLogEveryPercentageOnlyOnce() {
        var loggedPercentages = performLogging(400, 4);
        var expected = loggedPercentages.stream().distinct().collect(Collectors.toList());

        assertEquals(expected.size(), loggedPercentages.size());
    }

    @Test
    void shouldLogPercentagesSequentially() {
        var loggedPercentages = performLogging(400, 4);
        var expected = loggedPercentages.stream().sorted().collect(Collectors.toList());

        assertEquals(expected, loggedPercentages);
    }

    @Test
    void shouldLogEveryPercentageUnderHeavyContention() {
        var loggedPercentages = performLogging(20, 4);
        var expected = IntStream.rangeClosed(1, 20).map(p -> p * 5).boxed().collect(Collectors.toList());

        assertEquals(expected, loggedPercentages);
    }

    @Test
    void shouldLogAfterResetWhereACallCountHigherThanBatchSizeIsLeftBehind() {
        var concurrency = 1;
        var taskVolume = 1337;

        TestLog log = Neo4jProxy.testLog();
        var logger = new BatchingProgressLogger(log, Tasks.leaf("Test", taskVolume), concurrency); // batchSize is 13
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
        try (var ignore = RenamesCurrentThread.renameThread("test")) {
            TestLog log = Neo4jProxy.testLog();
            var testProgressLogger = new BatchingProgressLogger(log, Tasks.leaf("Test"), 1);
            testProgressLogger.reset(1337);
            testProgressLogger.logFinishPercentage();
            assertThat(log.getMessages(TestLog.INFO)).containsExactly("[test] Test 100%");
        }
    }

    @Test
    void shouldLog100OnlyOnce() {
        try (var ignore = RenamesCurrentThread.renameThread("test")) {
            TestLog log = Neo4jProxy.testLog();
            var testProgressLogger = new BatchingProgressLogger(log, Tasks.leaf("Test"), 1);
            testProgressLogger.reset(1);
            testProgressLogger.logProgress(1);
            testProgressLogger.logFinishPercentage();
            assertThat(log.getMessages(TestLog.INFO)).containsExactly("[test] Test 100%");
        }
    }

    @Test
    void closesThreadLocal() {
        var logger = new BatchingProgressLogger(
            Neo4jProxy.testLog(),
            Tasks.leaf("foo", 42),
            1
        );

        logger.release();

        Field threadLocalField = FieldUtils.getField(BatchingProgressLogger.class, "callCounter", true);
        try {
            //noinspection unchecked
            final var threadLocal = (CloseableThreadLocal<Random>) threadLocalField.get(logger);
            assertThatThrownBy(threadLocal::get).isInstanceOf(NullPointerException.class);
        } catch (IllegalAccessException e) {
            fail("couldn't inspect the field");
        }
    }

    private static List<Integer> performLogging(long taskVolume, int concurrency) {
        TestLog log = Neo4jProxy.testLog();
        var logger = new BatchingProgressLogger(log, Tasks.leaf("Test", taskVolume), concurrency);
        logger.reset(taskVolume);

        var batchSize = (int) BitUtil.ceilDiv(taskVolume, concurrency);

        var tasks = IntStream
            .range(0, concurrency)
            .mapToObj(i -> (Runnable) () ->
                IntStream.range(0, batchSize).forEach(ignore -> {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    logger.logProgress();
                }))
            .collect(Collectors.toList());

        RunWithConcurrency.builder()
            .concurrency(4)
            .tasks(tasks)
            .run();

        return log
            .getMessages(TestLog.INFO)
            .stream()
            .map(progress -> progress.split(" ")[2].replace("%", ""))
            .map(Integer::parseInt)
            .collect(Collectors.toList());
    }

}
