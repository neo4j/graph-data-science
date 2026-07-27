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
package org.neo4j.gds.procedures.operations;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ProgressResultTest {

    @Test
    void shouldBuildFromArrowProcess() {
        var result = ProgressResult.fromArrowProcess(
            "neo4j",
            "job-1",
            "FastPath",
            0.5,
            "RUNNING",
            1_000L,
            6_000L
        );

        assertThat(result.username()).isEqualTo("neo4j");
        assertThat(result.jobId()).isEqualTo("job-1");
        assertThat(result.taskName()).isEqualTo("FastPath");
        assertThat(result.progress()).isEqualTo("50%");
        assertThat(result.progressBar()).isEqualTo("[#####~~~~~]");
        assertThat(result.status()).isEqualTo("RUNNING");
        assertThat(result.timeStarted()).isNotNull();
        // 6000 - 1000 = 5 seconds
        assertThat(result.elapsedTime()).contains("5 seconds");
    }

    @Test
    void shouldRenderNotStartedWhenStartTimeUnknown() {
        var result = ProgressResult.fromArrowProcess(
            "neo4j",
            "job-2",
            "FastPath",
            0.0,
            "PENDING",
            -1L,
            0L
        );

        assertThat(result.timeStarted()).isNull();
        assertThat(result.elapsedTime()).isEqualTo("Not yet started");
        assertThat(result.progress()).isEqualTo("0%");
        assertThat(result.progressBar()).isEqualTo("[~~~~~~~~~~]");
    }
}
