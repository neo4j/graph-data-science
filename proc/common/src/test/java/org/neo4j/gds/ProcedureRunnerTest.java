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
package org.neo4j.gds;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.GlobalTaskStore;
import org.neo4j.gds.core.utils.progress.TaskRegistry;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.test.TestProc;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

import static org.assertj.core.api.Assertions.assertThat;

class ProcedureRunnerTest extends BaseTest {

    @Test
    void shouldPassCorrectUsername() {
        try (var tx = db.beginTx()) {
            var procedureCallContext = new ProcedureCallContext(42, new String[]{"prop"}, true, db.databaseName(), false);
            var log = new TestLog();
            var username = Username.of("foo");
            TaskRegistryFactory taskRegistryFactory = () -> new TaskRegistry(username.username(), new GlobalTaskStore());
            var allocationTracker = AllocationTracker.empty();
            var licenseState = OpenGdsLicenseState.INSTANCE;
            ProcedureRunner.applyOnProcedure(
                db,
                TestProc.class,
                procedureCallContext,
                log,
                taskRegistryFactory,
                allocationTracker,
                tx,
                licenseState,
                username,
                proc -> {
                    assertThat(proc.procedureTransaction).isEqualTo(tx);
                    assertThat(proc.callContext).isEqualTo(procedureCallContext);
                    assertThat(proc.log).isEqualTo(log);
                    assertThat(proc.taskRegistryFactory).isEqualTo(taskRegistryFactory);
                    assertThat(proc.username).isEqualTo(username);
                    assertThat(proc.allocationTracker).isEqualTo(allocationTracker);
                    assertThat(proc.licenseState).isEqualTo(licenseState);
                }
            );
        }
    }
}