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
package org.neo4j.gds.internal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProc;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.ListProgressProc;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.config.ImmutableGraphCreateFromStoreConfig;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.progress.ProgressEventExtension;
import org.neo4j.gds.core.utils.progress.ProgressEventTracker;
import org.neo4j.gds.core.utils.progress.ProgressFeatureSettings;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.gdl.GdlFactory;
import org.neo4j.gds.model.catalog.TestTrainConfig;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;
import org.neo4j.scheduler.Group;
import org.neo4j.test.FakeClockJobScheduler;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThatCode;

class AuraMaintenanceFunctionTest extends BaseTest {

    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        super.configuration(builder);
        builder.setConfig(AuraMaintenanceSettings.maintenance_function_enabled, true);
        builder.addExtension(new AuraMaintenanceExtension());
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
        ModelCatalog.removeAllLoadedModels();
    }

    @Test
    void shouldReturnTrueIfThereAreNoGraphsInTheCatalog() {
        assertSafeToRestart(true);
    }

    @Test
    void shouldReturnFalseIfThereAreGraphsInTheCatalog() {
        var config = ImmutableGraphCreateFromStoreConfig.of(
            AuthSubject.ANONYMOUS.username(),
            "config",
            NodeProjections.all(),
            RelationshipProjections.all()
        );
        var graphStore = new StoreLoaderBuilder().api(db)
            .build()
            .graphStore();
        GraphStoreCatalog.set(config, graphStore);

        assertSafeToRestart(false);
    }

    @Test
    void shouldReturnFalseIfThereAreModelsInTheCatalog() {
        ModelCatalog.set(Model.of(
            AuthSubject.ANONYMOUS.username(),
            "testModel",
            "testAlgo",
            GdlFactory.of("(:Node1)").build().graphStore().schema(),
            "modelData",
            TestTrainConfig.of(),
            Map::of
        ));

        assertSafeToRestart(false);
    }

    private static final AtomicReference<FakeClockJobScheduler> FAKE_SCHEDULER = new AtomicReference<>();

    @Nested
    class AuraMaintenanceFunctionWithProgressEventsTest extends BaseTest {

        private final FakeClockJobScheduler scheduler = new FakeClockJobScheduler();

        @Override
        @ExtensionCallback
        protected void configuration(TestDatabaseManagementServiceBuilder builder) {
            super.configuration(builder);
            builder.setConfig(AuraMaintenanceSettings.maintenance_function_enabled, true);
            builder.addExtension(new AuraMaintenanceExtension());
            builder.setConfig(ProgressFeatureSettings.progress_tracking_enabled, true);
            // make sure that we 1) have our extension under test and 2) have it only once
            builder.removeExtensions(ex -> ex instanceof ProgressEventExtension);
            builder.addExtension(new ProgressEventExtension(scheduler));
        }

        @BeforeEach
        void setUp() throws Exception {
            GraphDatabaseApiProxy.registerProcedures(db, ProgressLoggingTestProc.class, ListProgressProc.class);
        }

        @Test
        void shouldReturnFalseIfThereAreProgressEvents() {
            // no progress - safe to restart
            assertSafeToRestart(true);

            // share scheduler with test proc
            FAKE_SCHEDULER.set(scheduler);

            // the test proc will
            //   - submit en event
            //   - wait 420 fake milliseconds
            //   - remove its events
            runQuery("CALL gds.test.addEvent()");

            // wait 100 milliseconds for the initial queue wait time to pick up the event
            scheduler.forward(100, TimeUnit.MILLISECONDS);

            // now we should see the event, so not safe to restart
            assertSafeToRestart(false);

            // wait 420 milliseconds to see the release event
            scheduler.forward(420, TimeUnit.MILLISECONDS);

            // no more events, safe to restart again
            assertSafeToRestart(true);
        }
    }

    private void assertSafeToRestart(boolean expected) {
        assertCypherResult(
            "RETURN gds.internal.safeToRestart() AS safeToRestart",
            List.of(Map.of("safeToRestart", expected))
        );
    }

    @Nested
    class AuraMaintenanceFunctionDisabledTest extends BaseTest {

        @Override
        @ExtensionCallback
        protected void configuration(TestDatabaseManagementServiceBuilder builder) {
            super.configuration(builder);
            builder.setConfig(AuraMaintenanceSettings.maintenance_function_enabled, false);
            builder.addExtension(new AuraMaintenanceExtension());
        }

        @Test
        void shouldNotBeVisibleWithoutTheSettingEnabled() {
            assertThatCode(() -> runQuery("RETURN gds.internal.safeToRestart()"))
                .hasMessageStartingWith("Unknown function 'gds.internal.safeToRestart'");
        }
    }

    public static class ProgressLoggingTestProc extends BaseProc {
        @Context
        public ProgressEventTracker progress;

        @Procedure("gds.test.addEvent")
        public void addEvent() {
            var task = Tasks.leaf("gds.test");
            progress.addTaskProgressEvent(task);
            taskRegistry.registerTask(task);
            FAKE_SCHEDULER.get().schedule(Group.DATA_COLLECTOR, () -> { progress.release(); taskRegistry.unregisterTask(); }, 420, TimeUnit.MILLISECONDS);
        }
    }
}
