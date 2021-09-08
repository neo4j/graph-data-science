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
package org.neo4j.gds.extension;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.gds.core.utils.ClockService;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import java.time.Clock;

import static org.neo4j.gds.extension.ExtensionUtil.injectInstance;

public class FakeClockSupportExtension implements BeforeEachCallback, AfterEachCallback {

    private static final FakeClock FAKE_CLOCK = Clocks.fakeClock();

    private Clock clockBefore;

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        clockBefore = ClockService.clock();
        ClockService.setClock(FAKE_CLOCK);
        context.getRequiredTestInstances().getAllInstances().forEach(testInstance -> {
            injectInstance(testInstance, FAKE_CLOCK, FakeClock.class);
        });
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        ClockService.setClock(clockBefore);
        clockBefore = null;
    }
}
