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
package org.neo4j.gds.compat;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.neo4j.annotations.service.Service;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.gds.graphbuilder.util.CaptureStdOut;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class ProxyUtilTest {

    @Test
    void shouldLoadAndLogAtMostOnce() {
        var proxyInfoOutput = CaptureStdOut.run(() -> {
            // load twice, lookup a third time for the assertions on the log output
            ProxyUtil.findProxy(TestProxyFactory.class);
            ProxyUtil.findProxy(TestProxyFactory.class);
            return ProxyUtil.findProxyInfo(TestProxyFactory.class);
        });

        assertThat(proxyInfoOutput.stdout())
            .hasLineCount(1)
            .endsWith(
                "GDS compatibility: for Test proxy -- available, for Wrong Test proxy -- not available, selected: Test proxy");

        var output = proxyInfoOutput.output();
        assertThat(output).isNotNull();

        assertThat(output.availability()).containsExactly(
            Map.entry("Test proxy", true),
            Map.entry("Wrong Test proxy", false)
        );

        assertThat(output.factory())
            .isPresent()
            .get(InstanceOfAssertFactories.type(TestProxyFactoryImpl.class))
            .returns(1, proxy -> proxy.loadCount);
    }

    @Service
    interface TestProxyFactory extends ProxyFactory<Void> {
    }

    @ServiceProvider
    public static class TestProxyFactoryImpl implements TestProxyFactory {

        private int loadCount = 0;

        @Override
        public boolean canLoad(Neo4jVersion version) {
            return true;
        }

        @Override
        public Void load() {
            loadCount++;
            return null;
        }

        @Override
        public String description() {
            return "Test proxy";
        }

    }

    @ServiceProvider
    public static class WrongTestProxyFactoryImpl implements TestProxyFactory {
        @Override
        public boolean canLoad(Neo4jVersion version) {
            return false;
        }

        @Override
        public Void load() {
            return fail("This implementation should not be loaded");
        }

        @Override
        public String description() {
            return "Wrong Test proxy";
        }
    }
}
