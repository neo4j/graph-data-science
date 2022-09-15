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
import org.mockito.Mockito;
import org.neo4j.annotations.service.Service;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.gds.graphbuilder.util.CaptureStdOut;
import org.neo4j.logging.Log;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.endsWith;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

class ProxyUtilTest {

    @Test
    void shouldLoadAtMostOnce() {
        // load twice, lookup a third time for the assertions
        ProxyUtil.findProxy(TestProxyFactory.class);
        ProxyUtil.findProxy(TestProxyFactory.class);
        var output = ProxyUtil.findProxyInfo(TestProxyFactory.class);

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

    @Test
    void shallNotLog() {
        var proxyInfoOutput = CaptureStdOut.run(() -> {
            // load a couple fo times
            ProxyUtil.findProxy(TestProxyFactory.class);
            ProxyUtil.findProxy(TestProxyFactory.class);
            return ProxyUtil.findProxyInfo(TestProxyFactory.class);
        });

        assertThat(proxyInfoOutput.stdout()).isEmpty();
        assertThat(proxyInfoOutput.stderr()).isEmpty();
    }

    @Test
    void shouldLogOnDemand() {
        // do some loading
        ProxyUtil.findProxy(TestProxyFactory.class);
        ProxyUtil.findProxy(TestProxyFactory.class);
        ProxyUtil.findProxy(TestProxyFactory.class);

        // cannot use TestLog because it is behind the proxy we want to test
        // and also not have available
        var log = Mockito.mock(Log.class);
        ProxyUtil.dumpLogMessages(log);

        verify(log)
            .info(
                endsWith(
                    "GDS compatibility: for Test proxy -- available, for Wrong Test proxy -- not available, selected: Test proxy"
                ),
                // We should not need to know that the implementation calls this overload with an empty object
                // instead of the one with just a string, but we cannot do better with mocking.
                any(Object[].class)
            );

        verify(log)
            .debug(
                eq(
                    "Java vendor: [%s] Java version: [%s] Java home: [%s] GDS version: [%s] Detected Neo4j version: [%s]"
                ),
                anyString(/* javaVendor */),
                anyString(/* javaVersion */),
                anyString(/* javaHome() */),
                anyString(/* gdsVersion() */),
                any(Neo4jVersion.class)
            );
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
