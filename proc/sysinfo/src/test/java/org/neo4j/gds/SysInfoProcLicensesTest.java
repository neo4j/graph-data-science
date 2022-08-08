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

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;


class SysInfoProcLicensesTest {

    @Test
    void testExpiredLicense() throws IOException {
        var pastTime = ZonedDateTime.of(1337, 4, 2, 13, 37, 42, 0, ZoneOffset.UTC);
        var proc = new SysInfoProc();
        proc.licenseState = new LicenseState() {
            @Override
            public <R, P> R visit(VisitorWithParameter<R, P> visitor, P parameter) {
                return visitor.invalid(
                    "TestExpired",
                    "License expires a long time ago.",
                    Optional.of(pastTime),
                    parameter
                );
            }
        };

        proc.version().forEach(dv -> {
            switch (dv.key) {
                case "gdsEdition":
                    assertThat(dv.value).asInstanceOf(InstanceOfAssertFactories.STRING).isEqualTo("TestExpired");
                    break;
                case "gdsLicenseExpirationTime":
                    assertThat(dv.value).asInstanceOf(InstanceOfAssertFactories.ZONED_DATE_TIME).isEqualTo(pastTime);
                    break;
                case "gdsLicenseError":
                    assertThat(dv.value)
                        .asInstanceOf(InstanceOfAssertFactories.STRING)
                        .isEqualTo("License expires a long time ago.");
                    break;
                default:
                    // ignore, key is not relevant for this test
            }
        });
    }

    @Test
    void testOtherInvalidLicense() throws IOException {
        var proc = new SysInfoProc();
        proc.licenseState = new LicenseState() {
            @Override
            public <R, P> R visit(VisitorWithParameter<R, P> visitor, P parameter) {
                return visitor.invalid(
                    "TestInvalid",
                    "License invalid for some reason.",
                    Optional.empty(),
                    parameter
                );
            }
        };

        proc.version().forEach(dv -> {
            switch (dv.key) {
                case "gdsEdition":
                    assertThat(dv.value).asInstanceOf(InstanceOfAssertFactories.STRING).isEqualTo("TestInvalid");
                    break;
                case "gdsLicenseExpirationTime":
                    fail("Expected no license expiration time");
                    break;
                case "gdsLicenseError":
                    assertThat(dv.value)
                        .asInstanceOf(InstanceOfAssertFactories.STRING)
                        .isEqualTo("License invalid for some reason.");
                    break;
                default:
                    // ignore, key is not relevant for this test
            }
        });
    }

    @Test
    void testValidLicense() throws IOException {
        var futureTime = ZonedDateTime.of(4242, 2, 1, 13, 37, 42, 0, ZoneOffset.UTC);
        var proc = new SysInfoProc();
        proc.licenseState = new LicenseState() {
            @Override
            public <R, P> R visit(VisitorWithParameter<R, P> visitor, P parameter) {
                return visitor.licensed("TestValid", futureTime, parameter);
            }
        };

        proc.version().forEach(dv -> {
            switch (dv.key) {
                case "gdsEdition":
                    assertThat(dv.value).asInstanceOf(InstanceOfAssertFactories.STRING).isEqualTo("TestValid");
                    break;
                case "gdsLicenseExpirationTime":
                    assertThat(dv.value).asInstanceOf(InstanceOfAssertFactories.ZONED_DATE_TIME).isEqualTo(futureTime);
                    break;
                case "gdsLicenseError":
                    fail("Expected no license error");
                    break;
                default:
                    // ignore, key is not relevant for this test
            }
        });
    }

    @Test
    void testUnlicensed() throws IOException {
        var proc = new SysInfoProc();
        proc.licenseState = new LicenseState() {
            @Override
            public <R, P> R visit(VisitorWithParameter<R, P> visitor, P parameter) {
                return visitor.unlicensed("TestUnlicensed", parameter);
            }
        };

        proc.version().forEach(dv -> {
            switch (dv.key) {
                case "gdsEdition":
                    assertThat(dv.value).asInstanceOf(InstanceOfAssertFactories.STRING).isEqualTo("TestUnlicensed");
                    break;
                case "gdsLicenseExpirationTime":
                    fail("Expected no license expiration time");
                    break;
                case "gdsLicenseError":
                    fail("Expected no license error");
                    break;
                default:
                    // ignore, key is not relevant for this test
            }
        });
    }

    @Nested
    class SysInfoProcExpiredLicenseProcTest extends BaseProcTest {

        private final ZonedDateTime pastTime = ZonedDateTime.of(1337, 4, 2, 13, 37, 42, 0, ZoneOffset.UTC);

        private final LicenseState testLicenseState = new LicenseState() {
            @Override
            public <R, P> R visit(VisitorWithParameter<R, P> visitor, P parameter) {
                return visitor.invalid(
                    "TestExpired",
                    "License expires a long time ago.",
                    Optional.of(pastTime),
                    parameter
                );
            }
        };

        @Override
        @ExtensionCallback
        protected void configuration(TestDatabaseManagementServiceBuilder builder) {
            super.configuration(builder);
            builder
                .removeExtensions(e -> e instanceof EditionFactory)
                .addExtension(new EditionFactory(testLicenseState));
        }

        @BeforeEach
        void setup() throws Exception {
            registerProcedures(SysInfoProc.class);
        }


        @Test
        void shouldReturnLicenseExpirationTime() {
            runQueryWithRowConsumer(
                "CALL gds.debug.sysInfo() YIELD key, value WITH key, value WHERE key IN ['gdsEdition', 'gdsLicenseExpirationTime', 'gdsLicenseError'] RETURN key, value",
                row -> {
                    switch (row.getString("key")) {
                        case "gdsEdition":
                            assertThat(row.getString("value")).isEqualTo("TestExpired");
                            break;
                        case "gdsLicenseExpirationTime":
                            assertThat((ZonedDateTime) row.get("value")).isEqualTo(pastTime);
                            break;
                        case "gdsLicenseError":
                            assertThat(row.getString("value")).isEqualTo("License expires a long time ago.");
                            break;
                        default:
                            fail("Unexpected key: " + row.getString("key"));
                    }
                }
            );
        }
    }
}
