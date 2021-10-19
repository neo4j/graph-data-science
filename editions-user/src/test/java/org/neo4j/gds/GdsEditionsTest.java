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

import com.neo4j.gds.licensing.LicensingExtension;
import org.junit.jupiter.api.Test;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.util.ServiceLoader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GdsEditionsTest {

    @Test
    void canLoadOpenGdsEditionFactory() {
        var editionFactory = ServiceLoader.load(GdsEditionFactory.class)
            .stream()
            .map(ServiceLoader.Provider::get)
            .filter(factory -> !factory.handlesLicensing())
            .findFirst()
            .orElseThrow(() -> new LinkageError("Could not load " + GdsEditionFactory.class + " implementation"));
        var edition = editionFactory.create();
        assertThat(edition.label()).isEqualTo("OpenGDS");
        assertThat(edition.errorMessage()).isEmpty();
    }

    @Test
    void withoutLicenseKeyCheckingCommercialEditionFailsLoudly() {
        var editionFactory = ServiceLoader.load(GdsEditionFactory.class)
            .stream()
            .map(ServiceLoader.Provider::get)
            .filter(GdsEditionFactory::handlesLicensing)
            .findFirst()
            .orElseThrow(() -> new LinkageError("Could not load " + GdsEditionFactory.class + " implementation"));
        assertThatThrownBy(editionFactory::create).hasMessage("GdsEdition state is not initialized!");
    }

    @Test
    void withLicenseKeyCheckingCommercialEditionDoesntFail() {
        var dbms = new TestDatabaseManagementServiceBuilder()
            .impermanent()
            .noOpSystemGraphInitializer()
            .removeExtensions(extension -> extension instanceof LicensingExtension)
            .addExtension(new LicensingExtension())
            .build();
        dbms.shutdown();

        var editionFactory = ServiceLoader.load(GdsEditionFactory.class)
            .stream()
            .map(ServiceLoader.Provider::get)
            .filter(GdsEditionFactory::handlesLicensing)
            .findFirst()
            .orElseThrow(() -> new LinkageError("Could not load " + GdsEditionFactory.class + " implementation"));
        assertThatNoException().isThrownBy(editionFactory::create);
    }
}
