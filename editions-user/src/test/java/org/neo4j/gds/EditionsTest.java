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

class EditionsTest {

    @Test
    void firstLoadedIsOpenGds() {
        var newThingFactory = ServiceLoader.load(NewThingFactory.class)
            .findFirst()
            .orElseThrow(() -> new LinkageError("Could not load " + NewThingFactory.class + " implementation"));
        var editionStuff = newThingFactory.createEditionStuff();
        assertThat(editionStuff.label()).isEqualTo("OpenGDS");
        assertThat(editionStuff.errorMessage()).isEmpty();
    }

    @Test
    void withoutLicenseKeyCheckingCommercialStuffFailsLoudly() {
        ServiceLoader.load(NewThingFactory.class).stream()
            .map(factory -> factory.get().createEditionStuff())
            .forEach(edition -> {
                assertThatThrownBy(edition::label).hasMessageContaining("Ring the alarm!");
                assertThatThrownBy(edition::errorMessage).hasMessageContaining("Ring the alarm!");
            });
    }

    @Test
    void withLicenseKeyCheckingCommercialStuffDoesntFail() {
        var dbms = new TestDatabaseManagementServiceBuilder()
            .impermanent()
            .noOpSystemGraphInitializer()
            .removeExtensions(extension -> extension instanceof LicensingExtension)
            .addExtension(new LicensingExtension())
            .build();
        dbms.shutdown();

        ServiceLoader.load(NewThingFactory.class).stream()
            .map(factory -> factory.get().createEditionStuff())
            .forEach(edition -> {
                assertThatNoException().isThrownBy(edition::label);
                assertThatNoException().isThrownBy(edition::errorMessage);
            });
    }

}
