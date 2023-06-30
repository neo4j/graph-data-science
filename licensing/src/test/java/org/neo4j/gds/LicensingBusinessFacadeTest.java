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
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class LicensingBusinessFacadeTest {

    @Test
    void test(@Mock LicensingService licensingServiceMock) {
        when(licensingServiceMock.get()).thenReturn(new TestLicenseStates.Unlicensed());


        LicenseDetails licenseDetails = new LicensingBusinessFacade(licensingServiceMock).licenseDetails();

        assertThat(licenseDetails)
            .matches(details -> !details.isLicensed())
            .matches(details -> details.details().equals("No valid GDS license specified."), "details were: " + licenseDetails.details());
    }

}
