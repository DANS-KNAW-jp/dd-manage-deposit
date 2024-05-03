/*
 * Copyright (C) 2023 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.managedeposit.db;

import nl.knaw.dans.managedeposit.AbstractDatabaseTest;
import nl.knaw.dans.managedeposit.core.DepositProperties;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DepositPropertiesDAODeleteSelectionTest extends AbstractDatabaseTest {

    @Test
    public void should_delete_records_of_specified_user() {
        var testUser = "testUser";

        // Create a DepositProperties object and persist it
        var dp = new DepositProperties("testId", testUser, "testBag", "testState",
            "testDescription", OffsetDateTime.now(), "testLocation", 1000L, OffsetDateTime.now());
        daoTestExtension.inTransaction(() -> dao.create(dp));
        // TODO add more and prove that other users are not deleted

        // Create query parameters
        var queryParameters = Map.of(
            "user", List.of(testUser)
        );

        var deletedCount = daoTestExtension.inTransaction(() ->
            // method under test
            dao.deleteSelection(queryParameters).orElse(0)
        );

        // Assert that the correct number of records were deleted
        assertEquals(1, deletedCount);

        // Assert that the deleted record no longer exists
        var results = dao.findSelection(queryParameters);
        assertTrue(results.isEmpty());
    }
}