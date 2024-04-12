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
package nl.knaw.dans.managedeposit.core.service;

import nl.knaw.dans.managedeposit.AbstractTestWithTestDir;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.file.Files;

import static java.nio.file.Files.createDirectories;
import static java.util.Collections.singletonList;

public class IngestPathMonitorTest extends AbstractTestWithTestDir {

    private IngestPathMonitor startMonitor(DepositStatusUpdater mockUpdater, int pollingInterval) throws Exception {
        createDirectories(testDir);

        var monitor = new IngestPathMonitor(singletonList(testDir), mockUpdater, pollingInterval);
        monitor.start();
        Thread.sleep(60); // wait for the monitor to get ready
        return monitor;
    }

    @Test
    public void should_ignore_properties_in_root() throws Exception {
        var mockUpdater = Mockito.mock(DepositStatusUpdater.class);
        var monitor = startMonitor(mockUpdater, 50);

        var tempFile = Files.createFile(testDir.resolve("deposit.properties"));
        Thread.sleep(70);// Wait for the monitor to pick up the new file

        Mockito.verify(mockUpdater, Mockito.times(0)).onCreateDeposit(tempFile.toFile());

        monitor.stop();
    }

    @Test
    public void should_pick_up_properties_in_bag() throws Exception {
        var mockUpdater = Mockito.mock(DepositStatusUpdater.class);
        var monitor = startMonitor(mockUpdater, 50);

        var propertiesFile = testDir.resolve("bag/deposit.properties");
        createDirectories(propertiesFile.getParent());
        Files.createFile(propertiesFile);
        Thread.sleep(70);// Wait for the monitor to pick up the new file

        Mockito.verify(mockUpdater, Mockito.times(1)).onCreateDeposit(propertiesFile.toFile());

        monitor.stop();
    }

    @Test
    public void should_ignore_properties_in_bag_content() throws Exception {
        var mockUpdater = Mockito.mock(DepositStatusUpdater.class);
        var monitor = startMonitor(mockUpdater, 50);

        var propertiesFile = testDir.resolve("bag/content/deposit.properties");
        createDirectories(propertiesFile.getParent());
        Files.createFile(propertiesFile);
        Thread.sleep(70);// Wait for the monitor to pick up the new file

        Mockito.verify(mockUpdater, Mockito.times(0)).onCreateDeposit(propertiesFile.toFile());

        monitor.stop();
    }

    @Test
    public void should_pick_up_a_bag() throws Exception {
        var mockUpdater = Mockito.mock(DepositStatusUpdater.class);
        var monitor = startMonitor(mockUpdater, 50);

        var bagDir = testDir.resolve("bag");
        createDirectories(bagDir);
        Thread.sleep(70);// Wait for the monitor to pick up the new file

        Mockito.verify(mockUpdater, Mockito.times(1)).onCreateDeposit(bagDir.toFile());

        monitor.stop();
    }

    @Test
    public void should_ignore_bag_content() throws Exception {
        var mockUpdater = Mockito.mock(DepositStatusUpdater.class);
        var monitor = startMonitor(mockUpdater, 50);

        var dirInBag = testDir.resolve("bag/content");
        createDirectories(dirInBag);
        Thread.sleep(70);// Wait for the monitor to pick up the new file

        Mockito.verify(mockUpdater, Mockito.times(0)).onCreateDeposit(dirInBag.toFile());

        monitor.stop();
    }

    // TODO: Add more tests for onModifyDeposit and onDeleteDeposit and more complex scenarios, such as:
    //  events while the monitor was down
    //  not existing dir when starting the monitor
    //  disappearing dir while monitoring, etc.
}