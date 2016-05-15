package org.iidp.ostmap.batch_processing.areacalc;


import org.iidp.ostmap.commons.accumulo.AmcHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;



public class CalcTest {

        @ClassRule
        public static TemporaryFolder tmpDir = new TemporaryFolder();
        public static TemporaryFolder tmpSettingsDir = new TemporaryFolder();
        public static AmcHelper amc = new AmcHelper();

        public CalcTest() throws IOException {
        }

        @BeforeClass
        public static void setUpCluster() {

            amc.startMiniCluster(tmpDir.getRoot().getAbsolutePath());
        }

        @AfterClass
        public static void shutDownCluster() {

            amc.stopMiniCluster();
        }

        @Test
        public void testCalcProcess() throws Exception {

        }
}
