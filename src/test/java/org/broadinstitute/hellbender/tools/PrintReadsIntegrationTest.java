package org.broadinstitute.hellbender.tools;

import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.utils.read.SamAssertionUtils;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;

public final class PrintReadsIntegrationTest extends CommandLineProgramTest{

    private static final File TEST_DATA_DIR = getTestDataDir();

    @Override
    public String getTestedClassName() {
        return PrintReads.class.getSimpleName();
    }


    @Test(dataProvider="testingData")
    public void testFileToFile(String fileIn, String extOut) throws Exception {
        String samFile= fileIn;
        final File outFile = BaseTest.createTempFile(samFile + ".", extOut);
        File ORIG_BAM = new File(TEST_DATA_DIR, samFile);
        final String[] args = new String[]{
                "--input" , ORIG_BAM.getAbsolutePath(),
                "--output", outFile.getAbsolutePath()
        };
        Assert.assertEquals(runCommandLine(args), null);
        SamAssertionUtils.assertSamsEqual(ORIG_BAM, outFile);
    }

    @Test(dataProvider="testingDataCRAM")
    public void testFileToFileCRAM(String fileIn, String extOut) throws Exception {
        String samFile= fileIn;
        final File outFile = File.createTempFile(samFile + ".", extOut);
        outFile.deleteOnExit();
        File ORIG_BAM = new File(TEST_DATA_DIR, samFile);
        File reference = new File(TEST_DATA_DIR, "print_reads.fasta");
        final String[] args = new String[]{
                "--input" , ORIG_BAM.getAbsolutePath(),
                "--output", outFile.getAbsolutePath(),
                "-R", reference.getAbsolutePath()
        };
        Assert.assertEquals(runCommandLine(args), null);
        SamAssertionUtils.assertSamsEqual(ORIG_BAM, outFile, reference);
    }

    @DataProvider(name="testingData")
    public Object[][] testingData() {
        return new String[][]{
                {"print_reads.sam", ".sam"},
                {"print_reads.sam", ".bam"},
                {"print_reads.bam", ".sam"},
                {"print_reads.bam", ".bam"},
        };
    }

    @DataProvider(name="testingDataCRAM")
    public Object[][] testingDataCRAM() {
        return new String[][]{
                {"print_reads.sorted.cram", ".sam"},
                {"print_reads.sorted.cram", ".bam"},
                {"print_reads.sorted.cram", ".cram"},
                {"print_reads.sorted.sam", ".cram"},
                {"print_reads.sorted.bam", ".cram"}
        };
    }

}