package org.broadinstitute.hellbender.tools.walkers.haplotypecaller;

import com.google.common.base.Strings;
import htsjdk.variant.variantcontext.Allele;
import org.broadinstitute.hellbender.utils.MathUtils;
import org.broadinstitute.hellbender.utils.pairhmm.PairHMM;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for PairHMMLikelihoodCalculationEngine
 */
public class PairHMMLikelihoodCalculationEngineUnitTest extends BaseTest {

    Allele Aref, T, C, G, Cref, ATC, ATCATC;

    @BeforeSuite
    public void setup() {
        // alleles
        Aref = Allele.create("A", true);
        Cref = Allele.create("C", true);
        T = Allele.create("T");
        C = Allele.create("C");
        G = Allele.create("G");
        ATC = Allele.create("ATC");
        ATCATC = Allele.create("ATCATC");
    }

    @Test
    public void testNormalizeDiploidLikelihoodMatrixFromLog10() {
        double[][] likelihoodMatrix = {
            {-90.2,     0,      0},
            {-190.1, -2.1,      0},
            {-7.0,  -17.5,  -35.9}
        };
        double[][] normalizedMatrix = {
            {-88.1,     0,      0},
            {-188.0,  0.0,      0},
            {-4.9,  -15.4,  -33.8}
        };


        Assert.assertTrue(compareDoubleArrays(PairHMMLikelihoodCalculationEngine.normalizeDiploidLikelihoodMatrixFromLog10(likelihoodMatrix), normalizedMatrix));

        double[][] likelihoodMatrix2 = {
                {-90.2,     0,      0,        0},
                {-190.1, -2.1,      0,        0},
                {-7.0,  -17.5,  -35.9,        0},
                {-7.0,  -17.5,  -35.9,  -1000.0},
        };
        double[][] normalizedMatrix2 = {
                {-88.1,     0,      0,        0},
                {-188.0,  0.0,      0,        0},
                {-4.9,  -15.4,  -33.8,        0},
                {-4.9,  -15.4,  -33.8,   -997.9},
        };
        Assert.assertTrue(compareDoubleArrays(PairHMMLikelihoodCalculationEngine.normalizeDiploidLikelihoodMatrixFromLog10(likelihoodMatrix2), normalizedMatrix2));
    }

    @DataProvider(name = "PcrErrorModelTestProvider")
    public Object[][] createPcrErrorModelTestData() {
        List<Object[]> tests = new ArrayList<>();

        for ( final String repeat : Arrays.asList("A", "AC", "ACG", "ACGT") ) {
            for ( final int repeatLength : Arrays.asList(1, 2, 3, 5, 10, 15) ) {
                tests.add(new Object[]{repeat, repeatLength});
            }
        }

        return tests.toArray(new Object[][]{});
    }

    @Test(dataProvider = "PcrErrorModelTestProvider", enabled = true)
    public void createPcrErrorModelTest(final String repeat, final int repeatLength) {

        final PairHMMLikelihoodCalculationEngine engine = new PairHMMLikelihoodCalculationEngine((byte)0,
                PairHMM.Implementation.ORIGINAL, 0.0,
                PairHMMLikelihoodCalculationEngine.PCRErrorModel.CONSERVATIVE);

        final String readString = Strings.repeat(repeat, repeatLength);
        final byte[] insQuals = new byte[readString.length()];
        final byte[] delQuals = new byte[readString.length()];
        Arrays.fill(insQuals, (byte) PairHMMLikelihoodCalculationEngine.INITIAL_QSCORE);
        Arrays.fill(delQuals, (byte) PairHMMLikelihoodCalculationEngine.INITIAL_QSCORE);

        engine.applyPCRErrorModel(readString.getBytes(), insQuals, delQuals);

        for ( int i = 1; i < insQuals.length; i++ ) {

            final int repeatLengthFromCovariate = PairHMMLikelihoodCalculationEngine.findTandemRepeatUnits(readString.getBytes(), i-1).getRight();
            final byte adjustedScore = PairHMMLikelihoodCalculationEngine.getErrorModelAdjustedQual(repeatLengthFromCovariate, 3.0);

            Assert.assertEquals(insQuals[i - 1], adjustedScore);
            Assert.assertEquals(delQuals[i - 1], adjustedScore);
        }
    }

    /*
    private class BasicLikelihoodTestProvider extends TestDataProvider {
        public Double readLikelihoodForHaplotype1;
        public Double readLikelihoodForHaplotype2;
        public Double readLikelihoodForHaplotype3;
        
        public BasicLikelihoodTestProvider(double a, double b) {
            super(BasicLikelihoodTestProvider.class, String.format("Diploid haplotype likelihoods for reads %f / %f",a,b));
            readLikelihoodForHaplotype1 = a;
            readLikelihoodForHaplotype2 = b;
            readLikelihoodForHaplotype3 = null;
        }

        public BasicLikelihoodTestProvider(double a, double b, double c) {
            super(BasicLikelihoodTestProvider.class, String.format("Diploid haplotype likelihoods for reads %f / %f / %f",a,b,c));
            readLikelihoodForHaplotype1 = a;
            readLikelihoodForHaplotype2 = b;
            readLikelihoodForHaplotype3 = c;
        }
        
        public double[][] expectedDiploidHaplotypeMatrix() {
            if( readLikelihoodForHaplotype3 == null ) {
                double maxValue = Math.max(readLikelihoodForHaplotype1,readLikelihoodForHaplotype2);
                double[][] normalizedMatrix = {
                        {readLikelihoodForHaplotype1 - maxValue, Double.NEGATIVE_INFINITY},
                        {Math.log10(0.5*Math.pow(10,readLikelihoodForHaplotype1) + 0.5*Math.pow(10,readLikelihoodForHaplotype2)) - maxValue, readLikelihoodForHaplotype2 - maxValue}
                };
                return normalizedMatrix;
            } else {
                double maxValue = MathUtils.max(readLikelihoodForHaplotype1,readLikelihoodForHaplotype2,readLikelihoodForHaplotype3);
                double[][] normalizedMatrix = {
                        {readLikelihoodForHaplotype1 - maxValue, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY},
                        {Math.log10(0.5*Math.pow(10,readLikelihoodForHaplotype1) + 0.5*Math.pow(10,readLikelihoodForHaplotype2)) - maxValue, readLikelihoodForHaplotype2 - maxValue, Double.NEGATIVE_INFINITY},
                        {Math.log10(0.5*Math.pow(10,readLikelihoodForHaplotype1) + 0.5*Math.pow(10,readLikelihoodForHaplotype3)) - maxValue,
                         Math.log10(0.5*Math.pow(10,readLikelihoodForHaplotype2) + 0.5*Math.pow(10,readLikelihoodForHaplotype3)) - maxValue, readLikelihoodForHaplotype3 - maxValue}
                };
                return normalizedMatrix;
            }
        }
        
        public double[][] calcDiploidHaplotypeMatrix() {
            ArrayList<Haplotype> haplotypes = new ArrayList<Haplotype>();
            for( int iii = 1; iii <= 3; iii++) {
                Double readLikelihood = ( iii == 1 ? readLikelihoodForHaplotype1 : ( iii == 2 ? readLikelihoodForHaplotype2 : readLikelihoodForHaplotype3) );
                int readCount = 1;
                if( readLikelihood != null ) {
                    Haplotype haplotype = new Haplotype( (iii == 1 ? "AAAA" : (iii == 2 ? "CCCC" : "TTTT")).getBytes() );
                    haplotype.addReadLikelihoods("myTestSample", new double[]{readLikelihood}, new int[]{readCount});
                    haplotypes.add(haplotype);
                }
            }
            final HashSet<String> sampleSet = new HashSet<String>(1);
            sampleSet.add("myTestSample");
            return PairHMMLikelihoodCalculationEngine.computeDiploidHaplotypeLikelihoods(sampleSet, haplotypes);
        }
    }

    @DataProvider(name = "BasicLikelihoodTestProvider")
    public Object[][] makeBasicLikelihoodTests() {
        new BasicLikelihoodTestProvider(-1.1, -2.2);
        new BasicLikelihoodTestProvider(-2.2, -1.1);
        new BasicLikelihoodTestProvider(-1.1, -1.1);
        new BasicLikelihoodTestProvider(-9.7, -15.0);
        new BasicLikelihoodTestProvider(-1.1, -2000.2);
        new BasicLikelihoodTestProvider(-1000.1, -2.2);
        new BasicLikelihoodTestProvider(0, 0);
        new BasicLikelihoodTestProvider(-1.1, 0);
        new BasicLikelihoodTestProvider(0, -2.2);
        new BasicLikelihoodTestProvider(-100.1, -200.2);

        new BasicLikelihoodTestProvider(-1.1, -2.2, 0);
        new BasicLikelihoodTestProvider(-2.2, -1.1, 0);
        new BasicLikelihoodTestProvider(-1.1, -1.1, 0);
        new BasicLikelihoodTestProvider(-9.7, -15.0, 0);
        new BasicLikelihoodTestProvider(-1.1, -2000.2, 0);
        new BasicLikelihoodTestProvider(-1000.1, -2.2, 0);
        new BasicLikelihoodTestProvider(0, 0, 0);
        new BasicLikelihoodTestProvider(-1.1, 0, 0);
        new BasicLikelihoodTestProvider(0, -2.2, 0);
        new BasicLikelihoodTestProvider(-100.1, -200.2, 0);

        new BasicLikelihoodTestProvider(-1.1, -2.2, -12.121);
        new BasicLikelihoodTestProvider(-2.2, -1.1, -12.121);
        new BasicLikelihoodTestProvider(-1.1, -1.1, -12.121);
        new BasicLikelihoodTestProvider(-9.7, -15.0, -12.121);
        new BasicLikelihoodTestProvider(-1.1, -2000.2, -12.121);
        new BasicLikelihoodTestProvider(-1000.1, -2.2, -12.121);
        new BasicLikelihoodTestProvider(0, 0, -12.121);
        new BasicLikelihoodTestProvider(-1.1, 0, -12.121);
        new BasicLikelihoodTestProvider(0, -2.2, -12.121);
        new BasicLikelihoodTestProvider(-100.1, -200.2, -12.121);

        return BasicLikelihoodTestProvider.getTests(BasicLikelihoodTestProvider.class);
    }

    @Test(dataProvider = "BasicLikelihoodTestProvider", enabled = true)
    public void testOneReadWithTwoOrThreeHaplotypes(BasicLikelihoodTestProvider cfg) {
        double[][] calculatedMatrix = cfg.calcDiploidHaplotypeMatrix();
        double[][] expectedMatrix = cfg.expectedDiploidHaplotypeMatrix();
        logger.warn(String.format("Test: %s", cfg.toString()));
        Assert.assertTrue(compareDoubleArrays(calculatedMatrix, expectedMatrix));
    }
    */

    //Private function to compare 2d arrays
    private boolean compareDoubleArrays(double[][] b1, double[][] b2) {
        if( b1.length != b2.length ) {
            return false; // sanity check
        }

        for( int i=0; i < b1.length; i++ ){
            if( b1[i].length != b2[i].length) {
                return false; // sanity check
            }
            for( int j=0; j < b1.length; j++ ){
                if ( MathUtils.compareDoubles(b1[i][j], b2[i][j]) != 0 && !Double.isInfinite(b1[i][j]) && !Double.isInfinite(b2[i][j]))
                    return false;
            }
        }
        return true;
    }
}
