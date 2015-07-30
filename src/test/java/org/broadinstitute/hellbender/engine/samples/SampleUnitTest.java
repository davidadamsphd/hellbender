package org.broadinstitute.hellbender.engine.samples;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.broadinstitute.hellbender.utils.test.BaseTest;

public class SampleUnitTest extends BaseTest {
    SampleDB db;
    static Sample fam1A, fam1B, fam1C;
    static Sample s1, s2;
    static Sample trait1, trait2, trait3, trait4, trait5;

    @BeforeClass
    public void init() {
        db = new SampleDB();

        fam1A = new Sample("1A", db, "fam1", "1B", "1C", Gender.UNKNOWN);
        fam1B = new Sample("1B", db, "fam1", null, null, Gender.MALE);
        fam1C = new Sample("1C", db, "fam1", null, null, Gender.FEMALE);

        s1 = new Sample("s1", db);
        s2 = new Sample("s2", db);

        trait1 = new Sample("t1", db, Affection.AFFECTED, Sample.UNSET_QT);
        trait2 = new Sample("t2", db, Affection.UNAFFECTED, Sample.UNSET_QT);
        trait3 = new Sample("t3", db, Affection.UNKNOWN, Sample.UNSET_QT);
        trait4 = new Sample("t4", db, Affection.OTHER, "1.0");
        trait5 = new Sample("t4", db, Affection.OTHER, "CEU");
    }

    /**
     * Now basic getters
     */
    @Test()
    public void normalGettersTest() {
        Assert.assertEquals("1A", fam1A.getID());
        Assert.assertEquals("fam1", fam1A.getFamilyID());
        Assert.assertEquals("1B", fam1A.getPaternalID());
        Assert.assertEquals("1C", fam1A.getMaternalID());
        Assert.assertEquals(null, fam1B.getPaternalID());
        Assert.assertEquals(null, fam1B.getMaternalID());

        Assert.assertEquals(Affection.AFFECTED, trait1.getAffection());
        Assert.assertEquals(Sample.UNSET_QT, trait1.getOtherPhenotype());
        Assert.assertEquals(Affection.UNAFFECTED, trait2.getAffection());
        Assert.assertEquals(Sample.UNSET_QT, trait2.getOtherPhenotype());
        Assert.assertEquals(Affection.UNKNOWN, trait3.getAffection());
        Assert.assertEquals(Sample.UNSET_QT, trait3.getOtherPhenotype());
        Assert.assertEquals(Affection.OTHER, trait4.getAffection());
        Assert.assertEquals("1.0", trait4.getOtherPhenotype());
        Assert.assertEquals("CEU", trait5.getOtherPhenotype());
    }

    @Test()
    public void testGenders() {
        Assert.assertTrue(fam1A.getGender() == Gender.UNKNOWN);
        Assert.assertTrue(fam1B.getGender() == Gender.MALE);
        Assert.assertTrue(fam1C.getGender() == Gender.FEMALE);
    }
}
