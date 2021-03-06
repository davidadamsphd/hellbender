package org.broadinstitute.hellbender.utils.read;

import htsjdk.samtools.Cigar;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMReadGroupRecord;
import htsjdk.samtools.TextCigarCodec;
import htsjdk.samtools.reference.IndexedFastaSequenceFile;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.broadinstitute.hellbender.utils.BaseUtils;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.fasta.CachingIndexedFastaSequenceFile;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;


public final class ReadUtilsUnitTest extends BaseTest {
    private interface GetAdaptorFunc {
        public int getAdaptor(final GATKRead record);
    }

    @DataProvider(name = "AdaptorGetter")
    public Object[][] makeActiveRegionCutTests() {
        final List<Object[]> tests = new LinkedList<>();

        tests.add( new Object[]{ new GetAdaptorFunc() {
            @Override public int getAdaptor(final GATKRead record) { return ReadUtils.getAdaptorBoundary(record); }
        }});

        tests.add( new Object[]{ new GetAdaptorFunc() {
            @Override public int getAdaptor(final GATKRead record) { return ReadUtils.getAdaptorBoundary(record); }
        }});

        return tests.toArray(new Object[][]{});
    }

    private GATKRead makeRead(final int fragmentSize, final int mateStart) {
        final byte[] bases = {'A', 'C', 'G', 'T', 'A', 'C', 'G', 'T'};
        final byte[] quals = {30, 30, 30, 30, 30, 30, 30, 30};
        final String cigar = "8M";
        GATKRead read = ArtificialReadUtils.createArtificialRead(bases, quals, cigar);
        read.setIsProperlyPaired(true);
        read.setIsPaired(true);
        read.setMatePosition(read.getContig(), mateStart);
        read.setFragmentLength(fragmentSize);
        return read;
    }

    @Test(dataProvider = "AdaptorGetter")
    public void testGetAdaptorBoundary(final GetAdaptorFunc get) {
        final int fragmentSize = 10;
        final int mateStart = 1000;
        final int BEFORE = mateStart - 2;
        final int AFTER = mateStart + 2;
        int myStart, boundary;
        GATKRead read;

        // Test case 1: positive strand, first read
        read = makeRead(fragmentSize, mateStart);
        myStart = BEFORE;
        read.setPosition(read.getContig(), myStart);
        read.setIsReverseStrand(false);
        read.setMateIsReverseStrand(true);
        boundary = get.getAdaptor(read);
        Assert.assertEquals(boundary, myStart + fragmentSize + 1);

        // Test case 2: positive strand, second read
        read = makeRead(fragmentSize, mateStart);
        myStart = AFTER;
        read.setPosition(read.getContig(), myStart);
        read.setIsReverseStrand(false);
        read.setMateIsReverseStrand(true);
        boundary = get.getAdaptor(read);
        Assert.assertEquals(boundary, myStart + fragmentSize + 1);

        // Test case 3: negative strand, second read
        read = makeRead(fragmentSize, mateStart);
        myStart = AFTER;
        read.setPosition(read.getContig(), myStart);
        read.setIsReverseStrand(true);
        read.setMateIsReverseStrand(false);
        boundary = get.getAdaptor(read);
        Assert.assertEquals(boundary, mateStart - 1);

        // Test case 4: negative strand, first read
        read = makeRead(fragmentSize, mateStart);
        myStart = BEFORE;
        read.setPosition(read.getContig(), myStart);
        read.setIsReverseStrand(true);
        read.setMateIsReverseStrand(false);
        boundary = get.getAdaptor(read);
        Assert.assertEquals(boundary, mateStart - 1);

        // Test case 5: mate is mapped to another chromosome (test both strands)
        read = makeRead(fragmentSize, mateStart);
        read.setFragmentLength(0);
        read.setIsReverseStrand(true);
        read.setMateIsReverseStrand(false);
        boundary = get.getAdaptor(read);
        Assert.assertEquals(boundary, ReadUtils.CANNOT_COMPUTE_ADAPTOR_BOUNDARY);
        read.setIsReverseStrand(false);
        read.setMateIsReverseStrand(true);
        boundary = get.getAdaptor(read);
        Assert.assertEquals(boundary, ReadUtils.CANNOT_COMPUTE_ADAPTOR_BOUNDARY);
        read.setFragmentLength(10);

        // Test case 6: read is unmapped
        read = makeRead(fragmentSize, mateStart);
        read.setIsUnmapped();
        boundary = get.getAdaptor(read);
        Assert.assertEquals(boundary, ReadUtils.CANNOT_COMPUTE_ADAPTOR_BOUNDARY);

        // Test case 7:  reads don't overlap and look like this:
        //    <--------|
        //                 |------>
        // first read:
        read = makeRead(fragmentSize, mateStart);
        myStart = 980;
        read.setPosition(read.getContig(), myStart);
        read.setFragmentLength(20);
        read.setIsReverseStrand(true);
        boundary = get.getAdaptor(read);
        Assert.assertEquals(boundary, ReadUtils.CANNOT_COMPUTE_ADAPTOR_BOUNDARY);

        // second read:
        read = makeRead(fragmentSize, mateStart);
        myStart = 1000;
        read.setPosition(read.getContig(), myStart);
        read.setFragmentLength(20);
        read.setMatePosition(read.getContig(), 980);
        read.setIsReverseStrand(false);
        boundary = get.getAdaptor(read);
        Assert.assertEquals(boundary, ReadUtils.CANNOT_COMPUTE_ADAPTOR_BOUNDARY);

        // Test case 8: read doesn't have proper pair flag set
        read = makeRead(fragmentSize, mateStart);
        read.setIsPaired(true);
        read.setIsProperlyPaired(false);
        Assert.assertEquals(get.getAdaptor(read), ReadUtils.CANNOT_COMPUTE_ADAPTOR_BOUNDARY);

        // Test case 9: read and mate have same negative flag setting
        for ( final boolean negFlag: Arrays.asList(true, false) ) {
            read = makeRead(fragmentSize, mateStart);
            read.setPosition(read.getContig(), BEFORE);
            read.setIsPaired(true);
            read.setIsProperlyPaired(true);
            read.setIsReverseStrand(negFlag);
            read.setMateIsReverseStrand(!negFlag);
            Assert.assertTrue(get.getAdaptor(read) != ReadUtils.CANNOT_COMPUTE_ADAPTOR_BOUNDARY, "Get adaptor should have succeeded");

            read = makeRead(fragmentSize, mateStart);
            read.setPosition(read.getContig(), BEFORE);
            read.setIsPaired(true);
            read.setIsProperlyPaired(true);
            read.setIsReverseStrand(negFlag);
            read.setMateIsReverseStrand(negFlag);
            Assert.assertEquals(get.getAdaptor(read), ReadUtils.CANNOT_COMPUTE_ADAPTOR_BOUNDARY, "Get adaptor should have failed for reads with bad alignment orientation");
        }
    }

    @Test
    public void testGetBasesReverseComplement() {
        int iterations = 1000;
        Random random = Utils.getRandomGenerator();
        while(iterations-- > 0) {
            final int l = random.nextInt(1000);
            GATKRead read = ArtificialReadUtils.createRandomRead(l);
            byte [] original = read.getBases();
            byte [] reconverted = new byte[l];
            String revComp = ReadUtils.getBasesReverseComplement(read);
            for (int i=0; i<l; i++) {
                reconverted[l-1-i] = BaseUtils.getComplement((byte) revComp.charAt(i));
            }
            Assert.assertEquals(reconverted, original);
        }
    }

    @Test
    public void testGetMaxReadLength() {
        for( final int minLength : Arrays.asList( 5, 30, 50 ) ) {
            for( final int maxLength : Arrays.asList( 50, 75, 100 ) ) {
                final List<GATKRead> reads = new ArrayList<>();
                for( int readLength = minLength; readLength <= maxLength; readLength++ ) {
                    reads.add( ArtificialReadUtils.createRandomRead( readLength ) );
                }
                Assert.assertEquals(ReadUtils.getMaxReadLength(reads), maxLength, "max length does not match");
            }
        }

        final List<GATKRead> reads = new LinkedList<>();
        Assert.assertEquals(ReadUtils.getMaxReadLength(reads), 0, "Empty list should have max length of zero");
    }

    @Test
    public void testReadWithNsRefIndexInDeletion() throws FileNotFoundException {

        final IndexedFastaSequenceFile seq = new CachingIndexedFastaSequenceFile(new File(exampleReference));
        final SAMFileHeader header = ArtificialReadUtils.createArtificialSamHeader(seq.getSequenceDictionary());
        final int readLength = 76;

        final GATKRead read = ArtificialReadUtils.createArtificialRead(header, "myRead", 0, 8975, readLength);
        read.setBases(Utils.dupBytes((byte) 'A', readLength));
        read.setBaseQualities(Utils.dupBytes((byte)30, readLength));
        read.setCigar("3M414N1D73M");

        final int result = ReadUtils.getReadCoordinateForReferenceCoordinateUpToEndOfRead(read, 9392, ReadUtils.ClippingTail.LEFT_TAIL);
        Assert.assertEquals(result, 2);
    }

    @Test
    public void testReadWithNsRefAfterDeletion() throws FileNotFoundException {

        final IndexedFastaSequenceFile seq = new CachingIndexedFastaSequenceFile(new File(exampleReference));
        final SAMFileHeader header = ArtificialReadUtils.createArtificialSamHeader(seq.getSequenceDictionary());
        final int readLength = 76;

        final GATKRead read = ArtificialReadUtils.createArtificialRead(header, "myRead", 0, 8975, readLength);
        read.setBases(Utils.dupBytes((byte) 'A', readLength));
        read.setBaseQualities(Utils.dupBytes((byte)30, readLength));
        read.setCigar("3M414N1D73M");

        final int result = ReadUtils.getReadCoordinateForReferenceCoordinateUpToEndOfRead(read, 9393, ReadUtils.ClippingTail.LEFT_TAIL);
        Assert.assertEquals(result, 3);
    }

    @DataProvider(name = "HasWellDefinedFragmentSizeData")
    public Object[][] makeHasWellDefinedFragmentSizeData() throws Exception {
        final List<Object[]> tests = new LinkedList<>();

        // setup a basic read that will work
        final SAMFileHeader header = ArtificialReadUtils.createArtificialSamHeader();
        final GATKRead read = ArtificialReadUtils.createArtificialRead(header, "read1", 0, 10, 10);
        read.setIsPaired(true);
        read.setIsProperlyPaired(true);
        read.setPosition(read.getContig(), 100);
        read.setCigar("50M");
        read.setMatePosition(read.getContig(), 130);
        read.setFragmentLength(80);
        read.setIsFirstOfPair();
        read.setIsReverseStrand(false);
        read.setMateIsReverseStrand(true);

        tests.add( new Object[]{ "basic case", read.copy(), true });

        {
            final GATKRead bad1 = read.copy();
            bad1.setIsPaired(false);
            tests.add( new Object[]{ "not paired", bad1, false });
        }

        {
            final GATKRead bad = read.copy();
            bad.setIsProperlyPaired(false);
            // we currently don't require the proper pair flag to be set
            tests.add( new Object[]{ "not proper pair", bad, true });
//            tests.add( new Object[]{ "not proper pair", bad, false });
        }

        {
            final GATKRead bad = read.copy();
            bad.setIsUnmapped();
            tests.add( new Object[]{ "read is unmapped", bad, false });
        }

        {
            final GATKRead bad = read.copy();
            bad.setMateIsUnmapped();
            tests.add( new Object[]{ "mate is unmapped", bad, false });
        }

        {
            final GATKRead bad = read.copy();
            bad.setMateIsReverseStrand(false);
            tests.add( new Object[]{ "read and mate both on positive strand", bad, false });
        }

        {
            final GATKRead bad = read.copy();
            bad.setIsReverseStrand(true);
            tests.add( new Object[]{ "read and mate both on negative strand", bad, false });
        }

        {
            final GATKRead bad = read.copy();
            bad.setFragmentLength(0);
            tests.add( new Object[]{ "insert size is 0", bad, false });
        }

        {
            final GATKRead bad = read.copy();
            bad.setPosition(bad.getContig(), 1000);
            tests.add( new Object[]{ "positve read starts after mate end", bad, false });
        }

        {
            final GATKRead bad = read.copy();
            bad.setIsReverseStrand(true);
            bad.setMateIsReverseStrand(false);
            bad.setMatePosition(bad.getMateContig(), 1000);
            tests.add( new Object[]{ "negative strand read ends before mate starts", bad, false });
        }

        return tests.toArray(new Object[][]{});
    }

    @Test(dataProvider = "HasWellDefinedFragmentSizeData")
    private void testHasWellDefinedFragmentSize(final String name, final GATKRead read, final boolean expected) {
        Assert.assertEquals(ReadUtils.hasWellDefinedFragmentSize(read), expected);
    }

    @DataProvider(name = "ReadsWithReadGroupData")
    public Object[][] readsWithReadGroupData() {
        final SAMFileHeader header = ArtificialReadUtils.createArtificialSamHeader(2, 1, 1000000);
        final SAMReadGroupRecord readGroup = new SAMReadGroupRecord("FOO");
        readGroup.setPlatform("FOOPLATFORM");
        readGroup.setPlatformUnit("FOOPLATFORMUNIT");
        readGroup.setLibrary("FOOLIBRARY");
        readGroup.setSample("FOOSAMPLE");
        header.addReadGroup(readGroup);

        final GATKRead googleBackedRead = new GoogleGenomicsReadToGATKReadAdapter(ArtificialReadUtils.createArtificialGoogleGenomicsRead("google", "1", 5, new byte[]{'A', 'C', 'G', 'T'}, new byte[]{1, 2, 3, 4}, "4M"));
        googleBackedRead.setReadGroup("FOO");

        final GATKRead samBackedRead = new SAMRecordToGATKReadAdapter(ArtificialReadUtils.createArtificialSAMRecord(header, "sam", header.getSequenceIndex("1"), 5, new byte[]{'A', 'C', 'G', 'T'}, new byte[]{1, 2, 3, 4}, "4M"));
        samBackedRead.setReadGroup("FOO");

        return new Object[][] {
                { googleBackedRead, header, "FOO" },
                { samBackedRead, header, "FOO" }
        };
    }

    @Test(dataProvider = "ReadsWithReadGroupData")
    public void testReadGroupOperations( final GATKRead read, final SAMFileHeader header, final String expectedReadGroupID ) {
        final SAMReadGroupRecord readGroup = ReadUtils.getSAMReadGroupRecord(read, header);
        Assert.assertEquals(readGroup.getId(), expectedReadGroupID, "Wrong read group returned from ReadUtils.getSAMReadGroupRecord()");

        Assert.assertEquals(ReadUtils.getPlatform(read, header), readGroup.getPlatform(), "Wrong platform returned from ReadUtils.getPlatform()");
        Assert.assertEquals(ReadUtils.getPlatformUnit(read, header), readGroup.getPlatformUnit(), "Wrong platform unit returned from ReadUtils.getPlatformUnit()");
        Assert.assertEquals(ReadUtils.getLibrary(read, header), readGroup.getLibrary(), "Wrong library returned from ReadUtils.getLibrary()");
        Assert.assertEquals(ReadUtils.getSampleName(read, header), readGroup.getSample(), "Wrong sample name returned from ReadUtils.getSampleName()");
    }

    @Test(dataProvider = "ReadsWithReadGroupData")
    public void testReadGroupOperationsOnReadWithNoReadGroup( final GATKRead read, final SAMFileHeader header, final String expectedReadGroupID ) {
        read.setReadGroup(null);

        Assert.assertNull(ReadUtils.getSAMReadGroupRecord(read, header), "Read group should be null");
        Assert.assertNull(ReadUtils.getPlatform(read, header), "Platform should be null");
        Assert.assertNull(ReadUtils.getPlatformUnit(read, header), "Platform unit should be null");
        Assert.assertNull(ReadUtils.getLibrary(read, header), "Library should be null");
        Assert.assertNull(ReadUtils.getSampleName(read, header), "Sample name should be null");
    }

    @DataProvider(name = "ReferenceIndexTestData")
    public Object[][] referenceIndexTestData() {
        final SAMFileHeader header = ArtificialReadUtils.createArtificialSamHeader(2, 1, 1000000);

        final GATKRead googleBackedRead = new GoogleGenomicsReadToGATKReadAdapter(ArtificialReadUtils.createArtificialGoogleGenomicsRead("google", "2", 5, new byte[]{'A', 'C', 'G', 'T'}, new byte[]{1, 2, 3, 4}, "4M"));
        googleBackedRead.setMatePosition("1", 1);

        final GATKRead samBackedRead = new SAMRecordToGATKReadAdapter(ArtificialReadUtils.createArtificialSAMRecord(header, "sam", header.getSequenceIndex("2"), 5, new byte[]{'A', 'C', 'G', 'T'}, new byte[]{1, 2, 3, 4}, "4M"));
        samBackedRead.setMatePosition("1", 5);

        final GATKRead unmappedGoogleBackedRead = new GoogleGenomicsReadToGATKReadAdapter(ArtificialReadUtils.createArtificialGoogleGenomicsRead("google", "1", 5, new byte[]{'A', 'C', 'G', 'T'}, new byte[]{1, 2, 3, 4}, "4M"));
        unmappedGoogleBackedRead.setIsUnmapped();
        unmappedGoogleBackedRead.setMateIsUnmapped();

        final GATKRead unmappedSamBackedRead = new SAMRecordToGATKReadAdapter(ArtificialReadUtils.createArtificialSAMRecord(header, "sam", header.getSequenceIndex("1"), 5, new byte[]{'A', 'C', 'G', 'T'}, new byte[]{1, 2, 3, 4}, "4M"));
        unmappedSamBackedRead.setIsUnmapped();
        unmappedSamBackedRead.setMateIsUnmapped();

        return new Object[][] {
                { googleBackedRead, header, 1, 0 },
                { samBackedRead, header, 1, 0 },
                { unmappedGoogleBackedRead, header, -1, -1 },
                { unmappedSamBackedRead, header, -1, -1 }
        };
    }

    @Test(dataProvider = "ReferenceIndexTestData")
    public void testGetReferenceIndex( final GATKRead read, final SAMFileHeader header, final int expectedReferenceIndex, final int expectedMateReferenceIndex ) {
        Assert.assertEquals(ReadUtils.getReferenceIndex(read, header), expectedReferenceIndex, "Wrong reference index for read");
        Assert.assertEquals(ReadUtils.getMateReferenceIndex(read, header), expectedMateReferenceIndex, "Wrong reference index for read's mate");
    }
}
