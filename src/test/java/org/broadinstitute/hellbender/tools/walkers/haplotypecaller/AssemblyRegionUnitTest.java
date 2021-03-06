package org.broadinstitute.hellbender.tools.walkers.haplotypecaller;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.reference.IndexedFastaSequenceFile;
import org.broadinstitute.hellbender.utils.GenomeLoc;
import org.broadinstitute.hellbender.utils.GenomeLocParser;
import org.broadinstitute.hellbender.utils.GenomeLocSortedSet;
import org.broadinstitute.hellbender.utils.fasta.CachingIndexedFastaSequenceFile;
import org.broadinstitute.hellbender.utils.read.ArtificialReadUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;


public final class AssemblyRegionUnitTest extends BaseTest {
    private final static boolean DEBUG = false;
    private GenomeLocParser genomeLocParser;
    private IndexedFastaSequenceFile seq;
    private String contig;
    private int contigLength;
    private SAMFileHeader header;

    @BeforeClass
    public void init() throws FileNotFoundException {
        // sequence
        seq = new CachingIndexedFastaSequenceFile(new File(hg19MiniReference));
        genomeLocParser = new GenomeLocParser(seq);
        contig = "1";
        contigLength = genomeLocParser.getContigInfo(contig).getSequenceLength();
        header = ArtificialReadUtils.createArtificialSamHeader();
    }

    @Test
    public void testConstructor(){
        final GenomeLoc loc = genomeLocParser.createGenomeLoc("1", 10, 20);
        final AssemblyRegion ar = new AssemblyRegion(loc, genomeLocParser, 2, header);
        Assert.assertEquals(ar.getExtension(), 2);
        Assert.assertEquals(ar.isActive(), true);
        Assert.assertEquals(ar.getSpan(), loc);
    }

    @DataProvider(name = "ActionRegionCreationTest")
    public Object[][] makePollingData() {
        final List<Object[]> tests = new ArrayList<>();
        for ( final int start : Arrays.asList(1, 10, 100, contigLength - 10, contigLength - 1) ) {
            for ( final int size : Arrays.asList(1, 10, 100, 1000) ) {
                for ( final int ext : Arrays.asList(0, 1, 10, 100) ) {
                    for ( final boolean isActive : Arrays.asList(true, false) ) {
                        for ( final boolean addStates : Arrays.asList(true, false) ) {
                            List<ActivityProfileState> states = null;
                            if ( addStates ) {
                                states = new LinkedList<>();
                                for ( int i = start; i < start + size; i++ ) {
                                    states.add(new ActivityProfileState(genomeLocParser.createGenomeLoc(contig, i + start), isActive ? 1.0 : 0.0));
                                }
                            }
                            final GenomeLoc loc = genomeLocParser.createGenomeLoc(contig, start, start + size - 1);
                            tests.add(new Object[]{loc, states, isActive, ext});
                        }
                    }
                }
            }
        }

        return tests.toArray(new Object[][]{});
    }

    @Test(enabled = !DEBUG, dataProvider = "ActionRegionCreationTest")
    public void testCreatingAssemblyRegions(final GenomeLoc loc, final List<ActivityProfileState> supportingStates, final boolean isActive, final int extension) {
        final AssemblyRegion region = new AssemblyRegion(loc, supportingStates, isActive, genomeLocParser, extension, header);
        Assert.assertFalse(region.isFinalized());
        Assert.assertEquals(region.getSpan(), loc);
        Assert.assertEquals(region.getExtendedSpan().getStart(), Math.max(loc.getStart() - extension, 1));
        Assert.assertEquals(region.getExtendedSpan().getStop(), Math.min(loc.getStop() + extension, contigLength));
        Assert.assertEquals(region.getReadSpanLoc().getStart(), Math.max(loc.getStart() - extension, 1));
        Assert.assertEquals(region.getReadSpanLoc().getStop(), Math.min(loc.getStop() + extension, contigLength));
        Assert.assertEquals(region.isActive(), isActive);
        Assert.assertEquals(region.getExtension(), extension);
        Assert.assertEquals(region.getReads(), Collections.emptyList());
        Assert.assertEquals(region.size(), 0);
        Assert.assertEquals(region.getSupportingStates(), supportingStates == null ? Collections.emptyList() : supportingStates);
        Assert.assertNotNull(region.toString());

        assertGoodReferenceGetter(region.getAssemblyRegionReference(seq), region.getExtendedSpan(), 0);
        assertGoodReferenceGetter(region.getAssemblyRegionReference(seq, 0), region.getExtendedSpan(), 0);
        assertGoodReferenceGetter(region.getAssemblyRegionReference(seq, 10), region.getExtendedSpan(), 10);
        assertGoodReferenceGetter(region.getFullReference(seq), region.getReadSpanLoc(), 0);
        assertGoodReferenceGetter(region.getFullReference(seq, 0), region.getReadSpanLoc(), 0);
        assertGoodReferenceGetter(region.getFullReference(seq, 10), region.getReadSpanLoc(), 10);


        region.setFinalized(false);
        Assert.assertFalse(region.isFinalized());
        region.setFinalized(true);
        Assert.assertTrue(region.isFinalized());
        region.setFinalized(false);
        Assert.assertFalse(region.isFinalized());
    }

    private void assertGoodReferenceGetter(final byte[] actualBytes, final GenomeLoc span, final int padding) {
        final int expectedStart = Math.max(span.getStart() - padding, 1);
        final int expectedStop = Math.min(span.getStop() + padding, contigLength);
        final byte[] expectedBytes = seq.getSubsequenceAt(span.getContig(), expectedStart, expectedStop).getBases();
        Assert.assertEquals(actualBytes, expectedBytes);
    }

    @DataProvider(name = "AssemblyRegionReads")
    public Object[][] makeAssemblyRegionReads() {
        final List<Object[]> tests = new ArrayList<>();
        final SAMFileHeader header = ArtificialReadUtils.createArtificialSamHeader(seq.getSequenceDictionary());
        for ( final int start : Arrays.asList(1, 10, 100, contigLength - 10, contigLength - 1) ) {
            for ( final int readStartOffset : Arrays.asList(-100, -10, 0, 10, 100) ) {
                for ( final int readSize : Arrays.asList(10, 100, 1000) ) {
                    final GenomeLoc loc = genomeLocParser.createGenomeLocOnContig(contig, start, start + 10);

                    final int readStart = Math.max(start + readStartOffset, 1);
                    final int readStop = Math.min(readStart + readSize, contigLength);
                    final int readLength = readStop - readStart + 1;
                    if ( readLength > 0 ) {
                        final GATKRead read = ArtificialReadUtils.createArtificialRead(header, "read", 0, readStart, readLength);
                        final GenomeLoc readLoc = genomeLocParser.createGenomeLoc(read);
                        if ( readLoc.overlapsP(loc) )
                            tests.add(new Object[]{loc, read});
                    }
                }
            }
        }

        return tests.subList(2,3).toArray(new Object[][]{});       //HACK!
    }

    @Test(enabled = !DEBUG, dataProvider = "AssemblyRegionReads")
    public void testAssemblyRegionReads(final GenomeLoc loc, final GATKRead read) throws Exception {
        final GenomeLoc expectedSpan = loc.union(genomeLocParser.createGenomeLoc(read));

        final AssemblyRegion region = new AssemblyRegion(loc, null, true, genomeLocParser, 0, header);
        final AssemblyRegion region2 = new AssemblyRegion(loc, null, true, genomeLocParser, 0, header);
        Assert.assertEquals(region.getReads(), Collections.emptyList());
        Assert.assertEquals(region.size(), 0);
        Assert.assertEquals(region.getExtendedSpan(), loc);
        Assert.assertEquals(region.getReadSpanLoc(), loc);
        Assert.assertTrue(region.equalsIgnoreReads(region2));

        region.add(read);
        Assert.assertEquals(region.getReads(), Collections.singletonList(read));
        Assert.assertEquals(region.size(), 1);
        Assert.assertEquals(region.getExtendedSpan(), loc);
        Assert.assertEquals(region.getReadSpanLoc(), expectedSpan);
        Assert.assertTrue(region.equalsIgnoreReads(region2));

        region.clearReads();
        Assert.assertEquals(region.getReads(), Collections.emptyList());
        Assert.assertEquals(region.size(), 0);
        Assert.assertEquals(region.getExtendedSpan(), loc);
        Assert.assertEquals(region.getReadSpanLoc(), loc);
        Assert.assertTrue(region.equalsIgnoreReads(region2));

        region.addAll(Collections.singleton(read));
        Assert.assertEquals(region.getReads(), Collections.singletonList(read));
        Assert.assertEquals(region.size(), 1);
        Assert.assertEquals(region.getExtendedSpan(), loc);
        Assert.assertEquals(region.getReadSpanLoc(), expectedSpan);
        Assert.assertTrue(region.equalsIgnoreReads(region2));

        region.removeAll(Collections.<GATKRead>emptySet());
        Assert.assertEquals(region.getReads(), Collections.singletonList(read));
        Assert.assertEquals(region.size(), 1);
        Assert.assertEquals(region.getExtendedSpan(), loc);
        Assert.assertEquals(region.getReadSpanLoc(), expectedSpan);
        Assert.assertTrue(region.equalsIgnoreReads(region2));

        region.removeAll(Collections.singleton(read));
        Assert.assertEquals(region.getReads(), Collections.emptyList());
        Assert.assertEquals(region.size(), 0);
        Assert.assertEquals(region.getExtendedSpan(), loc);
        Assert.assertEquals(region.getReadSpanLoc(), loc);
        Assert.assertTrue(region.equalsIgnoreReads(region2));

        final GATKRead read2 = read.copy();
        read2.setName(read.getName() + ".clone");

        for ( final GATKRead readToKeep : Arrays.asList(read, read2)) {
            region.addAll(Arrays.asList(read, read2));
            final GATKRead readToDiscard = readToKeep == read ? read2 : read;
            region.removeAll(Collections.singleton(readToDiscard));
            Assert.assertEquals(region.getReads(), Arrays.asList(readToKeep));
            Assert.assertEquals(region.size(), 1);
            Assert.assertEquals(region.getExtendedSpan(), loc);
        }
    }

    // -----------------------------------------------------------------------------------------------
    //
    // Make sure bad inputs are properly detected
    //
    // -----------------------------------------------------------------------------------------------

    @DataProvider(name = "BadReadsTest")
    public Object[][] makeBadReadsTest() {
        List<Object[]> tests = new ArrayList<>();
        final SAMFileHeader header = ArtificialReadUtils.createArtificialSamHeader(seq.getSequenceDictionary());
        tests.add(new Object[]{
                header,
                ArtificialReadUtils.createArtificialRead(header, "read1", 0, 10, 10),
                ArtificialReadUtils.createArtificialRead(header, "read2", 0, 9, 10)});
        tests.add(new Object[]{
                header,
                ArtificialReadUtils.createArtificialRead(header, "read1", 0, 10, 10),
                ArtificialReadUtils.createArtificialRead(header, "read2", 1, 9, 10)});
        tests.add(new Object[]{
                header,
                ArtificialReadUtils.createArtificialRead(header, "read1", 1, 10, 10),
                ArtificialReadUtils.createArtificialRead(header, "read2", 0, 9, 10)});
        return tests.toArray(new Object[][]{});
    }

    @Test(enabled = !DEBUG, dataProvider = "BadReadsTest", expectedExceptions = IllegalArgumentException.class)
    public void testBadReads(final SAMFileHeader header, final GATKRead read1, final GATKRead read2) {
        final GenomeLoc loc = genomeLocParser.createGenomeLoc(read1);
        final AssemblyRegion region = new AssemblyRegion(loc, null, true, genomeLocParser, 0, header);
        region.add(read1);
        region.add(read2);
    }

    // -----------------------------------------------------------------------------------------------
    //
    // Make sure we can properly cut up an active region based on engine intervals
    //
    // -----------------------------------------------------------------------------------------------

    @DataProvider(name = "SplitAssemblyRegion")
    public Object[][] makeSplitAssemblyRegion() {
        final List<Object[]> tests = new ArrayList<>();

        final GenomeLoc whole_span = genomeLocParser.createGenomeLoc("1", 1, 500);
        final GenomeLoc gl_before = genomeLocParser.createGenomeLoc("1", 1, 9);
        final GenomeLoc gl_after = genomeLocParser.createGenomeLoc("1", 250, 500);
        final GenomeLoc gl_diff_contig = genomeLocParser.createGenomeLoc("2", 40, 50);

        final int regionStart = 10;
        final int regionStop = 100;
        final GenomeLoc region = genomeLocParser.createGenomeLoc("1", regionStart, regionStop);

        for ( final GenomeLoc noEffect : Arrays.asList(whole_span) )
            tests.add(new Object[]{
                    region,
                    Arrays.asList(noEffect),
                    Arrays.asList(region)});

        for ( final GenomeLoc noOverlap : Arrays.asList(gl_before, gl_after, gl_diff_contig) )
            tests.add(new Object[]{
                    region,
                    Arrays.asList(noOverlap),
                    Arrays.asList()});

        tests.add(new Object[]{region,
                Arrays.asList(genomeLocParser.createGenomeLoc("1", 5, 50)),
                Arrays.asList(genomeLocParser.createGenomeLoc("1", regionStart, 50))});

        tests.add(new Object[]{region,
                Arrays.asList(genomeLocParser.createGenomeLoc("1", 50, 200)),
                Arrays.asList(genomeLocParser.createGenomeLoc("1", 50, regionStop))});

        tests.add(new Object[]{region,
                Arrays.asList(genomeLocParser.createGenomeLoc("1", 40, 50)),
                Arrays.asList(genomeLocParser.createGenomeLoc("1", 40, 50))});

        tests.add(new Object[]{region,
                Arrays.asList(genomeLocParser.createGenomeLoc("1", 20, 30), genomeLocParser.createGenomeLoc("1", 40, 50)),
                Arrays.asList(genomeLocParser.createGenomeLoc("1", 20, 30), genomeLocParser.createGenomeLoc("1", 40, 50))});

        tests.add(new Object[]{region,
                Arrays.asList(genomeLocParser.createGenomeLoc("1", 1, 30), genomeLocParser.createGenomeLoc("1", 40, 50)),
                Arrays.asList(genomeLocParser.createGenomeLoc("1", regionStart, 30), genomeLocParser.createGenomeLoc("1", 40, 50))});

        tests.add(new Object[]{region,
                Arrays.asList(genomeLocParser.createGenomeLoc("1", 1, 30), genomeLocParser.createGenomeLoc("1", 70, 200)),
                Arrays.asList(genomeLocParser.createGenomeLoc("1", regionStart, 30), genomeLocParser.createGenomeLoc("1", 70, regionStop))});

        tests.add(new Object[]{region,
                Arrays.asList(genomeLocParser.createGenomeLoc("1", 1, 30), genomeLocParser.createGenomeLoc("1", 40, 50), genomeLocParser.createGenomeLoc("1", 70, 200)),
                Arrays.asList(genomeLocParser.createGenomeLoc("1", regionStart, 30), genomeLocParser.createGenomeLoc("1", 40, 50), genomeLocParser.createGenomeLoc("1", 70, regionStop))});

        return tests.toArray(new Object[][]{});
    }

    @Test(dataProvider = "SplitAssemblyRegion")
    public void testSplitAssemblyRegion(final GenomeLoc regionLoc, final List<GenomeLoc> intervalLocs, final List<GenomeLoc> expectedRegionLocs) {
        for ( final boolean addSubstates : Arrays.asList(true, false) ) {
            final List<ActivityProfileState> states;
            if ( addSubstates ) {
                states = new LinkedList<>();
                for ( int i = 0; i < regionLoc.size(); i++ )
                    states.add(new ActivityProfileState(genomeLocParser.createGenomeLoc(regionLoc.getContig(), regionLoc.getStart() + i), 1.0));
            } else {
                states = null;
            }

            final AssemblyRegion region = new AssemblyRegion(regionLoc, states, true, genomeLocParser, 0, header);
            final GenomeLocSortedSet intervals = new GenomeLocSortedSet(genomeLocParser,  intervalLocs);
            final List<AssemblyRegion> regions = region.splitAndTrimToIntervals(intervals);

            Assert.assertEquals(regions.size(), expectedRegionLocs.size(), "Wrong number of split locations");
            for ( int i = 0; i < expectedRegionLocs.size(); i++ ) {
                final GenomeLoc expected = expectedRegionLocs.get(i);
                final AssemblyRegion actual = regions.get(i);
                Assert.assertEquals(actual.getSpan(), expected, "Bad region after split");
                Assert.assertEquals(actual.isActive(), region.isActive());
                Assert.assertEquals(actual.getExtension(), region.getExtension());
            }
        }
    }

    // -----------------------------------------------------------------------------------------------
    //
    // Make sure we can properly cut up an active region based on engine intervals
    //
    // -----------------------------------------------------------------------------------------------

    @DataProvider(name = "TrimAssemblyRegionData")
    public Object[][] makeTrimAssemblyRegionData() {
        final List<Object[]> tests = new ArrayList<>();

        // fully enclosed within active region
        tests.add(new Object[]{
                genomeLocParser.createGenomeLoc("1", 10, 20), 10,
                genomeLocParser.createGenomeLoc("1", 15, 16),
                genomeLocParser.createGenomeLoc("1", 15, 16), 0});

        tests.add(new Object[]{
                genomeLocParser.createGenomeLoc("1", 10, 20), 10,
                genomeLocParser.createGenomeLoc("1", 10, 15),
                genomeLocParser.createGenomeLoc("1", 10, 15), 0});

        tests.add(new Object[]{
                genomeLocParser.createGenomeLoc("1", 10, 20), 10,
                genomeLocParser.createGenomeLoc("1", 15, 20),
                genomeLocParser.createGenomeLoc("1", 15, 20), 0});

        // needs extra padding on the right
        tests.add(new Object[]{
                genomeLocParser.createGenomeLoc("1", 10, 20), 10,
                genomeLocParser.createGenomeLoc("1", 15, 25),
                genomeLocParser.createGenomeLoc("1", 15, 20), 5});

        // needs extra padding on the left
        tests.add(new Object[]{
                genomeLocParser.createGenomeLoc("1", 10, 20), 10,
                genomeLocParser.createGenomeLoc("1", 5, 15),
                genomeLocParser.createGenomeLoc("1", 10, 15), 5});

        // needs extra padding on both
        tests.add(new Object[]{
                genomeLocParser.createGenomeLoc("1", 10, 20), 10,
                genomeLocParser.createGenomeLoc("1", 7, 21),
                genomeLocParser.createGenomeLoc("1", 10, 20), 3});
        tests.add(new Object[]{
                genomeLocParser.createGenomeLoc("1", 10, 20), 10,
                genomeLocParser.createGenomeLoc("1", 9, 23),
                genomeLocParser.createGenomeLoc("1", 10, 20), 3});

        // desired span captures everything, so we're returning everything.  Tests that extension is set correctly
        tests.add(new Object[]{
                genomeLocParser.createGenomeLoc("1", 10, 20), 10,
                genomeLocParser.createGenomeLoc("1", 1, 50),
                genomeLocParser.createGenomeLoc("1", 10, 20), 10});

        // At the start of the chromosome, potentially a bit weird
        tests.add(new Object[]{
                genomeLocParser.createGenomeLoc("1", 1, 10), 10,
                genomeLocParser.createGenomeLoc("1", 1, 50),
                genomeLocParser.createGenomeLoc("1", 1, 10), 10});

        return tests.toArray(new Object[][]{});
    }

    @Test(dataProvider = "TrimAssemblyRegionData")
    public void testTrimAssemblyRegion(final GenomeLoc regionLoc, final int extension, final GenomeLoc desiredSpan, final GenomeLoc expectedAssemblyRegion, final int expectedExtension) {
        final AssemblyRegion region = new AssemblyRegion(regionLoc, Collections.<ActivityProfileState>emptyList(), true, genomeLocParser, extension, header);
        final AssemblyRegion trimmed = region.trim(desiredSpan);
        Assert.assertEquals(trimmed.getSpan(), expectedAssemblyRegion, "Incorrect region");
        Assert.assertEquals(trimmed.getExtension(), expectedExtension, "Incorrect region");
    }
}