package org.broadinstitute.hellbender.utils.pairhmm;

import com.google.common.annotations.VisibleForTesting;
import htsjdk.variant.variantcontext.Allele;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.broadinstitute.hellbender.utils.MathUtils;
import org.broadinstitute.hellbender.utils.genotyper.LikelihoodMatrix;
import org.broadinstitute.hellbender.utils.haplotype.Haplotype;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.ReadUtils;

import java.io.Closeable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Class for performing the pair HMM for local alignment. Figure 4.3 in Durbin 1998 book.
 */
public abstract class PairHMM implements Closeable{
    protected final static Logger logger = LogManager.getLogger(PairHMM.class);

    protected boolean constantsAreInitialized = false;

    protected byte[] previousHaplotypeBases;
    protected int hapStartIndex;

    public enum HMM_IMPLEMENTATION {
        /* Very slow implementation which uses very accurate log10 sum functions. Only meant to be used as a reference test implementation */
        EXACT,
        /* PairHMM as implemented for the UnifiedGenotyper. Uses log10 sum functions accurate to only 1E-4 */
        ORIGINAL,
        /* Optimized version of the PairHMM which caches per-read computations and operations in real space to avoid costly sums of log10'ed likelihoods */
        LOGLESS_CACHING,
    }

    protected int maxHaplotypeLength, maxReadLength;
    protected int paddedMaxReadLength, paddedMaxHaplotypeLength;
    protected int paddedReadLength, paddedHaplotypeLength;
    protected boolean initialized = false;

    // only used for debugging purposes
    protected boolean doNotUseTristateCorrection = false;
    protected void doNotUseTristateCorrection() { doNotUseTristateCorrection = true; }

    //debug array
    protected double[] mLikelihoodArray;

    //profiling information
    protected static Boolean doProfiling = true;
    protected static long pairHMMComputeTime = 0;
    protected long threadLocalPairHMMComputeTimeDiff = 0;
    protected long startTime = 0;

    /**
     * Initialize this PairHMM, making it suitable to run against a read and haplotype with given lengths
     *
     * Note: Do not worry about padding, just provide the true max length of the read and haplotype. The HMM will take care of the padding.
     *
     * @param haplotypeMaxLength the max length of haplotypes we want to use with this PairHMM
     * @param readMaxLength the max length of reads we want to use with this PairHMM
     * @throws IllegalArgumentException if readMaxLength or haplotypeMaxLength is less than or equal to zero
     */
    public void initialize( final int readMaxLength, final int haplotypeMaxLength ) throws IllegalArgumentException {
        if ( readMaxLength <= 0 ) throw new IllegalArgumentException("READ_MAX_LENGTH must be > 0 but got " + readMaxLength);
        if ( haplotypeMaxLength <= 0 ) throw new IllegalArgumentException("HAPLOTYPE_MAX_LENGTH must be > 0 but got " + haplotypeMaxLength);

        maxHaplotypeLength = haplotypeMaxLength;
        maxReadLength = readMaxLength;

        // M, X, and Y arrays are of size read and haplotype + 1 because of an extra column for initial conditions and + 1 to consider the final base in a non-global alignment
        paddedMaxReadLength = readMaxLength + 1;
        paddedMaxHaplotypeLength = haplotypeMaxLength + 1;

        previousHaplotypeBases = null;

        constantsAreInitialized = false;
        initialized = true;
    }

    /**
     * Initialize this PairHMM, making it suitable to run against a read and haplotype with given lengths
     * This function is used by the JNI implementations to transfer all data once to the native code
     * @param haplotypes the list of haplotypes
     * @param perSampleReadList map from sample name to list of reads
     * @param haplotypeMaxLength the max length of haplotypes we want to use with this PairHMM
     * @param readMaxLength the max length of reads we want to use with this PairHMM
     */
    public void initialize( final List<Haplotype> haplotypes, final Map<String, List<GATKRead>> perSampleReadList, final int readMaxLength, final int haplotypeMaxLength ) {
        initialize(readMaxLength, haplotypeMaxLength);
    }

    private static int findMaxAlleleLength(final List<? extends Allele> alleles) {
        int max = 0;
        for (final Allele allele : alleles) {
            final int alleleLength = allele.length();
            if (max < alleleLength) {
                max = alleleLength;
            }
        }
        return max;
    }

    static int findMaxReadLength(final List<GATKRead> reads) {
        int listMaxReadLength = 0;
        for(final GATKRead read : reads){
            final int readLength = read.getLength();
            if( readLength > listMaxReadLength ) {
                listMaxReadLength = readLength;
            }
        }
        return listMaxReadLength;
    }

    /**
     *  Given a list of reads and haplotypes, for every read compute the total probability of said read arising from
     *  each haplotype given base substitution, insertion, and deletion probabilities.
     *
     * @param processedReads reads to analyze instead of the ones present in the destination read-likelihoods.
     * @param likelihoods where to store the likelihoods where position [a][r] is reserved for the likelihood of {@code reads[r]}
     *             conditional to {@code alleles[a]}.
     * @param gcp penalty for gap continuations base array map for processed reads.
     *
     * @return never {@code null}.
     */
    public void computeLikelihoods(final LikelihoodMatrix<Haplotype> likelihoods,
                                   final List<GATKRead> processedReads,
                                   final Map<GATKRead, byte[]> gcp) {
        if (processedReads.isEmpty()) {
            return;
        }
        if(doProfiling) {
            startTime = System.nanoTime();
        }
        // (re)initialize the pairHMM only if necessary
        final int readMaxLength = findMaxReadLength(processedReads);
        final int haplotypeMaxLength = findMaxAlleleLength(likelihoods.alleles());
        if (!initialized || readMaxLength > maxReadLength || haplotypeMaxLength > maxHaplotypeLength) {
            initialize(readMaxLength, haplotypeMaxLength);
        }

        final int readCount = processedReads.size();
        final List<Haplotype> alleles = likelihoods.alleles();
        final int alleleCount = alleles.size();
        mLikelihoodArray = new double[readCount * alleleCount];
        int idx = 0;
        int readIndex = 0;
        for(final GATKRead read : processedReads){
            final byte[] readBases = read.getBases();
            final byte[] readQuals = read.getBaseQualities();
            final byte[] readInsQuals = ReadUtils.getBaseInsertionQualities(read);
            final byte[] readDelQuals = ReadUtils.getBaseDeletionQualities(read);
            final byte[] overallGCP = gcp.get(read);

            // peak at the next haplotype in the list (necessary to get nextHaplotypeBases, which is required for caching in the array implementation)
            final boolean isFirstHaplotype = true;
            for (int a = 0; a < alleleCount; a++) {
                final Allele allele = alleles.get(a);
                final byte[] alleleBases = allele.getBases();
                final byte[] nextAlleleBases = a == alleles.size() - 1 ? null : alleles.get(a + 1).getBases();
                final double lk = computeReadLikelihoodGivenHaplotypeLog10(alleleBases,
                        readBases, readQuals, readInsQuals, readDelQuals, overallGCP, isFirstHaplotype, nextAlleleBases);
                likelihoods.set(a, readIndex, lk);
                mLikelihoodArray[idx++] = lk;
            }
            readIndex++;
        }
        if(doProfiling) {
            threadLocalPairHMMComputeTimeDiff = (System.nanoTime() - startTime);
            {
                pairHMMComputeTime += threadLocalPairHMMComputeTimeDiff;
            }
        }
    }

    /**
     * Compute the total probability of read arising from haplotypeBases given base substitution, insertion, and deletion
     * probabilities.
     *
     * Note on using hapStartIndex.  This allows you to compute the exact true likelihood of a full haplotypes
     * given a read, assuming that the previous calculation read over a full haplotype, recaching the read values,
     * starting only at the place where the new haplotype bases and the previous haplotype bases different.  This
     * index is 0-based, and can be computed with findFirstPositionWhereHaplotypesDiffer given the two haplotypes.
     * Note that this assumes that the read and all associated quals values are the same.
     *
     * @param haplotypeBases the full sequence (in standard SAM encoding) of the haplotype, must be >= than read bases in length
     * @param readBases the bases (in standard encoding) of the read, must be <= haplotype bases in length
     * @param readQuals the phred-scaled per base substitution quality scores of read.  Must be the same length as readBases
     * @param insertionGOP the phred-scaled per base insertion quality scores of read.  Must be the same length as readBases
     * @param deletionGOP the phred-scaled per base deletion quality scores of read.  Must be the same length as readBases
     * @param overallGCP the phred-scaled gap continuation penalties scores of read.  Must be the same length as readBases
     * @param recacheReadValues if false, we don't recalculate any cached results, assuming that readBases and its associated
     *                          parameters are the same, and only the haplotype bases are changing underneath us
     * @throws IllegalStateException  if did not call initialize() beforehand
     * @throws IllegalArgumentException haplotypeBases is null or greater than maxHaplotypeLength
     * @throws IllegalArgumentException readBases is null or greater than maxReadLength
     * @throws IllegalArgumentException readBases, readQuals, insertionGOP, deletionGOP and overallGCP are not the same size
     * @return the log10 probability of read coming from the haplotype under the provided error model
     */
    @VisibleForTesting
    double computeReadLikelihoodGivenHaplotypeLog10( final byte[] haplotypeBases,
                                                                  final byte[] readBases,
                                                                  final byte[] readQuals,
                                                                  final byte[] insertionGOP,
                                                                  final byte[] deletionGOP,
                                                                  final byte[] overallGCP,
                                                                  final boolean recacheReadValues,
                                                                  final byte[] nextHaploytpeBases) throws IllegalStateException, IllegalArgumentException {

        if ( ! initialized ) throw new IllegalStateException("Must call initialize before calling computeReadLikelihoodGivenHaplotypeLog10");
        if ( haplotypeBases == null ) throw new IllegalArgumentException("haplotypeBases cannot be null");
        if ( haplotypeBases.length > maxHaplotypeLength ) throw new IllegalArgumentException("Haplotype bases is too long, got " + haplotypeBases.length + " but max is " + maxHaplotypeLength);
        if ( readBases == null ) throw new IllegalArgumentException("readBases cannot be null");
        if ( readBases.length > maxReadLength ) throw new IllegalArgumentException("readBases is too long, got " + readBases.length + " but max is " + maxReadLength);
        if ( readQuals.length != readBases.length ) throw new IllegalArgumentException("Read bases and read quals aren't the same size: " + readBases.length + " vs " + readQuals.length);
        if ( insertionGOP.length != readBases.length ) throw new IllegalArgumentException("Read bases and read insertion quals aren't the same size: " + readBases.length + " vs " + insertionGOP.length);
        if ( deletionGOP.length != readBases.length ) throw new IllegalArgumentException("Read bases and read deletion quals aren't the same size: " + readBases.length + " vs " + deletionGOP.length);
        if ( overallGCP.length != readBases.length ) throw new IllegalArgumentException("Read bases and overall GCP aren't the same size: " + readBases.length + " vs " + overallGCP.length);

        paddedReadLength = readBases.length + 1;
        paddedHaplotypeLength = haplotypeBases.length + 1;

        hapStartIndex =  (recacheReadValues) ? 0 : hapStartIndex;

        // Pre-compute the difference between the current haplotype and the next one to be run
        // Looking ahead is necessary for the ArrayLoglessPairHMM implementation
        final int nextHapStartIndex =  (nextHaploytpeBases == null || haplotypeBases.length != nextHaploytpeBases.length) ? 0 : findFirstPositionWhereHaplotypesDiffer(haplotypeBases, nextHaploytpeBases);

        final double result = subComputeReadLikelihoodGivenHaplotypeLog10(haplotypeBases, readBases, readQuals, insertionGOP, deletionGOP, overallGCP, hapStartIndex, recacheReadValues, nextHapStartIndex);

        if ( result > 0.0) {
            throw new IllegalStateException("PairHMM Log Probability cannot be greater than 0: " + String.format("haplotype: %s, read: %s, result: %f, PairHMM: %s", new String(haplotypeBases), new String(readBases), result, this.getClass().getSimpleName()));
        } else if (!MathUtils.goodLog10Probability(result)) {
            throw new IllegalStateException("Invalid Log Probability: " + result);
        }

        // Warning: This assumes no downstream modification of the haplotype bases (saves us from copying the array). It is okay for the haplotype caller.
        previousHaplotypeBases = haplotypeBases;

        // For the next iteration, the hapStartIndex for the next haploytpe becomes the index for the current haplotype
        // The array implementation has to look ahead to the next haplotype to store caching info. It cannot do this if nextHapStart is before hapStart
        hapStartIndex = (nextHapStartIndex < hapStartIndex) ? 0: nextHapStartIndex;

        return result;
    }

    /**
     * To be implemented by subclasses to do calculation for #computeReadLikelihoodGivenHaplotypeLog10
     */
    protected abstract double subComputeReadLikelihoodGivenHaplotypeLog10( final byte[] haplotypeBases,
                                                                           final byte[] readBases,
                                                                           final byte[] readQuals,
                                                                           final byte[] insertionGOP,
                                                                           final byte[] deletionGOP,
                                                                           final byte[] overallGCP,
                                                                           final int hapStartIndex,
                                                                           final boolean recacheReadValues,
                                                                           final int nextHapStartIndex);

    /**
     * Compute the first position at which two haplotypes differ
     *
     * If the haplotypes are exact copies of each other, returns the min length of the two haplotypes.
     *
     * @param haplotype1 the first haplotype1
     * @param haplotype2 the second haplotype1
     * @throws IllegalArgumentException if haplotype1 or haplotype2 are null or zero length
     * @return the index of the first position in haplotype1 and haplotype2 where the byte isn't the same
     */
    public static int findFirstPositionWhereHaplotypesDiffer(final byte[] haplotype1, final byte[] haplotype2) throws IllegalArgumentException {
        if ( haplotype1 == null || haplotype1.length == 0 ) throw new IllegalArgumentException("Haplotype1 is bad " + Arrays.toString(haplotype1));
        if ( haplotype2 == null || haplotype2.length == 0 ) throw new IllegalArgumentException("Haplotype2 is bad " + Arrays.toString(haplotype2));

        for( int i = 0; i < haplotype1.length && i < haplotype2.length; i++ ) {
            if( haplotype1[i] != haplotype2[i] ) {
                return i;
            }
        }

        return Math.min(haplotype1.length, haplotype2.length);
    }

    /**
     * Return the results of the computeLikelihoods function
     */
    public double[] getLikelihoodArray() {
        return mLikelihoodArray;
    }

    /**
     * Called at the end of the program to close files, print profiling information etc 
     */
    @Override
    public void close() {
        if(doProfiling)
            logger.info("Total compute time in PairHMM computeLikelihoods() : "+(pairHMMComputeTime*1e-9));
    }
}
