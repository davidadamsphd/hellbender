package org.broadinstitute.hellbender.engine.filters;

import org.broadinstitute.hellbender.utils.QualityUtils;
import org.broadinstitute.hellbender.utils.read.CigarUtils;

/**
 * Standard ReadFilters
 */
public final class ReadFilterLibrary {

    private ReadFilterLibrary(){ /*no instance*/ }

    public static final ReadFilter ALLOW_ALL_READS = read -> true;

    public static final ReadFilter MAPPED =  read -> ! read.isUnmapped();
    public static final ReadFilter PRIMARY_ALIGNMENT = read -> ! read.isNonPrimaryAlignment();
    public static final ReadFilter NOT_DUPLICATE = read -> ! read.isDuplicate();
    public static final ReadFilter PASSES_VENDOR_QUALITY_CHECK = read -> ! read.failsVendorQualityCheck();
    public static final ReadFilter MAPPING_QUALITY_AVAILABLE = read -> read.getMappingQuality() != QualityUtils.MAPPING_QUALITY_UNAVAILABLE;
    public static final ReadFilter MAPPING_QUALITY_NOT_ZERO = read -> read.getMappingQuality() != 0;
    public static final ReadFilter READLENGTH_EQUALS_CIGARLENGTH = read -> read.getLength() == read.getCigar().getReadLength();
    public static final ReadFilter GOOD_CIGAR =  read -> CigarUtils.isGood(read.getCigar());
    public static final ReadFilter NON_ZERO_REFERENCE_LENGTH_ALIGNMENT = read -> read.getCigar().getCigarElements().stream().anyMatch(c -> c.getOperator().consumesReferenceBases() && c.getLength() > 0);

    /**
     * Reads whose mate maps to the same contig.
     */
    public static final ReadFilter MATE_ON_SAME_CONTIG = read -> read.isPaired() && ! read.isUnmapped() && ! read.mateIsUnmapped() && read.getContig().equals(read.getMateContig());

    /**
     * Reads that have a mapped mate and both mate and read are on different same strands (ie the usual situation).
     */
    public static final ReadFilter MATE_DIFFERENT_STRAND = read -> read.isPaired() && ! read.isUnmapped() && ! read.mateIsUnmapped() && read.mateIsReverseStrand() != read.isReverseStrand();

    public static final ReadFilter VALID_ALIGNMENT_START = read -> read.isUnmapped() || read.getStart() > 0;

    // Alignment doesn't align to negative number of bases in the reference.
    // Note: there seems to be no way a SAMRecord can fail this filter but we'll keep it because another read implementation may.
    public static final ReadFilter VALID_ALIGNMENT_END = read -> read.isUnmapped() || (read.getEnd() - read.getStart() + 1) >= 0;

    public static final ReadFilter HAS_READ_GROUP = read -> read.getReadGroup() != null;
    public static final ReadFilter HAS_MATCHING_BASES_AND_QUALS = read -> read.getBases().length == read.getBaseQualities().length;
    public static final ReadFilter SEQ_IS_STORED = read -> read.getBases().length > 0;
    public static final ReadFilter CIGAR_IS_SUPPORTED = read -> ! CigarUtils.containsNOperator(read.getCigar());
}
