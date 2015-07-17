package org.broadinstitute.hellbender.tools.walkers.annotator;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypesContext;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeaderLine;
import org.apache.commons.lang3.tuple.Pair;
import org.broadinstitute.hellbender.engine.AlignmentContext;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.ActiveRegionBasedAnnotation;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.AnnotatorCompatible;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.InfoFieldAnnotation;
import org.broadinstitute.hellbender.utils.MannWhitneyU;
import org.broadinstitute.hellbender.utils.QualityUtils;
import org.broadinstitute.hellbender.utils.genotyper.MostLikelyAllele;
import org.broadinstitute.hellbender.utils.genotyper.PerReadAlleleLikelihoodMap;
import org.broadinstitute.hellbender.utils.pileup.PileupElement;
import org.broadinstitute.hellbender.utils.pileup.ReadPileup;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.util.*;


/**
 * Abstract root for all RankSum based annotations
 */
public abstract class RankSumTest extends InfoFieldAnnotation implements ActiveRegionBasedAnnotation {
    static final boolean DEBUG = false;
    private boolean useDithering = true;

    public RankSumTest(final boolean useDithering){
        this.useDithering = useDithering;
    }

    public RankSumTest(){
        this(true);
    }

    public Map<String, Object> annotate(final AnnotatorCompatible walker,
                                        final ReferenceContext ref,
                                        final Map<String, AlignmentContext> stratifiedContexts,
                                        final VariantContext vc,
                                        final Map<String, PerReadAlleleLikelihoodMap> stratifiedPerReadAlleleLikelihoodMap) {
        // either stratifiedContexts or stratifiedPerReadAlleleLikelihoodMap has to be non-null

        final GenotypesContext genotypes = vc.getGenotypes();
        if (genotypes == null || genotypes.size() == 0) {
            return null;
        }

        final ArrayList<Double> refQuals = new ArrayList<>();
        final ArrayList<Double> altQuals = new ArrayList<>();

        for ( final Genotype genotype : genotypes.iterateInSampleNameOrder() ) {

            boolean usePileup = true;

            if ( stratifiedPerReadAlleleLikelihoodMap != null ) {
                final PerReadAlleleLikelihoodMap likelihoodMap = stratifiedPerReadAlleleLikelihoodMap.get(genotype.getSampleName());
                if ( likelihoodMap != null && !likelihoodMap.isEmpty() ) {
                    fillQualsFromLikelihoodMap(vc.getAlleles(), vc.getStart(), likelihoodMap, refQuals, altQuals);
                    usePileup = false;
                }
            }

            // the old UG SNP-only path through the annotations
            if ( usePileup && stratifiedContexts != null ) {
                final AlignmentContext context = stratifiedContexts.get(genotype.getSampleName());
                if ( context != null ) {
                    final ReadPileup pileup = context.getBasePileup();
                    if ( pileup != null ) {
                        fillQualsFromPileup(vc.getAlleles(), pileup, refQuals, altQuals);
                    }
                }
            }
        }

        if ( refQuals.isEmpty() && altQuals.isEmpty() ) {
            return null;
        }

        final MannWhitneyU mannWhitneyU = new MannWhitneyU(useDithering);
        for (final Double qual : altQuals) {
            mannWhitneyU.add(qual, MannWhitneyU.USet.SET1);
        }
        for (final Double qual : refQuals) {
            mannWhitneyU.add(qual, MannWhitneyU.USet.SET2);
        }

        if (DEBUG) {
            System.out.format("%s, REF QUALS:", this.getClass().getName());
            for (final Double qual : refQuals) {
                System.out.format("%4.1f ", qual);
            }
            System.out.println();
            System.out.format("%s, ALT QUALS:", this.getClass().getName());
            for (final Double qual : altQuals) {
                System.out.format("%4.1f ", qual);
            }
            System.out.println();

        }
        // we are testing that set1 (the alt bases) have lower quality scores than set2 (the ref bases)
        final Pair<Double, Double> testResults = mannWhitneyU.runOneSidedTest(MannWhitneyU.USet.SET1);

        final Map<String, Object> map = new HashMap<>();
        if (!Double.isNaN(testResults.getLeft())) {
            map.put(getKeyNames().get(0), String.format("%.3f", testResults.getLeft()));
        }
        return map;
    }

    private void fillQualsFromPileup(final List<Allele> alleles,
                                     final ReadPileup pileup,
                                     final List<Double> refQuals,
                                     final List<Double> altQuals) {
        for ( final PileupElement p : pileup ) {
            if ( isUsableBase(p) ) {
                final Double value = getElementForPileupElement(p);
                if ( value == null ) {
                    continue;
                }

                if ( alleles.get(0).equals(Allele.create(p.getBase(), true)) ) {
                    refQuals.add(value);
                } else if ( alleles.contains(Allele.create(p.getBase())) ) {
                    altQuals.add(value);
                }
            }
        }
     }

    private void fillQualsFromLikelihoodMap(final List<Allele> alleles,
                                            final int refLoc,
                                            final PerReadAlleleLikelihoodMap likelihoodMap,
                                            final List<Double> refQuals,
                                            final List<Double> altQuals) {
        for ( final Map.Entry<GATKRead, Map<Allele,Double>> el : likelihoodMap.getLikelihoodReadMap().entrySet() ) {
            final MostLikelyAllele a = PerReadAlleleLikelihoodMap.getMostLikelyAllele(el.getValue());
            if ( ! a.isInformative() ) {
                continue; // read is non-informative
            }

            final GATKRead read = el.getKey();
            if ( isUsableRead(read, refLoc) ) {
                final Double value = getElementForRead(read, refLoc, a);
                if ( value == null ) {
                    continue;
                }

                if ( a.getMostLikelyAllele().isReference() ) {
                    refQuals.add(value);
                } else if ( alleles.contains(a.getMostLikelyAllele()) ) {
                    altQuals.add(value);
                }
            }
        }
    }

    /**
     * Get the element for the given read at the given reference position
     *
     * @param read     the read
     * @param refLoc   the reference position
     * @param mostLikelyAllele the most likely allele for this read
     * @return a Double representing the element to be used in the rank sum test, or null if it should not be used
     */
    protected Double getElementForRead(final GATKRead read, final int refLoc, final MostLikelyAllele mostLikelyAllele) {
        return getElementForRead(read, refLoc);
    }

    /**
     * Get the element for the given read at the given reference position
     *
     * @param read     the read
     * @param refLoc   the reference position
     * @return a Double representing the element to be used in the rank sum test, or null if it should not be used
     */
    protected abstract Double getElementForRead(final GATKRead read, final int refLoc);

    // TODO -- until the ReadPosRankSumTest stops treating these differently, we need to have separate methods for GATKSAMRecords and PileupElements.  Yuck.

    /**
     * Get the element for the given read at the given reference position
     *
     * By default this function returns null, indicating that the test doesn't support the old style of pileup calculations
     *
     * @param p        the pileup element
     * @return a Double representing the element to be used in the rank sum test, or null if it should not be used
     */
    protected Double getElementForPileupElement(final PileupElement p) {
        // does not work in pileup mode
        return null;
    }

    /**
     * Can the base in this pileup element be used in comparative tests between ref / alt bases?
     *
     * Note that this function by default does not allow deletion pileup elements
     *
     * @param p the pileup element to consider
     * @return true if this base is part of a meaningful read for comparison, false otherwise
     */
    protected boolean isUsableBase(final PileupElement p) {
        return !(p.isDeletion() ||
                 p.getMappingQual() == 0 ||
                 p.getMappingQual() == QualityUtils.MAPPING_QUALITY_UNAVAILABLE ||
                 ((int) p.getQual()) < QualityUtils.MIN_USABLE_Q_SCORE); // need the unBAQed quality score here
    }

    /**
     * Can the read be used in comparative tests between ref / alt bases?
     *
     * @param read   the read to consider
     * @param refLoc the reference location
     * @return true if this read is meaningful for comparison, false otherwise
     */
    protected boolean isUsableRead(final GATKRead read, final int refLoc) {
        return !( read.getMappingQuality() == 0 ||
                read.getMappingQuality() == QualityUtils.MAPPING_QUALITY_UNAVAILABLE );
    }
}