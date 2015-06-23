package org.broadinstitute.hellbender.tools;

import htsjdk.samtools.*;
import org.broadinstitute.hellbender.cmdline.*;
import org.broadinstitute.hellbender.cmdline.programgroups.ReadProgramGroup;
import org.broadinstitute.hellbender.engine.FeatureContext;
import org.broadinstitute.hellbender.engine.ReadWalker;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.utils.read.MutableGATKRead;
import org.broadinstitute.hellbender.utils.read.ReadUtils;
import org.broadinstitute.hellbender.utils.read.SAMFileGATKReadWriter;

import java.io.File;

@CommandLineProgramProperties(
	usage = "Prints reads from the input to the output.",
    usageShort = "Print reads",
    programGroup = ReadProgramGroup.class
)
public final class PrintReads extends ReadWalker {

    @Argument(fullName = "output", shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME, doc="Write output to this file")
    public File OUTPUT;

    private SAMFileGATKReadWriter outputWriter;

    @Override
    public void onTraversalStart() {
        final SAMFileHeader outputHeader = ReadUtils.cloneSAMFileHeader(getHeaderForReads());
        outputWriter = new SAMFileGATKReadWriter(new SAMFileWriterFactory().makeWriter(outputHeader, true, OUTPUT, referenceArguments.getReferenceFile()));
    }

    @Override
    public void apply( MutableGATKRead read, ReferenceContext referenceContext, FeatureContext featureContext ) {
        outputWriter.addRead(read);
    }

    @Override
    public Object onTraversalDone() {
        if ( outputWriter != null ) {
            outputWriter.close();
        }
        return null;
    }
}
