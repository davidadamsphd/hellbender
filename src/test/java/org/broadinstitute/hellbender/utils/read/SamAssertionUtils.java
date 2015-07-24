package org.broadinstitute.hellbender.utils.read;

import htsjdk.samtools.SAMValidationError;
import htsjdk.samtools.SamFileValidator;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.ValidationStringency;
import org.testng.Assert;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

/**
 * Collection of utilities for making common assertions about SAM files for unit testing purposes.
 */
public final class SamAssertionUtils {

    private static SamReader getReader(File sam, ValidationStringency validationStringency, File reference) {
        return SamReaderFactory.makeDefault().validationStringency(validationStringency).referenceSequence(reference).open(sam);
    }

    public static void assertSamsEqual(final File sam1, final File sam2, ValidationStringency validationStringency, final File reference) throws IOException {
        try (final SamReader reader1 = getReader(sam1, validationStringency, reference);
             final SamReader reader2 = getReader(sam2, validationStringency, reference)) {
            final SamComparison comparison = new SamComparison(reader1, reader2);
            final boolean equal = comparison.areEqual();
            Assert.assertTrue(equal, "SAM file output differs from expected output");
        }
    }

    public static void assertSamsEqual(final File sam1, final File sam2, ValidationStringency validationStringency) throws IOException {
        assertSamsEqual(sam1, sam2, validationStringency, null);
    }

    public static void assertSamsEqual(final File sam1, final File sam2, final File reference) throws IOException {
        assertSamsEqual(sam1, sam2, ValidationStringency.DEFAULT_STRINGENCY, reference);
    }

    public static void assertSamsEqual(final File sam1, final File sam2) throws IOException {
        assertSamsEqual(sam1, sam2, ValidationStringency.DEFAULT_STRINGENCY, null);
    }

    public static void assertSamsNonEqual(final File sam1, final File sam2, final File reference) throws IOException {
        try (final SamReader reader1 = getReader(sam1, ValidationStringency.DEFAULT_STRINGENCY, reference);
             final SamReader reader2 = getReader(sam2, ValidationStringency.DEFAULT_STRINGENCY, reference)) {
            final SamComparison comparison = new SamComparison(reader1, reader2);
            final boolean equal = comparison.areEqual();
            Assert.assertFalse(equal, "SAM files are expected to differ, but they do not");
        }
    }

    public static void assertSamsNonEqual(final File sam1, final File sam2) throws IOException {
        assertSamsNonEqual(sam1,sam2,null);
    }

    public static void assertSamValid(final File sam, final File reference) throws IOException {
        try (final SamReader samReader = getReader(sam, ValidationStringency.LENIENT, reference)) {
            final SamFileValidator validator = new SamFileValidator(new PrintWriter(System.out), 8000);
            validator.setIgnoreWarnings(true);
            validator.setVerbose(true, 1000);
            validator.setErrorsToIgnore(Arrays.asList(SAMValidationError.Type.MISSING_READ_GROUP));
            final boolean validated = validator.validateSamFileVerbose(samReader, null);
            Assert.assertTrue(validated, "SAM file validation failed");
        }
    }

    public static void assertSamValid(final File sam) throws IOException {
        assertSamValid(sam, null);
    }

}
