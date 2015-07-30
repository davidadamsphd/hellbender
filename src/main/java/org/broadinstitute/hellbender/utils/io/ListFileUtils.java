package org.broadinstitute.hellbender.utils.io;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.text.XReadLines;


/**
 * A collection of convenience methods for working with list files.
 */
public class ListFileUtils {
    /**
     * Lines starting with this String in .list files are considered comments.
     */
    public static final String LIST_FILE_COMMENT_START = "#";

    /**
     * Returns a new set of values, containing a final set of values expanded from values
     * <p/>
     * Each element E of values can either be a literal string or a file ending in .list.
     * For each E ending in .list we try to read a file named E from disk, and if possible
     * all lines from that file are expanded into unique values.
     *
     * @param values Original values
     * @return entries from values or the files listed in values
     */
    public static Set<String> unpackSet(Collection<String> values) {
        if (values == null)
            throw new NullPointerException("values cannot be null");
        Set<String> unpackedValues = new LinkedHashSet<String>();
        // Let's first go through the list and see if we were given any files.
        // We'll add every entry in the file to our set, and treat the entries as
        // if they had been specified on the command line.
        for (String value : values) {
            File file = new File(value);
            if (value.toLowerCase().endsWith(".list") && file.exists()) {
                try {
                    Reader rdr = new FileReader(file);
                    unpackedValues.addAll(new XReadLines(rdr, true, LIST_FILE_COMMENT_START).readLines());
                } catch (IOException e) {
                    throw new UserException.CouldNotReadInputFile(file, e);
                }
            } else {
                unpackedValues.add(value);
            }
        }
        return unpackedValues;
    }

    /**
     * Returns a new set of values including only values listed by filters
     * <p/>
     * Each element E of values can either be a literal string or a file.  For each E,
     * we try to read a file named E from disk, and if possible all lines from that file are expanded
     * into unique names.
     * <p/>
     * Filters may also be a file of filters.
     *
     * @param values     Values or files with values
     * @param filters    Filters or files with filters
     * @param exactMatch If true match filters exactly, otherwise use as both exact and regular expressions
     * @return entries from values or the files listed in values, filtered by filters
     */
    public static Set<String> includeMatching(Collection<String> values, Collection<String> filters, boolean exactMatch) {
        return includeMatching(values, IDENTITY_STRING_CONVERTER, filters, exactMatch);
    }

    /**
     * Converts a type T to a String representation.
     *
     * @param <T> Type to convert to a String.
     */
    public static interface StringConverter<T> {
        String convert(T value);
    }

    /**
     * Returns a new set of values including only values matching filters
     * <p/>
     * Filters may also be a file of filters.
     * <p/>
     * The converter should convert T to a unique String for each value in the set.
     *
     * @param values     Values or files with values
     * @param converter  Converts values to strings
     * @param filters    Filters or files with filters
     * @param exactMatch If true match filters exactly, otherwise use as both exact and regular expressions
     * @return entries from values including only values matching filters
     */
    public static <T> Set<T> includeMatching(Collection<T> values, StringConverter<T> converter, Collection<String> filters, boolean exactMatch) {
        if (values == null)
            throw new NullPointerException("values cannot be null");
        if (converter == null)
            throw new NullPointerException("converter cannot be null");
        if (filters == null)
            throw new NullPointerException("filters cannot be null");

        Set<String> unpackedFilters = unpackSet(filters);
        Set<T> filteredValues = new LinkedHashSet<T>();
        Collection<Pattern> patterns = null;
        if (!exactMatch)
            patterns = compilePatterns(unpackedFilters);
        for (T value : values) {
            String converted = converter.convert(value);
            if (unpackedFilters.contains(converted)) {
                filteredValues.add(value);
            } else if (!exactMatch) {
                for (Pattern pattern : patterns)
                    if (pattern.matcher(converted).find())
                        filteredValues.add(value);
            }
        }
        return filteredValues;
    }

    /**
     * Returns a new set of values excluding any values matching filters.
     * <p/>
     * Filters may also be a file of filters.
     * <p/>
     * The converter should convert T to a unique String for each value in the set.
     *
     * @param values     Values or files with values
     * @param converter  Converts values to strings
     * @param filters    Filters or files with filters
     * @param exactMatch If true match filters exactly, otherwise use as both exact and regular expressions
     * @return entries from values exluding any values matching filters
     */
    public static <T> Set<T> excludeMatching(Collection<T> values, StringConverter<T> converter, Collection<String> filters, boolean exactMatch) {
        if (values == null)
            throw new NullPointerException("values cannot be null");
        if (converter == null)
            throw new NullPointerException("converter cannot be null");
        if (filters == null)
            throw new NullPointerException("filters cannot be null");

        Set<String> unpackedFilters = unpackSet(filters);
        Set<T> filteredValues = new LinkedHashSet<T>();
        filteredValues.addAll(values);
        Collection<Pattern> patterns = null;
        if (!exactMatch)
            patterns = compilePatterns(unpackedFilters);
        for (T value : values) {
            String converted = converter.convert(value);
            if (unpackedFilters.contains(converted)) {
                filteredValues.remove(value);
            } else if (!exactMatch) {
                for (Pattern pattern : patterns)
                    if (pattern.matcher(converted).find())
                        filteredValues.remove(value);
            }
        }
        return filteredValues;
    }

    private static Collection<Pattern> compilePatterns(Collection<String> filters) {
        Collection<Pattern> patterns = new ArrayList<Pattern>();
        for (String filter: filters) {
            patterns.add(Pattern.compile(filter));
        }
        return patterns;
    }

    protected static final StringConverter<String> IDENTITY_STRING_CONVERTER = new StringConverter<String>() {
        @Override
        public String convert(String value) {
            return value;
        }
    };
}
