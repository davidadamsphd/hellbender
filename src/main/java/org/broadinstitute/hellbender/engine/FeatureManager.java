package org.broadinstitute.hellbender.engine;

import htsjdk.tribble.Feature;
import htsjdk.tribble.FeatureCodec;
import htsjdk.variant.vcf.VCFHeader;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.ClassFinder;
import org.broadinstitute.hellbender.cmdline.CommandLineParser;
import org.broadinstitute.hellbender.cmdline.CommandLineProgram;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.SimpleInterval;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;


/**
 * Handles discovery of available codecs and Feature arguments, file format detection and codec selection,
 * and creation/management/querying of FeatureDataSources for each source of Features.
 *
 * At startup, walks the packages specified in CODEC_PACKAGES to discover what codecs are available
 * to decode Feature-containing files.
 *
 * Then, given a tool instance, it discovers what FeatureInput argument fields are declared in the
 * tool's class hierarchy (and associated ArgumentCollections), and for each argument actually specified
 * by the user on the command line, determines the type of the file and the codec required to decode it,
 * creates a FeatureDataSource for that file, and adds it to a query-able resource pool.
 *
 * Clients can then call {@link #getFeatures(FeatureInput, SimpleInterval)} to query the data source for
 * a particular FeatureInput over a specific interval.
 */
public final class FeatureManager implements AutoCloseable {
    private static final Logger logger = LogManager.getLogger(FeatureManager.class);

    /**
     * We will search these packages at startup to look for FeatureCodecs
     */
    private static final List<String> CODEC_PACKAGES = Arrays.asList("htsjdk.variant",
                                                                     "htsjdk.tribble",
                                                                     "org.broadinstitute.hellbender.utils.codecs");

    /**
     * All codecs descend from this class
     */
    private static final Class<FeatureCodec> CODEC_BASE_CLASS = FeatureCodec.class;

    /**
     * The codec classes we locate when searching CODEC_PACKAGES
     */
    private static final Set<Class<?>> DISCOVERED_CODECS;

    /**
     * Feature arguments in tools are of this type
     */
    private static final Class<FeatureInput> FEATURE_ARGUMENT_CLASS = FeatureInput.class;

    /**
     * At startup, walk through the packages in CODEC_PACKAGES, and save any (concrete) FeatureCodecs discovered
     * in DISCOVERED_CODECS
     */
    static {
        final ClassFinder finder = new ClassFinder();
        for ( final String codecPackage : CODEC_PACKAGES ) {
            finder.find(codecPackage, CODEC_BASE_CLASS);
        }
        // Exclude abstract classes and interfaces from the list of discovered codec classes
        DISCOVERED_CODECS = Collections.unmodifiableSet(finder.getConcreteClasses());
    }

    /**
     * The tool instance containing the FeatureInput argument values that will form the basis of our
     * pool of FeatureDataSources
     */
    private CommandLineProgram toolInstance;

    /**
     * Mapping from FeatureInput argument to query-able FeatureDataSource for that source of Features
     */
    private Map<FeatureInput<? extends Feature>, FeatureDataSource<? extends Feature>> featureSources;

    /**
     * Create a FeatureManager given a CommandLineProgram tool instance, discovering all FeatureInput
     * arguments in the tool and creating query-able FeatureDataSources for them. Uses the default
     * caching behavior of {@link FeatureDataSource}.
     *
     * @param toolInstance Instance of the tool to be run (potentially containing one or more FeatureInput arguments)
     *                     Must have undergone command-line argument parsing and argument value injection already.
     */
    public FeatureManager( final CommandLineProgram toolInstance ) {
        this(toolInstance, FeatureDataSource.DEFAULT_QUERY_LOOKAHEAD_BASES);
    }

    /**
     * Create a FeatureManager given a CommandLineProgram tool instance, discovering all FeatureInput
     * arguments in the tool and creating query-able FeatureDataSources for them. Allows control over
     * how much caching is performed by each {@link FeatureDataSource}.
     *
     * @param toolInstance Instance of the tool to be run (potentially containing one or more FeatureInput arguments)
     *                     Must have undergone command-line argument parsing and argument value injection already.
     * @param featureQueryLookahead When querying FeatureDataSources, cache this many extra bases of context beyond
     *                              the end of query intervals in anticipation of future queries (>= 0).
     */
    public FeatureManager( final CommandLineProgram toolInstance, final int featureQueryLookahead ) {
        this.toolInstance = toolInstance;
        featureSources = new HashMap<>();

        initializeFeatureSources(featureQueryLookahead);
    }

    /**
     * Given our tool instance, discover all argument of type FeatureInput (or Collections thereof), determine
     * the type of each Feature-containing file, and add a FeatureDataSource for each file to our query pool.
     *
     * @param featureQueryLookahead Set up each FeatureDataSource to cache this many extra bases of context beyond
     *                              the end of query intervals in anticipation of future queries (>= 0).
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void initializeFeatureSources( final int featureQueryLookahead ) {

        // Discover all arguments of type FeatureInput (or Collections thereof) in our tool's class hierarchy
        // (and associated ArgumentCollections). Arguments not specified by the user on the command line will
        // come back to us with a null FeatureInput.
        final List<Pair<Field, FeatureInput>> featureArgumentValues =
                CommandLineParser.gatherArgumentValuesOfType(FEATURE_ARGUMENT_CLASS, toolInstance);

        for ( final Pair<Field, FeatureInput> featureArgument : featureArgumentValues ) {
            final FeatureInput<? extends Feature> featureInput = featureArgument.getValue();

            final String fullName = featureArgument.getKey().getAnnotation(Argument.class).fullName();
            final String shortName = featureArgument.getKey().getAnnotation(Argument.class).shortName();

            // Only create a data source for Feature arguments that were actually specified
            if ( featureInput != null ) {
                final Class<? extends Feature> featureType = getFeatureTypeForFeatureInputField(featureArgument.getKey());
                addToFeatureSources(featureQueryLookahead, featureInput, fullName, shortName, featureType);
            }
        }
    }


    /**
     * Add the feature data source to the given feature input.
     *
     * @param featureQueryLookahead look ahead this many bases during queries that produce cache misses
     * @param featureInput source of features
     * @param fullName full name of the argument (used in error message)
     * @param shortName short name of the argument (used in error message)
     * @param featureType class of features
     * Note: package-visible to enable access from the core walker classes
     * (but not actual tools, so it's not protected).
     */
    void addToFeatureSources(final int featureQueryLookahead, final FeatureInput<? extends Feature> featureInput, final String fullName, final String shortName, final Class<? extends Feature> featureType) {
        // Record the expected Feature type as declared in the parameterized type of the Field declaration
        // (eg., VariantContext if the field was a FeatureInput<VariantContext> or List<FeatureInput<VariantContext>>).
        // This is used for type-checking purposes.
        featureInput.setFeatureType(featureType);

        // Select the right codec for decoding the underlying file
        final FeatureCodec<? extends Feature, ?> codec = getCodecForFile(featureInput.getFeatureFile());

        // Make sure that the declared Feature type for the argument matches the actual type of Feature
        // that we will be getting from the selected codec
        if ( ! featureType.isAssignableFrom(codec.getFeatureType()) ) {
            throw new UserException(String.format("Argument --%s/-%s requires file(s) containing Features of type %s, " +
                                                  "but file %s contains Features of type %s",
                                                  fullName,
                                                  shortName,
                                                  featureType,
                                                  featureInput.getFeatureFile().getAbsolutePath(),
                                                  codec.getFeatureType()));
        }

        // Create a new FeatureDataSource for this file, and add it to our query pool
        featureSources.put(featureInput, new FeatureDataSource<>(featureInput.getFeatureFile(), codec, featureInput.getName(), featureQueryLookahead));
    }

    /**
     * Given a Field known to be of type FeatureInput (or a Collection thereof), retrieves the type
     * parameter for the FeatureInput (eg., for FeatureInput<VariantContext> or List<FeatureInput<VariantContext>>
     * this would be VariantContext).
     *
     * @param field a Field known to be of type FeatureInput whose type parameter to retrieve
     * @return type parameter of the FeatureInput declaration represented by the given field
     */
    @SuppressWarnings("unchecked")
    static Class<? extends Feature> getFeatureTypeForFeatureInputField( final Field field ) {
        final Type featureInputType = CommandLineParser.isCollectionField(field) ?
                                getNextTypeParameter((ParameterizedType)(field.getGenericType())) :
                                field.getGenericType();

        if ( ! (featureInputType instanceof ParameterizedType) ) {
            throw new GATKException(String.format("FeatureInput declaration for argument --%s lacks an explicit type parameter for the Feature type",
                                                  field.getAnnotation(Argument.class).fullName()));
        }

        return (Class<? extends Feature>)getNextTypeParameter((ParameterizedType)featureInputType);
    }

    /**
     * Helper method for {@link #getFeatureTypeForFeatureInputField(Field)} that "unpacks" a
     * parameterized type by one level of parameterization. Eg., given List<FeatureInput<VariantContext>>
     * would return FeatureInput<VariantContext>.
     *
     * @param parameterizedType parameterized type to unpack
     * @return the type parameter of the given parameterized type
     */
    private static Type getNextTypeParameter( final ParameterizedType parameterizedType ) {
        final Type[] typeParameters = parameterizedType.getActualTypeArguments();
        if ( typeParameters.length != 1 ) {
            throw new GATKException("Found a FeatureInput declaration with multiple type parameters, which is not supported");
        }
        return typeParameters[0];
    }

    /**
     * Does this manager have no sources of Features to query?
     *
     * @return true if there are no Feature sources available to query, otherwise false
     */
    public boolean isEmpty() {
        return featureSources.isEmpty();
    }


    /**
     * This method finds and returns all of the variant headers from the feature sources.
     *
     * @return A list of all variant headers for features.
     */
    public List<VCFHeader> getAllVariantHeaders() {
        List<VCFHeader> headers = new ArrayList<>();
        for (Map.Entry<FeatureInput<? extends Feature>, FeatureDataSource<? extends Feature>> entry : featureSources.entrySet()) {
            final Object header = entry.getValue().getHeader();
            if ( header != null && header instanceof VCFHeader ) {
                headers.add((VCFHeader)header);
            }
        }
        return headers;
    }

    /**
     * Given a FeatureInput argument field from our tool, queries the data source for that FeatureInput
     * over the specified interval, and returns a List of the Features overlapping that interval from
     * that data source.
     *
     * Will throw an exception if the provided FeatureInput did not come from the tool that this
     * FeatureManager was initialized with, or was not an @Argument-annotated field in the tool
     * (or parent classes).
     *
     * @param featureDescriptor FeatureInput argument from our tool representing the Feature source to query
     * @param interval interval to query over (returned Features will overlap this interval)
     * @param <T> type of Feature in the source represented by featureDescriptor
     * @return A List of all Features in the backing data source for the provided FeatureInput that overlap
     *         the provided interval (may be empty if there are none, but never null)
     */
    public <T extends Feature> List<T> getFeatures( final FeatureInput<T> featureDescriptor, final SimpleInterval interval ) {
        final FeatureDataSource<T> dataSource = lookupDataSource(featureDescriptor);

        // No danger of a ClassCastException here, since we verified that the FeatureDataSource for this
        // FeatureInput will return Features of the expected type T when we first created the data source
        // in initializeFeatureSources()
        return dataSource.queryAndPrefetch(interval);
    }

    /**
     * Given a FeatureInput argument field from our tool, returns an iterator to its features starting
     * from the first one.
     * <p><b>Warning!</b>: calling this method a second time on the same {@link FeatureInput}
     * on the same FeatureManager instance will invalidate (close) the iterator returned from
     * the first call.
     * </p>
     * <p>
     * An exception will be thrown if the {@link FeatureInput} provided did not come from the tool that this
     * manager was initialized with, or was not an &#64;Argument-annotated field in the tool
     * (or parent classes).
     * </p>
     *
     * @param featureDescriptor FeatureInput argument from our tool representing the Feature source to query
     * @param <T> type of Feature in the source represented by featureDescriptor
     * @return never {@code null}, a iterator to all the features in the backing data source.
     * @throws GATKException if the feature-descriptor is not found in the manager or is {@code null}.
     */
    public <T extends Feature> Iterator<T> getFeatureIterator(final FeatureInput<T> featureDescriptor) {
        final FeatureDataSource<T> dataSource = lookupDataSource(featureDescriptor);
        return dataSource.iterator();
    }

    /**
     * Get the header associated with a particular FeatureInput
     *
     * @param featureDescriptor the FeatureInput whose header we want to retrieve
     * @param <T> type of Feature in our FeatureInput
     * @return header for the provided FeatureInput
     */
    public <T extends Feature> Object getHeader( final FeatureInput<T> featureDescriptor ) {
        final FeatureDataSource<T> dataSource = lookupDataSource(featureDescriptor);
        return dataSource.getHeader();
    }

    /**
     * Retrieve the data source for a particular FeatureInput. Throws an exception if the provided
     * FeatureInput is not among our discovered sources of Features.
     *
     * @param featureDescriptor FeatureInput whose data source to retrieve
     * @param <T> type of Feature in our FeatureInput
     * @return query-able data source for the provided FeatureInput, if it was found
     */
    private <T extends Feature> FeatureDataSource<T> lookupDataSource( final FeatureInput<T> featureDescriptor ) {
        @SuppressWarnings("unchecked") final FeatureDataSource<T> dataSource = (FeatureDataSource<T>)featureSources.get(featureDescriptor);

        // Make sure the provided FeatureInput actually came from our tool as an @Argument-annotated field
        if ( dataSource == null ) {
            throw new GATKException(String.format("FeatureInput %s not found in feature manager's database for tool %s. " +
                                                  "In order to be detected, FeatureInputs must be declared in the tool class " +
                                                  "itself, a superclass of the tool class, or an @ArgumentCollection declared " +
                                                  "in the tool class or a superclass. They must also be annotated as an @Argument.",
                                                  featureDescriptor.getName(), toolInstance.getClass().getSimpleName()));
        }

        return dataSource;
    }

    /**
     * Utility method that determines the correct codec to use to read Features from the provided file.
     *
     * Codecs MUST correctly implement the {@link htsjdk.tribble.FeatureCodec#canDecode(String)} method
     * in order to be considered as candidates for decoding the file.
     *
     * Throws an exception if no suitable codecs are found (this is a user error, since the file is of
     * an unsupported format), or if more than one codec claims to be able to decode the file (this is
     * a configuration error on the codec authors' part).
     *
     * @param featureFile file for which to find the right codec
     * @return the codec suitable for decoding the provided file
     */
    public static FeatureCodec<? extends Feature, ?> getCodecForFile( final File featureFile ) {
        // Make sure file exists/is readable
        if ( ! featureFile.canRead() ) {
            throw new UserException.CouldNotReadInputFile(featureFile);
        }

        // Gather all discovered codecs that claim to be able to decode the given file according to their
        // canDecode() methods
        final List<FeatureCodec<? extends Feature, ?>> candidateCodecs = getCandidateCodecsForFile(featureFile);

        // If no codecs can handle the file, it's a user error (the user provided a file in an unsupported format)
        if ( candidateCodecs.isEmpty() ) {
            throw new UserException.CouldNotReadInputFile(featureFile, "no suitable codecs found");
        }
        // If multiple codecs can handle the file, it's a configuration error on the part of the codec authors
        else if ( candidateCodecs.size() > 1 ) {
            final StringBuilder multiCodecMatches = new StringBuilder();
            for ( FeatureCodec<? extends Feature, ?> candidateCodec : candidateCodecs ) {
                multiCodecMatches.append(candidateCodec.getClass().getCanonicalName());
                multiCodecMatches.append(' ');
            }
            throw new GATKException("Multiple codecs found able to decode file " + featureFile.getAbsolutePath() +
                                    ". This indicates a misconfiguration on the part of the codec authors. " +
                                    "Matching codecs are: " + multiCodecMatches.toString());
        }

        final FeatureCodec<? extends Feature, ?> selectedCodec = candidateCodecs.get(0);
        logger.info("Using codec " + selectedCodec.getClass().getSimpleName() + " to read file " + featureFile.getAbsolutePath());
        return selectedCodec;
    }

    /**
     * Returns a List of all codecs in DISCOVERED_CODECS that claim to be able to decode the specified file
     * according to their {@link htsjdk.tribble.FeatureCodec#canDecode(String)} methods.
     *
     * @param featureFile file for which to find potential codecs
     * @return A List of all codecs in DISCOVERED_CODECS for which {@link htsjdk.tribble.FeatureCodec#canDecode(String)} returns true on the specified file
     */
    private static List<FeatureCodec<? extends Feature, ?>> getCandidateCodecsForFile( final File featureFile )  {
        final List<FeatureCodec<? extends Feature, ?>> candidateCodecs = new ArrayList<>();

        for ( final Class<?> codecClass : DISCOVERED_CODECS ) {
            try {
                final FeatureCodec<? extends Feature, ?> codec = (FeatureCodec<? extends Feature, ?>)codecClass.newInstance();
                if ( codec.canDecode(featureFile.getAbsolutePath()) ) {
                    candidateCodecs.add(codec);
                }
            }
            catch ( InstantiationException | IllegalAccessException e ) {
                throw new GATKException("Unable to instantiate codec " + codecClass.getName());
            }
        }

        return candidateCodecs;
    }

    /**
     * @param file file to check
     * @return True if the file exists and contains Features (ie., we have a FeatureCodec that can decode it), otherwise false
     */
    public static boolean isFeatureFile( final File file ) {
        return file.exists() && ! getCandidateCodecsForFile(file).isEmpty();
    }

    /**
     * Permanently closes this manager by closing all backing data sources
     */
    public void close() {
        featureSources.values().forEach(ds -> ds.close());
    }
}
