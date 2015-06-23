package org.broadinstitute.hellbender.utils.dataflow;

import com.cloudera.dataflow.hadoop.HadoopIO;
import com.google.api.services.genomics.model.Read;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.readers.bam.ReadConverter;
import com.google.cloud.genomics.dataflow.utils.GenomicsDatasetOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import htsjdk.samtools.SAMException;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.ValidationStringency;
import org.apache.hadoop.io.LongWritable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.cloud.genomics.dataflow.utils.DataflowWorkarounds;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import org.apache.commons.lang3.tuple.Pair;
import org.broadinstitute.hellbender.engine.ReadsDataSource;
import org.broadinstitute.hellbender.engine.dataflow.coders.GATKReadCoder;
import org.broadinstitute.hellbender.engine.dataflow.coders.UUIDCoder;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.GoogleGenomicsReadToGATKReadAdapter;
import org.broadinstitute.hellbender.utils.read.MutableGATKRead;
import org.broadinstitute.hellbender.utils.read.SAMRecordToGATKReadAdapter;
import org.seqdoop.hadoop_bam.AnySAMInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.channels.Channels;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.UUID;

/**
 * Utilities for working with google Dataflow
 *
 * Provides a a number of useful PTransforms and DoFns
 */
public final class DataflowUtils {

    private final static Logger logger = LogManager.getLogger(DataflowUtils.class);

    public enum SaveDestination {
        LOCAL_DISK,
        CLOUD
    };

    private DataflowUtils(){} //prevent instantiation

    public static void registerGATKCoders( final Pipeline p ) {
        DataflowWorkarounds.registerGenomicsCoders(p);
        p.getCoderRegistry().registerCoder(GATKRead.class, new GATKReadCoder<GATKRead>());
        p.getCoderRegistry().registerCoder(MutableGATKRead.class, new GATKReadCoder<MutableGATKRead>());
        p.getCoderRegistry().registerCoder(GoogleGenomicsReadToGATKReadAdapter.class, GoogleGenomicsReadToGATKReadAdapter.CODER);
        p.getCoderRegistry().registerCoder(SAMRecordToGATKReadAdapter.class, SerializableCoder.of(SAMRecordToGATKReadAdapter.class));
        p.getCoderRegistry().registerCoder(SimpleInterval.class, SerializableCoder.of(SimpleInterval.class));
        p.getCoderRegistry().registerCoder(UUID.class, UUIDCoder.CODER);
    }

    /**
     * a transform which will convert the input PCollection<I> to a PCollection<String> by calling toString() on each element
     * @return a Transform from I -> String
     */
    public static <I> PTransform<PCollection<? extends I>,PCollection<String>> convertToString(){
        return ParDo.of(
                new DoFn<I, String>() {
                    @Override
                    public void processElement( ProcessContext c ) {
                        c.output(c.element().toString());
                    }
                });
    }

    /**
     * ingest local bam files from the file system and loads them into a PCollection<MutableGATKRead>
     * @param pipeline a configured Pipeline
     * @param intervals intervals to select reads from
     * @param bams paths to bam files to read from
     * @return a PCollection<Read> with all the reads the overlap the given intervals in the bams
     */
    public static PCollection<Read> getReadsFromLocalBams(final Pipeline pipeline, final List<SimpleInterval> intervals, final List<File> bams) {
        return getReadsFromLocalBams(pipeline, intervals, ValidationStringency.SILENT, bams);
    }

    /**
     * ingest local bam files from the file system and loads them into a PCollection<Read>
     * @param pipeline a configured Pipeline
     * @param intervals intervals to select reads from
     * @param stringency stringency of the input validation checks
     * @param bams paths to bam files to read from
     * @return a PCollection<Read> with all the reads the overlap the given intervals in the bams
     */
    public static PCollection<MutableGATKRead> getReadsFromLocalBams(final Pipeline pipeline, final List<SimpleInterval> intervals, final ValidationStringency stringency, final List<File> bams) {
        return pipeline.apply(Create.of(bams))
                .apply(ParDo.of(new LoadReadsFromFileFn(intervals, stringency)));
    }

    /**
     * Ingest a BAM file from a Hadoop file system and loads into a
     * <code>PCollection<Read></code>.
     * @param pipeline a configured Pipeline
     * @param intervals intervals to select reads from
     * @param bam Hadoop file path to read from
     * @return a <code>PCollection<Read></code> with all the reads that overlap the
     * given intervals in the BAM file
     */
    public static PCollection<Read> getReadsFromHadoopBam(final Pipeline pipeline, final List<SimpleInterval> intervals, final ValidationStringency stringency, final String bam) {
        PCollection<KV<LongWritable, SAMRecordWritable>> input =
            (PCollection<KV<LongWritable, SAMRecordWritable>>) pipeline.apply(
                HadoopIO.Read.from(bam).withFormatClass(AnySAMInputFormat.class)
                    .withKeyClass(LongWritable.class).withValueClass(SAMRecordWritable.class));
        return input.apply(ParDo.of(new DoFn<KV<LongWritable, SAMRecordWritable>, Read>() {
            @Override
            public void processElement(ProcessContext c) throws Exception {
                SAMRecord sam = c.element().getValue().get();
                if (overlaps(sam, intervals)) {
                    try {
                        Read read = ReadConverter.makeRead(sam);
                        c.output(read);
                    } catch (SAMException e) {
                        if (stringency == ValidationStringency.STRICT) {
                            throw e;
                        } else if (stringency == ValidationStringency.LENIENT) {
                            logger.info("getReadsFromHadoopBam: " + e.getMessage());
                        }
                        // do nothing if silent
                    }
                }
            }
        }));
    }

    /**
     * Tests if a given record overlaps any interval in a collection.
     */
    //TODO: remove this method when https://github.com/broadinstitute/hellbender/issues/559 is fixed
    private static boolean overlaps(SAMRecord record, List<SimpleInterval> intervals) {
        if (intervals == null || intervals.isEmpty()) {
            return true;
        }
        for (SimpleInterval interval : intervals) {
            if (interval.overlaps(record)) {
                return true;
            }
        }
        return false;
    }

    /**
     * get a transform that throws a specified exception
     */
    public static <I,O> PTransform<PCollection<? extends I>,PCollection<O>> throwException(Exception e){
        return ParDo.of(new ThrowExceptionFn<I, O>(e));
    }

    /**
     * throw a specified exception on execution
     */
    public static class ThrowExceptionFn<I,O> extends DoFn<I,O> {
        private final Exception e;

        public ThrowExceptionFn(Exception e){
            this.e = e;
        }

        @Override
        public void processElement(ProcessContext c) throws Exception {
            throw e;
        }
    }

    /**
     * Read a bam file and output each of the reads in it
     */
    public static class LoadReadsFromFileFn extends DoFn<File, MutableGATKRead> {
        private final static Logger logger = LogManager.getLogger(LoadReadsFromFileFn.class);

        private final List<SimpleInterval> intervals;
        private final ValidationStringency stringency;

        public LoadReadsFromFileFn(List<SimpleInterval> intervals, final ValidationStringency stringency) {
            this.intervals = intervals;
            this.stringency = stringency;
        }

        @Override
        public void processElement(ProcessContext c) {
            ReadsDataSource bam = new ReadsDataSource(c.element());
            bam.setIntervalsForTraversal(intervals);
            for (MutableGATKRead read : bam) {
                try {
                    c.output(read);
                } catch (SAMException x) {
                    if (stringency==ValidationStringency.STRICT) {
                        throw x;
                    } else if (stringency==ValidationStringency.LENIENT) {
                        logger.info("LoadReadsFromFileFn: "+x.getMessage());
                    }
                    // do nothing if silent
                }
            }
        }
    }

    // TODO: refactor this please
    public static <K, T, U> Pair<KeyedPCollectionTuple<K>, Pair<TupleTag<T>, TupleTag<U>>>
        makeKeyedPCollectionTuple( final PCollection<KV<K, T>> first, final PCollection<KV<K, U>> second ) {

        final TupleTag<T> firstTag = new TupleTag<>();
        final TupleTag<U> secondTag = new TupleTag<>();
        final KeyedPCollectionTuple<K> tuple = KeyedPCollectionTuple.of(firstTag, first).and(secondTag, second);
        return Pair.of(tuple, Pair.of(firstTag, secondTag));
    }


    /**
     * Serializes the collection's single object to the specified file.
     *
     * Of course if you run on the cloud and specify a local path, the file will be saved
     * on a cloud worker, which may not be very useful.
     *
     * @param collection A collection with a single serializable object to save.
     * @param fname the name of the destination, starting with "gs://" to save to GCS.
     * @returns SaveDestination.CLOUD if saved to GCS, SaveDestination.LOCAL_DISK otherwise.
     */
    public static <T> SaveDestination serializeSingleObject(PCollection<T> collection, String fname) {
        if (BucketUtils.isCloudStorageUrl(fname)) {
            saveSingleResultToGCS(collection, fname);
            return SaveDestination.CLOUD;
        } else {
            saveSingleResultToLocalDisk(collection, fname);
            return SaveDestination.LOCAL_DISK;
        }
    }

    /**
     * Serializes the collection's single object to the specified file.
     *
     * @param collection A collection with a single serializable object to save.
     * @param fname the name of the destination.
     */
    public static <T> void saveSingleResultToLocalDisk(PCollection<T> collection, String fname) {
        collection.apply(ParDo
                .named("save to " + fname)
                .of(new DoFn<T, Void>() {
                    @Override
                    public void processElement(ProcessContext c) throws IOException {
                        T obj = c.element();
                        try (ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(fname))) {
                            os.writeObject(obj);
                        }
                    }
                }));
    }

    /**
     * Serializes the collection's single object to the specified file.
     *
     * @param collection A collection with a single serializable object to save.
     * @param gcsDestPath the name of the destination (must start with "gs://").
     */
    public static <T> void saveSingleResultToGCS(final PCollection<T> collection, String gcsDestPath) {
        collection.apply(ParDo.named("save to " + gcsDestPath)
                .of(new DoFn<T, Void>() {
                    @Override
                    public void processElement(ProcessContext c) throws IOException, GeneralSecurityException {
                        GcsPath dest = GcsPath.fromUri(gcsDestPath);
                        GcsUtil gcsUtil = new GcsUtil.GcsUtilFactory().create(c.getPipelineOptions());
                        try (ObjectOutputStream out = new ObjectOutputStream(Channels.newOutputStream(gcsUtil.create(dest, "application/octet-stream")))) {
                            out.writeObject(c.element());
                        }
                    }
                }));
    }

}
