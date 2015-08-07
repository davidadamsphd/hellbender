package org.broadinstitute.hellbender.engine.spark;

import com.google.api.services.genomics.model.Read;
import com.google.cloud.genomics.utils.ReadUtils;
import com.google.common.collect.Lists;
import htsjdk.samtools.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.broadinstitute.hellbender.engine.ReadsDataSource;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.IntervalUtils;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.GoogleGenomicsReadToGATKReadAdapter;
import org.seqdoop.hadoop_bam.AnySAMInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import scala.Tuple2;

import java.io.File;
import java.util.List;

/**
 * Created by davidada on 8/6/15.
 */
public class LoadReads {
    public static void main(String[] args) throws Exception {

        /*
        if (args.length < 1) {
            System.err.println("Usage: BamLoading <file>");
            System.exit(1);
        }*/

        SparkConf sparkConf = new SparkConf().setAppName("LoadReads")
                .setMaster("local[2]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "org.broadinstitute.hellbender.engine.spark.GATKRegistrator");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        String bam = "src/test/resources/org/broadinstitute/hellbender/tools/BQSR/HiSeq.1mb.1RG.2k_lines.alternate.bam"; //args[0];


        JavaRDD<GATKRead> rddReads = getSerialReads(ctx, bam);
        long count = rddReads.count();

        JavaRDD<GATKRead> filter = getParallelReads(ctx, bam);
        long count1 = filter.count();

        filter.collect();
        System.out.println("****************************");
        System.out.println("****************************");
        System.out.println("counts: " + count + ", " + count1);
        System.out.println("****************************");
        System.out.println("****************************");


        ctx.stop();
    }

    public static JavaRDD<GATKRead> getSerialReads(JavaSparkContext ctx, String bam) {
        final SAMFileHeader readsHeader = getHeader(bam);
        List<SimpleInterval> intervals = IntervalUtils.getAllIntervalsForReference(readsHeader.getSequenceDictionary());
        final SamReaderFactory samReaderFactory = SamReaderFactory.makeDefault().validationStringency(ValidationStringency.SILENT);

        ReadsDataSource bam2 = new ReadsDataSource(new File(bam), samReaderFactory);
        bam2.setIntervalsForTraversal(intervals);
        List<GATKRead> records = Lists.newArrayList();
        for ( GATKRead read : bam2 ) {
            records.add(read);
        }


        return ctx.parallelize(records);
    }

    public static JavaRDD<GATKRead> getParallelReads(JavaSparkContext ctx, String bam) {
        JavaPairRDD<LongWritable, SAMRecordWritable> rdd2 = ctx.newAPIHadoopFile(
                bam, AnySAMInputFormat.class, LongWritable.class, SAMRecordWritable.class,
                new Configuration());

        final SAMFileHeader readsHeader = getHeader(bam);
        List<SimpleInterval> intervals = IntervalUtils.getAllIntervalsForReference(readsHeader.getSequenceDictionary());

        return rdd2.map(new Function<Tuple2<LongWritable, SAMRecordWritable>, GATKRead>() {
            @Override
            public GATKRead call(Tuple2<LongWritable, SAMRecordWritable> v1) throws Exception {
                SAMRecord sam = v1._2().get();
                if (samRecordOverlaps(sam, intervals)) {
                    try {
                        Read read = ReadUtils.makeRead(sam);
                        if (read == null) {
                            throw new GATKException("null read");
                        }
                        return new GoogleGenomicsReadToGATKReadAdapter(read);
                    } catch (SAMException e) {
                        // do nothing if silent
                    }
                }
                return null;

            }
        }).filter(v1 -> v1 != null);

    }

    private static SAMFileHeader getHeader(String bam) {
        return SamReaderFactory.makeDefault().getFileHeader(new File(bam));
    }

    /**
     * Tests if a given SAMRecord overlaps any interval in a collection.
     */
    //TODO: remove this method when https://github.com/broadinstitute/hellbender/issues/559 is fixed
    private static boolean samRecordOverlaps( SAMRecord record, List<SimpleInterval> intervals ) {
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
}
