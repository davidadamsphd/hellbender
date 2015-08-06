/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.broadinstitute.hellbender.tools.spark.examples;

import com.beust.jcommander.internal.Lists;
import com.google.api.services.genomics.model.Read;
import com.google.cloud.genomics.utils.ReadUtils;
import htsjdk.samtools.*;
import htsjdk.tribble.Feature;
import htsjdk.tribble.FeatureCodec;
import htsjdk.variant.variantcontext.VariantContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.engine.FeatureDataSource;
import org.broadinstitute.hellbender.engine.FeatureManager;
import org.broadinstitute.hellbender.engine.ReadsDataSource;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.IntervalUtils;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.GoogleGenomicsReadToGATKReadAdapter;
import org.broadinstitute.hellbender.utils.variant.Variant;
import org.broadinstitute.hellbender.utils.variant.VariantContextVariantAdapter;
import org.seqdoop.hadoop_bam.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

// Try with:
// src/test/resources/org/broadinstitute/hellbender/tools/BQSR/dbsnp_132.b37.excluding_sites_after_129.chr17_69k_70k.vcf
public final class VcfLoading {

    public static void main(String[] args) throws Exception {

        /*
        if (args.length < 1) {
            System.err.println("Usage: BamLoading <file>");
            System.exit(1);
        }*/

        SparkConf sparkConf = new SparkConf().setAppName("BamLoading");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        String vcf = "src/test/resources/org/broadinstitute/hellbender/tools/BQSR/dbsnp_132.b37.excluding_sites_after_129.chr17_69k_70k.vcf"; //args[0];

        JavaRDD<Variant> rddVariants = getSerialVariants(ctx, vcf);
        long count = rddVariants.count();

        JavaRDD<Variant> filter = getParallelVariants(ctx, vcf);
        long count1 = filter.count();
        System.out.println("****************************");
        System.out.println("****************************");
        System.out.println("counts: " + count + ", " + count1);
        System.out.println("****************************");
        System.out.println("****************************");

        ctx.stop();
    }

    public static JavaRDD<Variant> getSerialVariants(JavaSparkContext ctx, String vcf) {
        List<Variant> records = Lists.newArrayList();
        try ( final FeatureDataSource<VariantContext> dataSource = new FeatureDataSource<>(new File(vcf), getCodecForVariantSource(vcf), null, 0) ) {
            records.addAll(wrapQueryResults(dataSource.iterator()));
        }

        return ctx.parallelize(records);
    }

    public static JavaRDD<Variant> getParallelVariants(JavaSparkContext ctx, String vcf) {
        JavaPairRDD<LongWritable, VariantContextWritable> rdd2 = ctx.newAPIHadoopFile(
                vcf, VCFInputFormat.class, LongWritable.class, VariantContextWritable.class,
                new Configuration());
        return rdd2.map(v1 -> {
            VariantContext sam = v1._2().get();
            return new VariantContextVariantAdapter(sam);
        });

    }

    @SuppressWarnings("unchecked")
    private static FeatureCodec<VariantContext, ?> getCodecForVariantSource( final String variantSource ) {
        final FeatureCodec<? extends Feature, ?> codec = FeatureManager.getCodecForFile(new File(variantSource));
        if ( !VariantContext.class.isAssignableFrom(codec.getFeatureType()) ) {
            throw new UserException(variantSource + " is not in a format that produces VariantContexts");
        }
        return (FeatureCodec<VariantContext, ?>)codec;
    }

    private static List<Variant> wrapQueryResults( final Iterator<VariantContext> queryResults ) {
        final List<Variant> wrappedResults = new ArrayList<>();
        while ( queryResults.hasNext() ) {
            wrappedResults.add(new VariantContextVariantAdapter(queryResults.next()));
        }
        return wrappedResults;
    }
}