package edu.ub.ahstfg.helloworld;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Integrator extends Configured implements Tool {

    public static class Interval implements Writable {

        double min;
        double max;

        public Interval(double min, double max) {
            this.min = min;
            this.max = max;
        }

        public Interval() {
            this(0.0, 1.0);
        }

        @Override
        public void readFields(DataInput arg0) throws IOException {
            min = arg0.readDouble();
            max = arg0.readDouble();
        }

        @Override
        public void write(DataOutput arg0) throws IOException {
            arg0.writeDouble(min);
            arg0.writeDouble(max);
        }

        @Override
        public String toString() {
            return "[" + min + "," + max + "]";
        }

    }

    public static class RectanglerMapper extends MapReduceBase implements
            Mapper<IntWritable, Interval, IntWritable, DoubleWritable> {

        private double function(double x) {
            return x;
        }

        @Override
        public void map(IntWritable key, Interval interval,
                OutputCollector<IntWritable, DoubleWritable> out,
                Reporter reporter) throws IOException {
            final double y0 = function(interval.min);
            final double y1 = function(interval.max);
            final double d = y1 - y0;
            final double h = interval.max - interval.min;
            final double aR = y0 * h;
            final double aT = (h * d) / 2.0;
            final double area = aR + aT;
            out.collect(key, new DoubleWritable(area));
        }
    }

    public static class AdderReducer extends MapReduceBase implements
            Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

        private JobConf conf;

        double sum;
        IntWritable key;

        @Override
        public void configure(JobConf job) {
            conf = job;
        }

        @Override
        public void reduce(IntWritable key, Iterator<DoubleWritable> divs,
                OutputCollector<IntWritable, DoubleWritable> out,
                Reporter reporter) throws IOException {
            this.key = key;
            for (; divs.hasNext(); sum += divs.next().get()) {}
        }

        @Override
        public void close() throws IOException {
            Path outDir = new Path(TMP_PATH, "output");
            Path outFile = new Path(outDir, "reduce-out");
            FileSystem fileSys = FileSystem.get(conf);
            SequenceFile.Writer writer = SequenceFile.createWriter(fileSys,
                    conf, outFile, IntWritable.class, DoubleWritable.class,
                    CompressionType.NONE);
            writer.append(key, new DoubleWritable(sum));
            writer.close();
        }

    }

    public static final String JOB_NAME = "integrator";
    public static final Path TMP_PATH = new Path(
            Integrator.class.getSimpleName() + "_TMP");

    public static final IntWritable KEY = new IntWritable(1);

    public static void integrate(JobConf jobConf, int divs, double min,
            double max) throws IOException {

        jobConf.setJobName(Integrator.class.getSimpleName());

        jobConf.setInputFormat(SequenceFileInputFormat.class);

        jobConf.setOutputKeyClass(IntWritable.class);
        jobConf.setOutputValueClass(DoubleWritable.class);
        jobConf.setOutputFormat(SequenceFileOutputFormat.class);

        jobConf.setMapperClass(RectanglerMapper.class);
        jobConf.setNumMapTasks(divs);

        jobConf.setReducerClass(AdderReducer.class);
        jobConf.setNumReduceTasks(1);

        jobConf.setSpeculativeExecution(false);

        final Path inDir = new Path(TMP_PATH, "input");
        final Path outDir = new Path(TMP_PATH, "output");
        FileInputFormat.addInputPath(jobConf, inDir);
        FileOutputFormat.setOutputPath(jobConf, outDir);

        final FileSystem fs = FileSystem.get(jobConf);
        if (fs.exists(TMP_PATH)) {
            throw new IOException("Tmp directory " + fs.makeQualified(TMP_PATH)
                    + " already exists.  Please remove it first.");
        }
        if (!fs.mkdirs(inDir)) {
            throw new IOException("Cannot create input directory " + inDir);
        }

        // Each map must receive one key-value pair.
        // Each key-value pair will be written on a file.
        // For this case, the key will be the same for every part and the value
        // will be the X axis values for the interval.
        try {
            final double sizeInterval = (max - min) / divs;
            Path file;
            Interval interval;
            for (int i = 0; i < divs; i++) {
                file = new Path(inDir, "part" + i);
                interval = new Interval(min + sizeInterval * i, min
                        + sizeInterval * (i + 1));
                final SequenceFile.Writer writer = SequenceFile.createWriter(
                        fs, jobConf, file, IntWritable.class, Interval.class,
                        CompressionType.NONE);
                try {
                    writer.append(KEY, interval);
                } finally {
                    writer.close();
                }
                System.out.println("Wrote input for Map #" + i);
            }

            //start a map/reduce job
            System.out.println("Starting Job");
            final long startTime = System.currentTimeMillis();
            JobClient.runJob(jobConf);
            final double duration = (System.currentTimeMillis() - startTime) / 1000.0;
            System.out.println("Job Finished in " + duration + " seconds");

            //read outputs
            Path inFile = new Path(outDir, "reduce-out");
            IntWritable key = new IntWritable();
            DoubleWritable result = new DoubleWritable();
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, inFile,
                    jobConf);
            try {
                reader.next(key, result);
            } finally {
                reader.close();
            }

            System.out.println("Integral result = " + String.valueOf(result));

        } finally {
            fs.delete(TMP_PATH, true);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: integrator <rects> <min> <max>");
            System.exit(2);
        }
        final JobConf jobConf = new JobConf(getConf(), getClass());
        integrate(jobConf, Integer.parseInt(args[0]),
                Double.parseDouble(args[1]), Double.parseDouble(args[2]));
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(null, new Integrator(), args));
    }

}
