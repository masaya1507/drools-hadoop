package jp.projects.miya.drools_hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.drools.KnowledgeBase;
import org.drools.KnowledgeBaseFactory;
import org.drools.builder.KnowledgeBuilder;
import org.drools.builder.KnowledgeBuilderFactory;
import org.drools.builder.ResourceType;
import org.drools.compiler.PackageBuilderConfiguration;
import org.drools.definition.KnowledgePackage;
import org.drools.io.Resource;
import org.drools.io.ResourceFactory;
import org.drools.rule.builder.dialect.java.JavaDialectConfiguration;
import org.drools.runtime.StatefulKnowledgeSession;
import org.drools.runtime.rule.FactHandle;

import au.com.bytecode.opencsv.CSVParser;

public class MapReduceByStateful extends Configured implements Tool {
	public static class Map extends
			Mapper<LongWritable, Text, Text, Text> {

		private StatefulKnowledgeSession ksession;
		private CSVParser parser;

		/*
		 * (非 Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			//
			OptimizerFactory.setDefaultOptimizer(OptimizerFactory.SAFE_REFLECTIVE);
					
			this.parser = new CSVParser(',', '"');

			KnowledgeBuilder kbuilder;

			Properties properties = new Properties();
			properties.setProperty("drools.dialect.java.compiler", "JANINO");
			PackageBuilderConfiguration cfg = new PackageBuilderConfiguration(
					properties);
			JavaDialectConfiguration javaConf = (JavaDialectConfiguration) cfg
					.getDialectConfiguration("java");
			javaConf.setCompiler(JavaDialectConfiguration.JANINO);

			kbuilder = KnowledgeBuilderFactory.newKnowledgeBuilder(cfg);

			Configuration conf = context.getConfiguration();
			Path[] pathList = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for (Path path : pathList) {
				if (!path.toString().endsWith("drl")) {
					continue;
				}

				InputStream is = Utils.getDistributedCacheInputStream(conf, path);
				Resource res = ResourceFactory.newInputStreamResource(is);
				kbuilder.add(res, ResourceType.DRL);
				is.close();
			}
			if (kbuilder.hasErrors()) {
				throw new RuntimeException("Unable to compile rule file:" + kbuilder.getErrors().toString());
			}

			final Collection<KnowledgePackage> pkgs = kbuilder.getKnowledgePackages();

			final KnowledgeBase kbase = KnowledgeBaseFactory.newKnowledgeBase();
			kbase.addKnowledgePackages(pkgs);

			this.ksession = kbase.newStatefulKnowledgeSession();
		}

		/*
		 * (非 Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			this.ksession.dispose();
		}

		/*
		 * (非 Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				// line
				String line = value.toString();
				String[] fields = this.parser.parseLine(line);

				if (fields.length == 5) {
					FactData data = new FactData();
					data.setId(fields[0]);
					data.setName(fields[1]);
					data.setRate(fields[2]);
					data.setValue1(fields[3]);
					data.setValue2(fields[4]);

					FactHandle hd = this.ksession.insert(data);
					this.ksession.fireAllRules();
					this.ksession.retract(hd);

					context.write(new Text(data.getId()), new Text(data.getResult()));
				} else {
					throw new Exception("Illeagal record format exists.");
				}
			} catch (Exception e) {
			}
		}
	}

	/*
	 * (非 Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf(
					"Usage: %s [generic options] <input dir> <output dir>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		} else {
			Job job = new Job(this.getConf());
			job.setJarByClass(MapReduceByStateful.class);

			job.setMapperClass(Map.class);
			job.setNumReduceTasks(0);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			boolean success = job.waitForCompletion(true);
			return success ? 0 : 1;
		}
	}

	/**
	 *
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new MapReduceByStateful(), args);
		System.exit(ret);
	}


}
