package com.mapreduce.persons;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
/**
 * Example using map reduce Join with 2 csv files: persons and address
 * Mapreduce process is executed by hadoop client.
 * @author VENTURA
 *
 */
public class PersonsDetails {
	
	private static final Logger LOG = Logger.getLogger(PersonsDetails.class);
	private static final String DATA_SEPARATOR = ",";
	private static final String DATA_SEPARATOR2 = "-";

	public static class PersonMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String record = value.toString();
			String[] parts = record.split(DATA_SEPARATOR);

			StringBuilder dataStringBuilder = new StringBuilder();
			LOG.info("PersonMapper: " + parts[0] + " , " + parts[1]);

			dataStringBuilder.append("person   ").append(parts[1]).append(DATA_SEPARATOR2).append(parts[2])
					.append(DATA_SEPARATOR2).append(parts[3]).append(DATA_SEPARATOR2).append(parts[4]);
			context.write(new Text(parts[0]), new Text(dataStringBuilder.toString()));
			// [4000001, person Cristina Perez 55 Pilot], [4000002, person Catalina Sánchez 74 teacher], etc.
			LOG.info("PersonMapper Context: " + context.getCurrentKey() + "-" + context.getCurrentValue());

		}
	}

	public static class AddressMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String record = value.toString();
			String[] parts = record.split(DATA_SEPARATOR);
			StringBuilder dataStringBuilder = new StringBuilder();

			LOG.info("AddresMapper: " + parts[2] + " , " + parts[3]);

			dataStringBuilder.append("addres   ").append(parts[3]).append(DATA_SEPARATOR2).append(parts[4])
					.append(DATA_SEPARATOR2).append(parts[5]).append(DATA_SEPARATOR2).append(parts[6])
					.append(DATA_SEPARATOR2).append(parts[7]).append(DATA_SEPARATOR2).append(parts[8]);
			context.write(new Text(parts[2]), new Text(dataStringBuilder.toString()));

			// [4000001, addres Vicente Guerrero 11 40000 Iguala Guerrero México],
			// [4000002, addres Bandera nacionl 22 40010 Ciudad de México,Ciudad de México México], etc.
			LOG.info("AddresMapper Context: " + context.getCurrentKey() + "-" + context.getCurrentValue());
		}

	}

	// {person ID1 – [(person Cristina Perez 55 Pilot), (addres Vicente Guerrero 11 40000 Iguala Guerrero México),…..]}
	public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String name = "";
			String address = "";
			boolean isAddres = false;
			boolean isPerson = false;
			LOG.info("ReduceJoinReducer " + " -reduce " + context.getCurrentKey() + "-" + context.getCurrentValue());

			StringBuilder outPut = new StringBuilder();
			StringBuilder addressSB = new StringBuilder();

			for (Text t : values) {
				String parts[] = t.toString().split("  ");

				if (parts[0].equals("addres")) {
					LOG.info("ReduceJoin: parts[0] " + key + ", " + parts[0] + " " + parts[1]);
					address = parts[1];
					addressSB.append(address).append(",");
					isAddres = true;
				} else if (parts[0].equals("person")) {
					LOG.info("ReduceJoin: parts[0] " + key + ", " + parts[0] + " " + parts[1]);
					name = parts[1];
					outPut.append(name);
					isPerson = true;
				}
				LOG.info("Flags isAddres" + isAddres + ":: isPerson " + isPerson);
				if (isAddres == true & isPerson == true) {
					LOG.info("List Address:  " + addressSB.toString());
					String partsAddres[] = addressSB.toString().split(",");
					for (String dir : partsAddres) {
						LOG.info("buildOutput:  " + dir);
						context.write(new Text(key), new Text(DATA_SEPARATOR2
								+ new StringBuilder(name).append(DATA_SEPARATOR2).append(dir).toString()));
					}
				}
			}
		}
	}

	/**
	 * args[] path input custDetails in HDFS
	 * args[] path input for  addresDetails in HDFS
	 * args[] path ouput results in HDFS
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "PersonsDetails");
		job.setJarByClass(PersonsDetails.class);
		job.setReducerClass(ReduceJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PersonMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AddressMapper.class);
		Path outputPath = new Path(args[2]);

		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
