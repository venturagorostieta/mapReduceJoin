package com.mapreduce.persons;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.MapDriverBase;
import org.apache.hadoop.mrunit.TestDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class PersonsDetailsTest {
	
	MapDriver<Object, Text, Text, Text> mapDriver;
	@Before
	public void setUp() {
		PersonsDetails.PersonMapper mapper = new PersonsDetails.PersonMapper();
	mapDriver = MapDriver.newMapDriver(mapper);
	}
	
	@Test
	public void testMapper() throws IOException {
	//mapDriver.withInput(new Text("4000001"), new Text("Cristina,Delgado,55,Pilot"));
	mapDriver.withInput("4000001", new Text("Cristina,Delgado,55,Pilot"));
//	mapDriver.withInput(new Text("4000002"), new Text("Elizabeth,Sánchez,74,teacher"));
//	mapDriver.withInput(new Text("4000003"), new Text("Guadalupe,De la Cruz,34,Firefighter"));
//	mapDriver.withOutput(new Text("4000004"), new Text("Jorge,Roman,66,Engineer"));
//	mapDriver.withOutput(new Text("4000005"), new Text("Francisco,Pardo,43,Doctor"));
	
	mapDriver.withOutput(new Text("4000001"), new Text("person   Cristina Delgado 55 Pilot"));
//	mapDriver.withOutput(new Text("a"), new Text("zwei"));
//	mapDriver.withOutput(new Text("c"), new Text("drei"));
	mapDriver.runTest();
	}
}
