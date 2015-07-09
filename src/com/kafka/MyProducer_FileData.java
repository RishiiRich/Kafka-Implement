package com.kafka;
import java.awt.List;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.log4j.PropertyConfigurator;


public class MyProducer_FileData {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		String i;
		Properties props=new Properties();
		props.put("metadata.broker.list", "localhost:9092");
		
		props.put("producer.type", "async");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		
		ProducerConfig config=new ProducerConfig(props);
		
		Producer<String, String> producer=new Producer<>(config);
		
		ArrayList<KeyedMessage<String,String>> msg=new ArrayList<KeyedMessage<String,String>>(); 
		
		BufferedReader bufferedreader=new BufferedReader(new FileReader("/rishi/file.log"));
		
		while((i=bufferedreader.readLine())!=null)
		{
			String line;
			line=bufferedreader.readLine();
			KeyedMessage<String, String> data=new KeyedMessage<String, String>("test4", line);
			msg.add(data);
		}
		
		producer.send(msg);
		System.out.println(msg);
		System.out.println("Sent");
		
		producer.close();
		
		

	}

}
