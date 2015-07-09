package com.kafka;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import kafka.producer.*;



public class MyProducer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		/*Logger logger=Logger.getLogger(MyProducer.class);
		logger.info("log4j appender");*/
		
		
		Random rand=new Random();
		
		 //long events = Long.parseLong(args[0]);
		 
		Properties props=new Properties();
		
		props.put("metadata.broker.list", "localhost:9092");
		//props.put("partitioner.class", "SimplePartitioner");
		props.put("producer.type", "async");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		//props.put("zk.connect", "2181");
		
		ProducerConfig config=new ProducerConfig(props);
		
		Producer<String, String> producer=new Producer<String,String>(config);
		
		for(long i=0;i<10;i++)
		{
			long runtime=new Date().getTime();
			String ip="192.22.3."+rand.nextInt(255);
			String msg=runtime+",www.Kafka.com,"+ip;
			KeyedMessage<String, String> data=new KeyedMessage<String, String>("flumedata",ip,msg);
			producer.send(data);
			System.out.println(data);
				
		}
		
		producer.close();
		
		System.out.println("Ok Done");
		

	}

}
