/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package populationanalyzer.spout;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
public class LineReaderSpout implements IRichSpout {
	private SpoutOutputCollector collector;
	private FileReader fileReader;
	private boolean completed = false;
	private TopologyContext context;

	private Integer tupleId = 0;
	private Random random;
        
        
        
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		try {
			this.random = new Random();
			this.context = context;
			this.fileReader = new FileReader(conf.get("inputFile").toString());
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file "
					+ conf.get("inputFile"));
		}
		this.collector = collector;
	}
        

	@Override
	public void nextTuple() {
		if (completed) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {

			}
		}

		for (int i = 0; i < 420000; i++) {
			this.collector.emit(getRandomTupleFields());
		}

//		String str;
//		BufferedReader reader = new BufferedReader(fileReader);
//		try {
//			while ((str = reader.readLine()) != null) {
//				this.collector.emit(fetchTupleFields(str));
//			}
//		} catch (Exception e) {
//			throw new RuntimeException("Error reading typle", e);
//		} finally {
//			completed = true;
//		}

	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("Name", "Age", "Income", "email",
                        "gender", "Country", "Id"));
                    
	}

	@Override
	public void close() {
		try {
			fileReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public boolean isDistributed() {
		return false;
	}
	@Override
	public void activate() {
	}
	@Override
	public void deactivate() {
	}
	@Override
	public void ack(Object msgId) {
	}
	@Override
	public void fail(Object msgId) {
	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	/** Splits the input line to fields and adds the tuple id field.
	 * @param line line read from the input source.
	 * @return fields array of fields fetched from the line.
	 * */
	private Values fetchTupleFields (String line) {
		String[] fields = line.split(",");
		return new Values(fields[0], fields[1], fields[2], fields[3],
				fields[4], fields[5], tupleId++);
	}

	private Values getRandomTupleFields () {
		return new Values("Ahmed", random.nextInt(100), random.nextInt(100000),
				"ahmed@gmail.com", (random.nextInt(2) == 0 ? "Male" : "Female"), "Egypt", tupleId++);
	}
}
