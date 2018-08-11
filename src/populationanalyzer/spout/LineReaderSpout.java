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
import populationanalyzer.TimeTracker;

public class LineReaderSpout implements IRichSpout {
	private SpoutOutputCollector collector;
	private FileReader fileReader;
	private boolean completed = false;
	private TopologyContext context;

	private Integer tupleId = 0;

	private Random random;

	private BufferedReader reader;



	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.random = new Random();
		try {
			this.random = new Random();
			this.context = context;
			this.fileReader = new FileReader(conf.get("inputFile").toString());
			this.reader = new BufferedReader(fileReader);
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file "
					+ conf.get("inputFile"));
		}
		this.collector = collector;
	}
        

	@Override
	public void nextTuple() {
		if (tupleId <= 420000) {
            String str;
            try {
                if ((str = reader.readLine()) != null) {
                    this.collector.emit(fetchTupleFields(str));
                }
            } catch (Exception e) {
                throw new RuntimeException("Error reading typle", e);
            }
        }

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
		return new Values(fields[0], Integer.parseInt(fields[1]), Integer.parseInt(fields[2]), fields[3],
				fields[4], fields[5], tupleId++);
	}

	/** Generates random fields with some fixed attributes and adds the tuple id field.
	 * @return fields array of randomly generated fields.
	 * */
	private Values getRandomTupleFields () {
		return new Values("Ahmed", random.nextInt(100), random.nextInt(100000),
				"ahmed@gmail.com", (random.nextInt(2) == 0 ? "Male" : "Female"), "Egypt", tupleId++);
	}
}
