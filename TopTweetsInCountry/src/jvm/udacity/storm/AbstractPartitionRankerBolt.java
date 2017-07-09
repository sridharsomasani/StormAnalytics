package udacity.storm;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import udacity.storm.tools.TupleHelpers;


/**
 * Base class for {@link PartitionRankingBolt}
 * Helps in partitioning and ranking streams based on Country-ID
 *
 */
public abstract class AbstractPartitionRankerBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 4931640198601530202L;
	private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 2;
	private static final int DEFAULT_COUNT = 10;

	private final int emitFrequencyInSeconds;
	protected final int count;
	private Map<String, HashtagWrapper> rankings;

	public AbstractPartitionRankerBolt() {
		this(DEFAULT_COUNT, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
	}

	public AbstractPartitionRankerBolt(int topN) {
		this(topN, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
	}

	public AbstractPartitionRankerBolt(int topN, int emitFrequencyInSeconds) {
		if (topN < 1) {
			throw new IllegalArgumentException("topN must be >= 1 (you requested " + topN + ")");
		}
		if (emitFrequencyInSeconds < 1) {
			throw new IllegalArgumentException(
					"The emit frequency must be >= 1 seconds (you requested " + emitFrequencyInSeconds + " seconds)");
		}
		count = topN;
		this.emitFrequencyInSeconds = emitFrequencyInSeconds;
		rankings = new Hashtable<String, HashtagWrapper>();
	}

	protected Map<String, HashtagWrapper> getRankings() {
		return rankings;
	}

	/**
	 * This method functions as a template method (design pattern).
	 */
	@Override
	public final void execute(Tuple tuple, BasicOutputCollector collector) {
		if (TupleHelpers.isTickTuple(tuple)) {
			getLogger().debug("Received tick tuple, triggering emit of current rankings");
			emitRankings(collector, tuple);
		} else {
			updateRankingsWithTuple(tuple);
		}
	}

	abstract void updateRankingsWithTuple(Tuple tuple);

	private void emitRankings(BasicOutputCollector collector, Tuple tuple) {
		if(rankings == null) return;
		for (String country_id : rankings.keySet()) {
			HashtagWrapper copy = rankings.get(country_id).copy();
			copy.rerank();
			copy.pruneExtra();
			collector.emit(new Values(country_id, copy));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("country-id", "rankings"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
		return conf;
	}

	abstract Logger getLogger();

}
