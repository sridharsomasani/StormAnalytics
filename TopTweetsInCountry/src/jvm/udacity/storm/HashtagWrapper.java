package udacity.storm;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import udacity.storm.utils.Utils;


/**
 * @author Sridhar Somasani
 *
 * Wrapper class to maintain and manage country wise partitioned tag counts
 *
 */
public class HashtagWrapper {
	
	private static final int DEFAULT_COUNT = 10;
	
	private Map<String, Long> countMap;
	private String country_id;
	private int count;
	
	
	public HashtagWrapper(){
		this.countMap = new Hashtable<>();
		this.country_id = null;
		this.count = DEFAULT_COUNT;
	}
	
	public HashtagWrapper(String _country_id, Map<String, Long> _countMap){
		this(_country_id, _countMap, DEFAULT_COUNT);
	}
	
	public HashtagWrapper(String _country_id, Map<String, Long> _countMap, int count){
		this.country_id = _country_id;
		this.countMap = _countMap;
		this.count = count;
	}
	
	public void updateRankings(HashtagWrapper map){
		synchronized(countMap){
			Map<String, Long> ranks = map.getRankings();
			for(String tag: ranks.keySet()){
				countMap.put(tag, ranks.get(tag));
			}
			rerank();
		}
	}
	
	
	public void rerank(){
		countMap = Utils.sortByValue(countMap);
	}
	
	public void pruneExtra(){
		int index = 0;
		Iterator<Map.Entry<String,Long>> iter = countMap.entrySet().iterator();
		while(iter.hasNext()){
			index++;
			iter.next();
			if(index > count){
				iter.remove();
			}
		}
	}
	
	private void copyMap(HashtagWrapper src){
		Map<String, Long> ranks = src.getRankings();
		Iterator<Map.Entry<String,Long>> iter = ranks.entrySet().iterator();
		while(iter.hasNext()){
			Map.Entry<String, Long> current = iter.next();
			countMap.put(current.getKey(), current.getValue());
		}
//		for(String tag: ranks.keySet()){
//			countMap.put(tag, ranks.get(tag));
//		}
	}
	
	
	public HashtagWrapper copy(){
		if(countMap == null) return null;
		HashtagWrapper copy = new HashtagWrapper();
		copy.copyMap(this);
		copy.count = this.count;
		copy.country_id = this.country_id;
		return copy;
	}
	
	public Long getTagRank(String tag){
		if(countMap.containsKey(tag)){
			return countMap.get(tag);
		}
		return -1L;
	}
	
	public void setCount(int _count){
		this.count = _count;
	}
	
	public Map<String, Long> getRankings(){
		return countMap;
	}
	
	public String toString(){
		return country_id + "-->" + countMap.toString();
	}

}
