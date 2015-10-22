package heap;

import global.PageId;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.TreeMap;

public class TwoWayMap {
	private TreeMap<Integer,HashSet<Integer>> _dir;
	
	public TwoWayMap() {
		_dir = new TreeMap<Integer,HashSet<Integer>>();
	}
	
	public int getMax(PageId pid) {
		Entry<Integer,HashSet<Integer>> entry = _dir.lastEntry();
		pid.pid = entry.getValue().iterator().next();
		return entry.getKey();
	}
	
	public boolean insert(int fSpace, int pageId) {
		return false;
	}
	
	public boolean remove(int fSpace, int pageId) {
		return false;
	}
}
