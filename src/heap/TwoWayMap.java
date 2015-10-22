package heap;

import global.PageId;

import java.util.HashSet;
import java.util.TreeMap;

public class TwoWayMap {
	private TreeMap<Integer,HashSet<Integer>> _dir;
	
	public TwoWayMap() {
		_dir = new TreeMap<Integer,HashSet<Integer>>();
	}
	
	public boolean insert(int fSpace, int pageId) {
		return false;
	}
	
	public boolean remove(int fSpace, int pageId) {
		return false;
	}
}
