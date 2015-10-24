package heap;

import global.PageId;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.TreeMap;

public class TwoWayMap {
	public TreeMap<Integer,HashSet<Integer>> _dir;
	
	public TwoWayMap() {
		_dir = new TreeMap<Integer,HashSet<Integer>>();
	}
	
	public TwoWayMap(TreeMap<Integer,HashSet<Integer>> dir) {
		_dir = new TreeMap<Integer, HashSet<Integer>>();
		_dir = (TreeMap<Integer, HashSet<Integer>>) dir.clone();
	}
	
	public int getMax(PageId pid) {
		if(_dir.size() == 0) {
			return -1;
		}
		
		Entry<Integer,HashSet<Integer>> entry = _dir.lastEntry();
		pid.pid = entry.getValue().iterator().next();
		return entry.getKey();
	}
	
	public int getPage(int fSpace) {
		HashSet<Integer> pageSet = _dir.get(fSpace);
		return pageSet.iterator().next();
	}
	
	public boolean insert(int fSpace, int pageId) {
		if(_dir.containsKey(fSpace)) {
			HashSet<Integer> pageSet = _dir.get(fSpace);
			pageSet.add(pageId);
		}
		else {
			HashSet<Integer> pageSet = new HashSet<>();
			pageSet.add(pageId);
			_dir.put(fSpace, pageSet);
		}
		return true;
	}
	
	public boolean remove(int fSpace, int pageId) throws Exception {
		if(_dir.containsKey(fSpace)) {
			HashSet<Integer> pageSet = _dir.get(fSpace);
			if(!(pageSet.remove(pageId))) {
				throw new Exception("Value not in Map Exception");
			}
			if(pageSet.isEmpty()) {
				_dir.remove(fSpace);
			}
		}
		else {
			throw new Exception("Key not in Map Exception");
		}
		return true;
	}

	public void clear() {
		_dir.clear();
	}
}
