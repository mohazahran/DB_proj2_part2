package heap;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import global.GlobalConst;
import global.Minibase;
import global.PageId;
import global.RID;

public class HeapScan implements GlobalConst 
{
	private HeapFile _heapFile;
	Iterator<Map.Entry<Integer,HashSet<Integer>>> _mapIterator = null;
	Iterator<Integer> _pageIterator = null;
	PageId _currentPageId = null;
	HFPage _currentPage;
	RID _curRid = null;
	private boolean _unpinLast = true;

	protected HeapScan(HeapFile hf) {
		_heapFile = hf;
		_currentPage = new HFPage();
		_mapIterator = _heapFile.getMap()._dir.entrySet().iterator();
		if(!(_mapIterator.hasNext())) {
			return;
		}
		Map.Entry<Integer,HashSet<Integer>> mapEntry = _mapIterator.next();
		_pageIterator = mapEntry.getValue().iterator();
		_currentPageId =  new PageId(_pageIterator.next());
		Minibase.BufferManager.pinPage(_currentPageId, _currentPage, PIN_DISKIO);
	}
	
	protected void finalize() throws Throwable {
		close();
	}
	
	public void close() {
		if(_currentPageId != null && _unpinLast) {
			Minibase.BufferManager.unpinPage(_currentPageId, false);
		}
	}
	 
	public boolean hasNext() {
		if(_curRid == null) {
			if(_currentPage.getSlotCount() != 0) {
				return true;
			}
			else {
				return false;
			}
		}
		else if(_currentPage.hasNext(_curRid) || _pageIterator.hasNext() || _mapIterator.hasNext()) {
			return true;
		}
		return false;
	}
	
	public Tuple getNext(RID rid) throws Exception {
		if(!hasNext()) {
			Minibase.BufferManager.unpinPage(_currentPageId, false);
			_unpinLast = false;
			return null;
		}
		
		byte[] bytes = getNextR(rid);
		return new Tuple(bytes,0,bytes.length);
	}
	
	private byte[] getNextR(RID rid) {
		if(_curRid == null) {
			_curRid = _currentPage.firstRecord();
			rid.pageno.pid = _currentPageId.pid;
			rid.slotno = _curRid.slotno;
			byte[] temp = _currentPage.selectRecord(_curRid);
			
			return temp; 
		}
		
		if(_currentPage.hasNext(_curRid)) {
			_curRid = _currentPage.nextRecord(_curRid);
			rid.pageno.pid = _currentPageId.pid;
			rid.slotno = _curRid.slotno;
			return _currentPage.selectRecord(_curRid);
		}
		
		Minibase.BufferManager.unpinPage(_currentPageId, false);
		if(_pageIterator.hasNext()) {
			Integer inte = _pageIterator.next();
			_currentPageId = new PageId(inte);
			Minibase.BufferManager.pinPage(_currentPageId, _currentPage, PIN_DISKIO);
			_curRid = _currentPage.firstRecord();
			rid.pageno.pid = _currentPageId.pid;
			rid.slotno = _curRid.slotno;
			return _currentPage.selectRecord(_curRid);
		}
		
		Map.Entry<Integer,HashSet<Integer>> mapEntry = _mapIterator.next();
		_pageIterator = mapEntry.getValue().iterator();
		_currentPageId =  new PageId(_pageIterator.next());
		Minibase.BufferManager.pinPage(_currentPageId, _currentPage, PIN_DISKIO);
		_curRid = _currentPage.firstRecord();
		rid.pageno.pid = _currentPageId.pid;
		rid.slotno = _curRid.slotno;
		return _currentPage.selectRecord(_curRid);
	}
}
