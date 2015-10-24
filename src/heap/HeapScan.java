package heap;

import global.GlobalConst;
import global.Minibase;
import global.PageId;
import global.RID;

public class HeapScan implements GlobalConst 
{
	private HeapFile _heapFile;
	private Header _header;
	PageId _currentPageId = null;
	HFPage _currentPage = null;
	RID _curRid = null;
	private boolean _unpinLast = true;

	protected HeapScan(HeapFile hf) {
		_heapFile = hf;
		_header = new Header();
		Minibase.BufferManager.pinPage(_heapFile._headPageId,_header, PIN_DISKIO);
		_currentPageId = _header.getLDId();
		if(_currentPageId.pid != -1) {
			Minibase.BufferManager.unpinPage(_heapFile._headPageId, false);
			_currentPage = new HFPage();
			Minibase.BufferManager.pinPage(_currentPageId, _currentPage, PIN_DISKIO);
		}
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
			if(_currentPage == null) {
				return false;
			}
			else {
				return true;
			}
		}
		else if(_currentPage.hasNext(_curRid) || _currentPage.getPrevPage().pid != _heapFile._headPageId.pid) {
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
			return _currentPage.selectRecord(_curRid); 
		}
		
		if(_currentPage.hasNext(_curRid)) {
			_curRid = _currentPage.nextRecord(_curRid);
			rid.pageno.pid = _currentPageId.pid;
			rid.slotno = _curRid.slotno;
			return _currentPage.selectRecord(_curRid);
		}
		
		Minibase.BufferManager.unpinPage(_currentPageId, false);
		_currentPageId = _currentPage.getPrevPage();
		_currentPage = new HFPage();
		Minibase.BufferManager.pinPage(_currentPageId, _currentPage, PIN_DISKIO);
		_curRid = _currentPage.firstRecord();
		rid.pageno.pid = _currentPageId.pid;
		rid.slotno = _curRid.slotno;
		return _currentPage.selectRecord(_curRid);
	}
}
