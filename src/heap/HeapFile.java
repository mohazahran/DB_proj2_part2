package heap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

import global.GlobalConst;
import global.Minibase;
import global.PageId;
import global.RID;


public class HeapFile implements GlobalConst
{
	private String _name;
	private Header _headPage;
	public PageId _headPageId = null;
	public TwoWayMap _map = null;
	private int rCount = 0;
	
	/* From Java doc
	 * If the given name already denotes a file, this opens it; otherwise, this creates a new empty file. 
	 * A null name produces a temporary heap file which requires no DB entry.
	 */
	@SuppressWarnings("unchecked")
	public HeapFile (String name) {
		
		_name = name;
		if(_name != null) {
			_headPageId = Minibase.DiskManager.get_file_entry(_name);
		}
		
		//check is the file exists already
		if(_headPageId == null) {
			// file does not exist, then create it 
			
			rCount = 0;
			_headPage = new Header();
			_headPageId = Minibase.BufferManager.newPage(_headPage, 1);
			if(_name != null) {
				Minibase.DiskManager.add_file_entry(_name,_headPageId);
			}
			_map = new TwoWayMap();
			_headPage.setLDId(new PageId(-1));
			_headPage.setRecordCount(rCount);
			Minibase.BufferManager.unpinPage(_headPageId, true);
		}
		else {
			// the file exists
			_headPage = new Header();
			Minibase.BufferManager.pinPage(_headPageId, _headPage, PIN_DISKIO);
			rCount = _headPage.getRecordCount();
			ArrayList<Byte> serialDataBytes = new ArrayList<Byte>();
			
			//reading data from header
			byte[] tempData = _headPage.getMetaData();
			for(int i=0;i<tempData.length;i++){				
				serialDataBytes.add(tempData[i]);
			}
			
			//looping over extra header pages and read data
			PageId nextId = _headPage.getNextHeader();
			NextHeader nHeader = new NextHeader();
			while(nextId.pid != -1) {					
				Minibase.BufferManager.pinPage(nextId, nHeader, PIN_DISKIO);
				tempData = nHeader.getData();
				for(int i=0;i<tempData.length;i++) {
					serialDataBytes.add(tempData[i]);
				}
				nextId = nHeader.getNextHeader();
				Minibase.BufferManager.freePage(nextId);
			}
			
			byte[] temp = new byte[serialDataBytes.size()];
			for(int i=0;i<serialDataBytes.size();i++){				
				temp[i] = serialDataBytes.get(i);
			}
			
			try {
				_map = new TwoWayMap((TreeMap<Integer,HashSet<Integer>>) deserialize(temp));
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Minibase.BufferManager.unpinPage(_headPageId, false);
		}
}

	private byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeObject(obj);
        return b.toByteArray();
    }

    private Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream b = new ByteArrayInputStream(bytes);
        ObjectInputStream o = new ObjectInputStream(b);
        return o.readObject();
    }
	
	//Must be in O(log N)
	public RID insertRecord(byte[] record) throws SpaceNotAvailableException {
		if(record.length > MAX_TUPSIZE) {
			throw new SpaceNotAvailableException(null, "SpaceNotAvailableException");
		}
		
		PageId pageId = new PageId();
		int cFree = _map.getMax(pageId);
		
		HFPage dPage = new HFPage();
		RID rId = null;
		if(cFree != -1) {
			Minibase.BufferManager.pinPage(pageId, dPage, PIN_DISKIO);
			rId = dPage.insertRecord(record);
			if(rId != null) {
				rId.pageno.pid = pageId.pid;
				try {
					_map.remove(cFree, pageId.pid);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				_map.insert(dPage.getFreeSpace(), pageId.pid);
			}
			else {
				Minibase.BufferManager.unpinPage(pageId, true);
				dPage = new HFPage();
				pageId = Minibase.BufferManager.newPage(dPage, 1);
				rId = dPage.insertRecord(record);
				rId.pageno.pid = pageId.pid;
				_map.insert(dPage.getFreeSpace(), pageId.pid);
				
				/* Set pointers in data pages */
				
				dPage.setNextPage(_headPage.getDataPageId());
				dPage.setPrevPage(_headPageId);
				_headPage.setDataPageId(pageId);
				
				PageId nextDId = new PageId(dPage.getNextPage().pid);
				HFPage nextDPage = new HFPage();
				Minibase.BufferManager.pinPage(nextDId, nextDPage, PIN_DISKIO);
				nextDPage.setPrevPage(pageId);
				Minibase.BufferManager.unpinPage(nextDId, true);
			}
		}
		else {
			pageId = Minibase.BufferManager.newPage(dPage, 1);
			rId = dPage.insertRecord(record);
			rId.pageno.pid = pageId.pid;
			_map.insert(dPage.getFreeSpace(), pageId.pid);
			
			/* Set pointers in data pages */
			
			_headPage.setDataPageId(pageId);
			_headPage.setLDId(pageId);
			dPage.setPrevPage(_headPageId);
			dPage.setNextPage(new PageId(-1));
		}
		rCount++;
		Minibase.BufferManager.unpinPage(pageId, true);
		return rId;
	}
	
	//Must be in O(log N)
	public boolean deleteRecord(RID rid) {
		PageId pageId = new PageId();
		pageId.pid= rid.pageno.pid;
		HFPage dataPage = new HFPage();
		Minibase.BufferManager.pinPage(pageId, dataPage, PIN_DISKIO);
		try {
			_map.remove(dataPage.getFreeSpace(), pageId.pid);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		rid.pageno.pid = -1;
		dataPage.deleteRecord(rid);
		rCount--;		
		if(dataPage.getFreeSpace() == 1004) {
			
			/* Set pointers in data pages */
			
			PageId prevDId = new PageId(dataPage.getPrevPage().pid);
			HFPage prevDPage = new HFPage();
			Minibase.BufferManager.pinPage(prevDId, prevDPage, PIN_DISKIO);
			
			if(dataPage.getNextPage().pid != -1) {
				PageId nextDId = new PageId(dataPage.getNextPage().pid);
				HFPage nextDPage = new HFPage();
				Minibase.BufferManager.pinPage(nextDId, nextDPage, PIN_DISKIO);
				
				prevDPage.setNextPage(nextDId);
				nextDPage.setPrevPage(prevDId);
				
				Minibase.BufferManager.unpinPage(nextDId, true);
			}
			else {
				prevDPage.setNextPage(new PageId(-1));
				_headPage = new Header();
				Minibase.BufferManager.pinPage(_headPageId, _headPage, PIN_DISKIO);
				_headPage.setLDId(prevDId);
				Minibase.BufferManager.unpinPage(_headPageId, true);
			}
			
			Minibase.BufferManager.unpinPage(prevDId, true);
			Minibase.BufferManager.freePage(pageId);
		}
		else {
			_map.insert(dataPage.getFreeSpace(), pageId.pid);
			Minibase.BufferManager.unpinPage(pageId, true);
		}
		return true;
	}
	
	
	//Must be in O(log N)
	public boolean updateRecord(RID rid, Tuple newRecord) throws InvalidUpdateException {
		PageId pageId = new PageId(rid.pageno.pid);
		HFPage dataPage = new HFPage();
		Minibase.BufferManager.pinPage(pageId, dataPage, PIN_DISKIO);
		if(dataPage.getSlotLength(rid.slotno) != newRecord.getLength()) {
			throw new InvalidUpdateException(null, "InvalidUpdateException");
		}
		rid.pageno.pid = -1;
		dataPage.updateRecord(rid, newRecord);
		Minibase.BufferManager.unpinPage(pageId, true);
		return true;
	}
	
	//get number of records in the file
	
	public int getRecCnt() {
		return rCount;
	}
	
	
	protected void finalize() throws Throwable {
		if(_name == null) {
			deleteFile();
			return;
		}
	}

	public void deleteFile() {
		for(Map.Entry<Integer,HashSet<Integer>> entry : _map._dir.entrySet()) {
			HashSet<Integer> value = entry.getValue();
			for (Integer pageId : value) {
			    Minibase.BufferManager.freePage(new PageId(pageId));
			}
		}
		_map.clear();
		Minibase.BufferManager.unpinPage(_headPageId, false);
		Minibase.BufferManager.freePage(_headPageId);
	}

	public HeapScan openScan() {
		HeapScan hs = new HeapScan(this);
		return hs;
	}
	
	
	public byte[] selectRecord(RID rid) {
		PageId pageId = new PageId(rid.pageno.pid);
		HFPage dataPage = new HFPage();
		Minibase.BufferManager.pinPage(pageId, dataPage, PIN_DISKIO);
		rid.pageno.pid = -1;
		byte[] record = dataPage.selectRecord(rid);
		Minibase.BufferManager.unpinPage(pageId, false);
		return record;
	}
	
	public String toString() {
		return _name;
	}

	public Tuple getRecord(RID rid) {
		byte[] record = selectRecord(rid);
		Tuple tuple = new Tuple(record,0,record.length);
		return tuple;
	}
	
	public TwoWayMap getMap() {
		return _map;
	}

	public void close() {
		byte[] byteMap = null;
		try {
			byteMap = serialize(_map._dir);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Minibase.BufferManager.pinPage(_headPageId, _headPage, PIN_DISKIO);
		_headPage.setRecordCount(rCount);
		if(rCount == 0) {
			_headPage.setLDId(new PageId(-1));
		}
		int hCapacity = PAGE_SIZE-5*Integer.BYTES;
		if(byteMap.length <= hCapacity) {
			try {
				_headPage.setMetaData(byteMap);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			_headPage.setNextHeader(new PageId(-1));
			Minibase.BufferManager.unpinPage(_headPageId, true);
			return;
		}
		
		byte[] tempHData = Arrays.copyOfRange(byteMap,0,hCapacity);
		try {
			_headPage.setMetaData(tempHData);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		int mapSize = byteMap.length - hCapacity;
		int numNHPages = (int)Math.ceil((float)mapSize / (float)(PAGE_SIZE-2*Integer.BYTES));
		NextHeader _cNHead = new NextHeader();
		PageId _cNHeadId = null;
		
		_cNHeadId = Minibase.BufferManager.newPage(_cNHead, numNHPages);
		_headPage.setNextHeader(new PageId(_cNHeadId.pid));
		Minibase.BufferManager.unpinPage(_headPageId, true);
		Minibase.BufferManager.unpinPage(_cNHeadId, false);
		
		int start = hCapacity;
		int end = start + (PAGE_SIZE-2*Integer.BYTES);
		for(int i=0; i< numNHPages; i++) {
			Minibase.BufferManager.pinPage(_cNHeadId, _cNHead, PIN_DISKIO);
			if(i<numNHPages - 1) {
				_cNHead.setNextHeader(new PageId(_cNHeadId.pid + 1));
				byte[] tempNHData = Arrays.copyOfRange(byteMap,start,end);
				try {
					_cNHead.setMetaData(tempNHData);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				start = end;
				end += (PAGE_SIZE-2*Integer.BYTES);
			}
			else {
				_cNHead.setNextHeader(new PageId(-1));
				byte[] tempNHData = Arrays.copyOfRange(byteMap,start,byteMap.length);
				try {
					_cNHead.setMetaData(tempNHData);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			Minibase.BufferManager.unpinPage(_cNHeadId, true);
			_cNHeadId.pid = _cNHeadId.pid + 1;
		}
	}
}
