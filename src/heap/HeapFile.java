package heap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

import global.GlobalConst;
import global.Minibase;
import global.PageId;
import global.RID;


public class HeapFile implements GlobalConst
{
	private String _name;
	private Header _headPage;
	private PageId _headPageId;
	private TwoWayMap _map = null;
	private int rCount = 0;
	
	/* From Java doc
	 * If the given name already denotes a file, this opens it; otherwise, this creates a new empty file. 
	 * A null name produces a temporary heap file which requires no DB entry.
	 */
	public HeapFile (String name) {
		if(name == null) {
			return;
		}

		_name = name;
		_headPageId = Minibase.DiskManager.get_file_entry(name);
		
		//check is the file exists already
		if(_headPageId == null) {
			// file does not exist, then create it 
			
			rCount = 0;
			_headPage = new Header();
			_headPageId = Minibase.BufferManager.newPage(_headPage, 1);
			_map = new TwoWayMap();
		}
		else {
			// the file exists
			
			Minibase.BufferManager.pinPage(_headPageId, _headPage, PIN_DISKIO);
			rCount = _headPage.getRecordCount();
			ArrayList<Byte> serialDataBytes = new ArrayList<Byte>();
			
			//reading data from header
			byte[] tempData = _headPage.getData();
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
				_map = (TwoWayMap) deserialize(temp);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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
		if(cFree >= record.length) {
			Minibase.BufferManager.pinPage(pageId, dPage, PIN_DISKIO);
			_map.remove(cFree, pageId.pid);
			rId = dPage.insertRecord(record);
			_map.insert(dPage.getFreeSpace(), pageId.pid);
		}
		else {
			PageId newPageId = Minibase.BufferManager.newPage(dPage, 1);
			rId = dPage.insertRecord(record);
			_map.insert(dPage.getFreeSpace(), newPageId.pid);
		}
		Minibase.BufferManager.unpinPage(pageId, true);
		return rId;
	}
	
	//Must be in O(log N)
	public boolean deleteRecord(RID rid) {
		PageId pageId = rid.pageno;
		HFPage dataPage = new HFPage();
		Minibase.BufferManager.pinPage(pageId, dataPage, PIN_DISKIO);
		_map.remove(dataPage.getFreeSpace(), pageId.pid);
		dataPage.deleteRecord(rid);
		_map.insert(dataPage.getFreeSpace(), pageId.pid);
		Minibase.BufferManager.unpinPage(pageId, true);
		return true;
	}
	
	//Must be in O(log N)
	public boolean updateRecord(RID rid, Tuple newRecord) {
		return false;
	}
	
	//get number of records in the file
	public int getRecCnt() {
		return rCount;
	}
	
	protected void finalize() {
		
	}
	
	public void deleteFile() {
		
	}

	public HeapScan openScan() {
		return null;
	}
	
	public byte[] selectRecord(RID rid) {
		return null;
	}
	
	public String toString() {
		return null;
	}
		
	//this fun in is the project doc, but not in the java doc
	public Tuple getRecord(RID rid) {
		return null;
	}
	
}
