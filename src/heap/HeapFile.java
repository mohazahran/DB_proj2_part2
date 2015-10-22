package heap;

import global.GlobalConst;
import global.Minibase;
import global.PageId;
import global.RID;


public class HeapFile implements GlobalConst
{
	private String _name;
	private boolean _isTemp;
	private Header _headPage;
	private PageId headPageId;
	static private TwoWayMap _map = null;
	
	
	/* From Java doc
	 * If the given name already denotes a file, this opens it; otherwise, this creates a new empty file. 
	 * A null name produces a temporary heap file which requires no DB entry.
	 */
	public HeapFile (String name)
	{
		_name = name;
		if(_name == null)
		{
			_isTemp = true;
		}
		else
		{
			_isTemp = false;
			//check is the file exists already
			PageId fHeadId = Minibase.DiskManager.get_file_entry(name);
			if(fHeadId == null) // file does not exist, then create it 
			{
				_isTemp = false;
				_name = name;
				_headPage = new Header();				
				headPageId = Minibase.BufferManager.newPage(_headPage, 1);
				_map = new TwoWayMap();
			}
			else // the file exists
			{
				
			}
				
		}		
			
	};
	public void deleteFile()
	{
		
	};
	//Must be in O(log N)
	public boolean deleteRecord(RID rid)
	{
		return false;
	};
	protected void finalize()
	{
		
	};
	public int getRecCnt() //get number of records in the file
	{
		return -1;
	};
	
	//Must be in O(log N)
	public RID insertRecord(byte[] record)
	{
		return null;
	};
	public HeapScan openScan()
	{
		return null;
	};
	public byte[] selectRecord(RID rid) 
	{
		return null;
	};
	
	public String toString()
	{
		return null;
	};
	
	public boolean updateRecord(RID rid, Tuple newRecord)
	//Must be in O(log N)
	{
		return false;
	};
		
	//this fun in is the project doc, but not in the java doc
	public Tuple getRecord(RID rid)
	{
		return null;
	};
	
	
	
	
	

}
