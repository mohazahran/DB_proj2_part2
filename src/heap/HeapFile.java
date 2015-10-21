package heap;

import global.GlobalConst;
import global.Minibase;
import global.Page;
import global.PageId;
import global.RID;


public class HeapFile implements GlobalConst
{		
	public String _name;
	public boolean _isTemp;
	public Page headPage;
	public PageId headPageId;
	
	/* From Java doc
	 * If the given name already denotes a file, this opens it; otherwise, this creates a new empty file. 
	 * A null name produces a temporary heap file which requires no DB entry.
	 */
	public HeapFile (String name)
	{
		if(name == null)
		{
			_isTemp = true;
		}
		else
		{
			//check is the file exists already
			PageId fHeadId = Minibase.DiskManager.get_file_entry(name);
			if(fHeadId == null) // file does not exist, then create it 
			{
				_isTemp = false;
				_name = name;
				headPage = new Page();				
				headPageId = Minibase.BufferManager.newPage(headPage, 1);
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
	public void deleteRecord(RID rid)
	{
		
	};
	protected void finalize()
	{
		
	};
	public int getRecCnt() //get number of records in the file
	{
		
	};
	
	//Must be in O(log N)
	public RID insertRecord(byte[] record)
	{
		
	};
	public HeapScan openScan()
	{
		
	};
	public byte[] selectRecord(RID rid) 
	{
		
	};
	
	public String toString()
	{
		
	};
	
	//public void updateRecord(RID rid, Tuple newRecord)
	//Must be in O(log N)
	public void updateRecord(RID rid, byte[] newRecord)
	{
		
	};
		
	//this fun in is the project doc, but not in the java doc
	public Tuple getRecord(RID rid)
	{
		
	};
	
	
	
	
	

}
