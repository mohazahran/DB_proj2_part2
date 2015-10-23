package heap;

import java.util.Arrays;

import global.Convert;
import global.Page;
import global.PageId;

public class Header extends Page {
	
	public void setRecordCount(int rCount) {
		Convert.setIntValue(rCount, 0, data);
	}
	
	public int getRecordCount() {
		return Convert.getIntValue(0, Arrays.copyOfRange(data, 0, Integer.BYTES));
	}
	
	public void setNextHeader(PageId pageId) {
		Convert.setIntValue(pageId.pid, Integer.BYTES, data);
	}
	
	public PageId getNextHeader() {
		return new PageId(Convert.getIntValue(0, Arrays.copyOfRange(data, Integer.BYTES, 2*Integer.BYTES)));
	}
	
	public void setDataPageId(PageId pageId) {
		Convert.setIntValue(pageId.pid, 2*Integer.BYTES, data);
	}
	
	public PageId getDataPageId() {
		return new PageId(Convert.getIntValue(0, Arrays.copyOfRange(data, 2*Integer.BYTES, 3*Integer.BYTES)));
	}
	
	private void setDataSize(int size) {
		Convert.setIntValue(size, 3*Integer.BYTES, data);
	}
	
	public int getDataSize() {
		return Convert.getIntValue(0, Arrays.copyOfRange(data, 3*Integer.BYTES, 4*Integer.BYTES));
	}
	
	public void setMetaData(byte[] _data) throws Exception {
		if(_data.length > PAGE_SIZE-4*Integer.BYTES) {
			throw new Exception("Header Exceeded Maximum Buffer Size");
		}
		
		setDataSize(_data.length);
		System.arraycopy(_data, 0, data, 4*Integer.BYTES, _data.length);
	}
	
	public byte[] getMetaData() {
		int size = getDataSize();
		return Arrays.copyOfRange(data, 4*Integer.BYTES, 4*Integer.BYTES+size);
	}
}
