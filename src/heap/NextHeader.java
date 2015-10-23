package heap;

import java.util.Arrays;

import global.Convert;
import global.Page;
import global.PageId;

public class NextHeader extends Page {
	
	public void setNextHeader(PageId pageId) {
		Convert.setIntValue(pageId.pid, 0, data);
	}
	
	public PageId getNextHeader() {
		return new PageId(Convert.getIntValue (0, Arrays.copyOfRange(data, 0, Integer.BYTES)));
	}
	
	private void setDataSize(int size) {
		Convert.setIntValue(size, Integer.BYTES, data);
	}
	
	public int getDataSize() {
		return Convert.getIntValue(0, Arrays.copyOfRange(data, Integer.BYTES, 2*Integer.BYTES));
	}
	
	public void setMetaData(byte[] _data) throws Exception {
		if(_data.length > PAGE_SIZE-2*Integer.BYTES) {
			throw new Exception("Header Exceeded Maximum Buffer Size");
		}
		
		setDataSize(_data.length);
		System.arraycopy(_data, 0, data, 2*Integer.BYTES, _data.length);
	}
	
	public byte[] getMetaData() {
		int size = getDataSize();
		return Arrays.copyOfRange(data, 2*Integer.BYTES, 2*Integer.BYTES+size);
	}
}
