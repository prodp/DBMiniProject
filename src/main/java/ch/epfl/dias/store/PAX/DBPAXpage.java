package ch.epfl.dias.store.PAX;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class DBPAXpage {

	public Object[][] data;
	public DataType[] types;
	
	public DBPAXpage(Object[][] data, DataType[] types) {
		this.data = data;
		this.types = types;
	}
}
