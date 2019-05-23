package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

public class Scan implements ColumnarOperator {

	private ColumnStore store;

	public Scan(ColumnStore store) {
		this.store = store;
	}

	@Override
	public DBColumn[] execute() {
		return store.getColumns(null);
	}
}
