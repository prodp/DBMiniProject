package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class Scan implements VolcanoOperator {

	private Store store;
	private int lineNumber;

	public Scan(Store store) {
		this.store = store;
	}

	@Override
	public void open() {
		lineNumber = 0;
	}

	@Override
	public DBTuple next() {
		DBTuple next = store.getRow(lineNumber);
		++lineNumber;
		return next;
	}

	@Override
	public void close() {
		lineNumber = 0;
	}
}