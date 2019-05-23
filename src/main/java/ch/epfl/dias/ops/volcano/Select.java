package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.row.DBTuple;

public class Select implements VolcanoOperator {

	private VolcanoOperator child;
	private BinaryOp op;
	private int fieldNo;
	private int value;

	public Select(VolcanoOperator child, BinaryOp op, int fieldNo, int value) {
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = value;
	}

	@Override
	public void open() {
		child.open();
	}

	@Override
	public DBTuple next() {
		DBTuple current = child.next();
		if(current.eof) {
			return new DBTuple();
		}
		while(!current.eof && !op.apply(current.getFieldAsInt(fieldNo), value)) {
			current = child.next();
		}
		return current;
	}

	@Override
	public void close() {
		child.close();
	}
}
