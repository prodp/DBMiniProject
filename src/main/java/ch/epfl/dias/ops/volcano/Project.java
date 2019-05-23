package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

public class Project implements VolcanoOperator {

	private VolcanoOperator child;
	private int[] fieldNo;

	public Project(VolcanoOperator child, int[] fieldNo) {
		this.child = child;
		this.fieldNo = fieldNo;
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
		int tupleSize = fieldNo.length;
		Object[] nextFields = new Object[tupleSize];
		DataType[] nextTypes = new DataType[tupleSize];
		for(int i = 0; i < tupleSize; i++){
			switch(current.types[fieldNo[i]]) {
				case INT: 
					nextFields[i] = current.getFieldAsInt(fieldNo[i]);
					nextTypes[i] = DataType.INT;
					break;
				case DOUBLE: 
					nextFields[i] = current.getFieldAsDouble(fieldNo[i]);
					nextTypes[i] = DataType.DOUBLE;
					break;
				case BOOLEAN: 
					nextFields[i] = current.getFieldAsBoolean(fieldNo[i]);
					nextTypes[i] = DataType.BOOLEAN;
					break;
				case STRING: 
					nextFields[i] = current.getFieldAsString(fieldNo[i]);
					nextTypes[i] = DataType.STRING;
					break;
				default:
					throw new RuntimeException("Type error");	
			}
		}
		return new DBTuple(nextFields, nextTypes);
	}

	@Override
	public void close() {
		child.close();
	}
}
