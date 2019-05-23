package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class HashJoin implements VolcanoOperator {

	private VolcanoOperator leftChild;
	private VolcanoOperator rightChild;
	private int leftFieldNo;
	private int rightFieldNo;
	private DBTuple currLeft;

	public HashJoin(VolcanoOperator leftChild, VolcanoOperator rightChild, int leftFieldNo, int rightFieldNo) {
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
	}

	@Override
	public void open() {
		leftChild.open();
		rightChild.open();
		this.currLeft = leftChild.next();
	}

	@Override
	public DBTuple next() {
		if(this.currLeft.eof) {
			return new DBTuple();
		}
		DBTuple currRight;
		boolean cond = true;
		do {
			currRight = rightChild.next();
			if(currRight.eof) {
				this.currLeft = leftChild.next();
				if(this.currLeft.eof) {
					return new DBTuple();
				}
				rightChild.close();
				rightChild.open();
				currRight = rightChild.next();
			}
			DataType leftType = currLeft.types[leftFieldNo];
			DataType rightType = currRight.types[rightFieldNo];
			if( leftType != rightType && 
					!((leftType == DataType.INT || leftType == DataType.DOUBLE) && 
					rightType == DataType.INT || rightType == DataType.DOUBLE)) {
				throw new RuntimeException("Join comparison with uncompatible types");
			}
			if(leftType == DataType.INT && rightType == DataType.DOUBLE) {
				if(currLeft.getFieldAsInt(leftFieldNo).intValue() == currRight.getFieldAsDouble(rightFieldNo).doubleValue()) {
					cond = false;
				}
			}else if(leftType == DataType.DOUBLE && rightType == DataType.INT) {
				if(currLeft.getFieldAsDouble(leftFieldNo).doubleValue() == currRight.getFieldAsInt(rightFieldNo).intValue()) {
					cond = false;
				}
			}
			else if(currLeft.fields[leftFieldNo].equals(currRight.fields[rightFieldNo])) {
				cond = false;
			}
		} while(cond);
		
		//join and projection where fields[rightFieldNo] is dropped
		int tupleSize = currLeft.fields.length + currRight.fields.length-1;
		Object[] nextFields = new Object[tupleSize];
		DataType[] nextTypes = new DataType[tupleSize];
		int currentPos = 0;
		for (int i = 0; i < currLeft.fields.length; i++) {
			nextFields[currentPos] = currLeft.fields[i];
			nextTypes[currentPos] = currLeft.types[i];
			++currentPos;
		}
		for (int i = 0; i < currRight.fields.length-1; i++) {
			if(i >= rightFieldNo) {
				nextFields[currentPos] = currRight.fields[i+1];
				nextTypes[currentPos] = currRight.types[i+1];
			}else {
				nextFields[currentPos] = currRight.fields[i];
				nextTypes[currentPos] = currRight.types[i];
			}
			++currentPos;
		}
		return new DBTuple(nextFields, nextTypes);
	}

	@Override
	public void close() {
		leftChild.close();
		rightChild.close();
		this.currLeft = null;
	}
}
