package ch.epfl.dias.store.column;

import java.util.ArrayList;
import java.util.Arrays;

import ch.epfl.dias.store.DataType;

public class DBColumn {

	public Object[] tuples;
	public DataType type;
	
	public DBColumn(Object[] tuples, DataType type) {
		this.tuples = tuples;
		this.type = type;
	}
	
	public Integer[] getAsInteger() {
		return Arrays.stream(tuples)
				.toArray(Integer[]::new);
	}
	
	public Double[] getAsDouble() {
		return Arrays.stream(tuples)
				.toArray(Double[]::new);
	}

	public Boolean[] getAsBoolean() {
		return Arrays.stream(tuples)
				.toArray(Boolean[]::new);
	}

	public String[] getAsString() {
		return Arrays.stream(tuples)
				 .map(Object::toString)
				 .toArray(String[]::new);
	}
}
