package ch.epfl.dias.store.row;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

public class RowStore extends Store {

	private DataType[] schema;
	private String filename;
	private String delimiter;
	
	private ArrayList<DBTuple> rowStore = new ArrayList<>();

	public RowStore(DataType[] schema, String filename, String delimiter) {
		this.schema = new DataType[schema.length];
		System.arraycopy(schema, 0, this.schema, 0, schema.length);
		this.filename = filename;
		this.delimiter = delimiter;
	}

	@Override
	public void load() throws IOException {
		Path path = Paths.get(filename);
		Files.lines(path).forEach(line -> {
			String[] strFields = line.split(delimiter);
			Object[] fields = new Object[strFields.length];
			for (int i = 0; i < strFields.length; i++) {
				switch(schema[i]) {
					case INT:
						fields[i] = Integer.parseInt(strFields[i]);
						break;
					case DOUBLE:
						fields[i] = Double.parseDouble(strFields[i]);
						break;
					case BOOLEAN:
						fields[i] = Boolean.parseBoolean(strFields[i]);
						break;
					case STRING:
						fields[i] = strFields[i];
						break;
					default:
						throw new RuntimeException("Type doesn't match any DataType");
				}
			}
			rowStore.add(new DBTuple(fields, schema));
		});
		rowStore.add(new DBTuple());
	}

	@Override
	public DBTuple getRow(int rownumber) {
		return rowStore.get(rownumber);
	}
}
