package ch.epfl.dias.store.column;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

public class ColumnStore extends Store {

	private DataType[] schema;
	private String filename;
	private String delimiter;
	private boolean lateMaterialization;
	
	private ArrayList<DBColumn> columns = new ArrayList<>();

	public ColumnStore(DataType[] schema, String filename, String delimiter) {
		this(schema, filename, delimiter, false);
	}

	public ColumnStore(DataType[] schema, String filename, String delimiter, boolean lateMaterialization) {
		this.schema = new DataType[schema.length];
		System.arraycopy(schema, 0, this.schema, 0, schema.length);
		this.filename = filename;
		this.delimiter = delimiter;
		this.lateMaterialization = lateMaterialization;
	}

	@Override
	public void load() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(filename));
		String line = "";
		ArrayList<ArrayList<Object>> listTuples = new ArrayList<>();
		int nbCol = schema.length;
		for(int i = 0; i < nbCol; i++) {
			listTuples.add(new ArrayList<>());
		}
		while( (line = br.readLine()) != null){
			String[] cols = line.split(delimiter);
			for(int i = 0; i < nbCol; i++) {
				switch(schema[i]) {
					case INT:
						listTuples.get(i).add(Integer.parseInt(cols[i]));
						break;
					case DOUBLE:
						listTuples.get(i).add(Double.parseDouble(cols[i]));
						break;
					case BOOLEAN:
						listTuples.get(i).add(Boolean.parseBoolean(cols[i]));
						break;
					case STRING:
						listTuples.get(i).add(cols[i]);
						break;
					default:
						throw new RuntimeException("Type doesn't match any DataType");
				}
			}
							
		}
		br.close();
		for(int i = 0; i < nbCol; i++) {
			DBColumn col = new DBColumn(listTuples.get(i).toArray(), schema[i]);
			columns.add(col);
		}
	}

	@Override
	public DBColumn[] getColumns(int[] columnsToGet) {
		if(columnsToGet == null) {
			return columns.toArray(new DBColumn[columns.size()]);
		}
		DBColumn[] tab = new DBColumn[columnsToGet.length];
		for(int i = 0; i < columnsToGet.length; i++) {
			tab[i] = columns.get(columnsToGet[i]);
		}
		return tab;
	}
}
