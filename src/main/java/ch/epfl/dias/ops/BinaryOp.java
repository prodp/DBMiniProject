package ch.epfl.dias.ops;

public enum BinaryOp {
	LT {
    	public boolean apply(Integer x, int y) {
    		return x < y;
    	}
    },LE{
    	public boolean apply(Integer x, int y) {
    		return x <= y;
    	}
    },EQ{
    	public boolean apply(Integer x, int y) {
    		return x == y;
    	}
    },NE{
    	public boolean apply(Integer x, int y) {
    		return x != y;
    	}
    },GT{
    	public boolean apply(Integer x, int y) {
    		return x > y;
    	}
    },GE{
    	public boolean apply(Integer x, int y) {
    		return x >= y;
    	}
    };
    
    public abstract boolean apply(Integer fieldAsInt, int value);
}
