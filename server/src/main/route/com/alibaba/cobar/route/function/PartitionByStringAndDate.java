package com.alibaba.cobar.route.function;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.alibaba.cobar.config.model.rule.RuleAlgorithm;
import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.expression.primary.function.FunctionExpression;
import com.alibaba.cobar.parser.util.Pair;
import com.alibaba.cobar.parser.util.PairUtil;
import com.alibaba.cobar.route.util.PartitionUtil;
import com.alibaba.cobar.util.SplitUtil;
import com.alibaba.cobar.util.StringUtil;

/**
 * 
 * @author JJF
 *
 */
public class PartitionByStringAndDate extends PartitionFunction implements RuleAlgorithm {
    public PartitionByStringAndDate(String functionName) {
        this(functionName, null);
    }

    public PartitionByStringAndDate(String functionName, List<Expression> arguments) {
        super(functionName, arguments);
    }

    private static final int PARTITION_KEY_TYPE_LONG = 1;
    private static final int PARTITION_KEY_TYPE_STRING = 2;

    private int[] countX;
    private int[] lengthX;
    private int[] countY;
    private int[] lengthY;

    private static int convertType(String keyType) {
        if ("long".equalsIgnoreCase(keyType))
            return PARTITION_KEY_TYPE_LONG;
        if ("string".equalsIgnoreCase(keyType))
            return PARTITION_KEY_TYPE_STRING;
        throw new IllegalArgumentException("unknown partition key type: " + keyType);
    }

    public void setKeyTypeX(String keyTypeX) {
        this.keyTypeX = convertType(keyTypeX);
    }

    public void setKeyTypeY(String keyTypeY) {
        this.keyTypeY = convertType(keyTypeY);
    }

    public void setPartitionCountX(String partitionCount) {
        this.countX = toIntArray(partitionCount);
        this.xSize = 0;
        for (int c : countX)
            this.xSize += c;
    }

    public void setPartitionLengthX(String partitionLength) {
        this.lengthX = toIntArray(partitionLength);
    }

    public void setHashLengthX(int hashLengthX) {
        setHashSliceX(String.valueOf(hashLengthX));
    }

    public void setHashSliceX(String hashSlice) {
        Pair<Integer, Integer> p = PairUtil.sequenceSlicing(hashSlice);
        hashSliceStartX = p.getKey();
        hashSliceEndX = p.getValue();
    }

    public void setPartitionCountY(String partitionCount) {
        this.countY = toIntArray(partitionCount);
        this.ySize = 0;
        for (int c : countY)
            this.ySize += c;
    }

    public void setPartitionLengthY(String partitionLength) {
        this.lengthY = toIntArray(partitionLength);
    }

    public void setHashLengthY(int hashLengthY) {
        setHashSliceY(String.valueOf(hashLengthY));
    }

    public void setHashSliceY(String hashSlice) {
        Pair<Integer, Integer> p = PairUtil.sequenceSlicing(hashSlice);
        hashSliceStartY = p.getKey();
        hashSliceEndY = p.getValue();
    }

    private static int[] toIntArray(String string) {
        String[] strs = SplitUtil.split(string, ',', true);
        int[] ints = new int[strs.length];
        for (int i = 0; i < strs.length; ++i) {
            ints[i] = Integer.parseInt(strs[i]);
        }
        return ints;
    }

    private int xSize;
    private int keyTypeX = PARTITION_KEY_TYPE_LONG;
    private int hashSliceStartX = 0;
    private int hashSliceEndX = 8;
    private PartitionUtil partitionUtilX;
    private int ySize;
    private int keyTypeY = PARTITION_KEY_TYPE_LONG;
    private int hashSliceStartY = 0;
    private int hashSliceEndY = 8;
    private PartitionUtil partitionUtilY;

    private Integer[][] byX;
    private Integer[][] byY;
    private Integer[] all;

    private void buildByX() {
        byX = new Integer[xSize][ySize];
        for (int x = 0; x < xSize; ++x) {
            for (int y = 0; y < ySize; ++y) {
                byX[x][y] = getByXY(x, y);
            }
        }
    }

    private void buildByY() {
        byY = new Integer[ySize][xSize];
        for (int y = 0; y < ySize; ++y) {
            for (int x = 0; x < xSize; ++x) {
                byY[y][x] = getByXY(x, y);
            }
        }
    }

    private void buildAll() {
        int size = xSize * ySize;
        all = new Integer[size];
        for (int i = 0; i < size; ++i)
            all[i] = i;
    }

    private Integer[] getAll() {
        return all;
    }

    private Integer[] getByX(final int x) {
        return byX[x];
    }

    private Integer[] getByY(final int y) {
        return byY[y];
    }

    private Integer getByXY(int x, int y) {
        if (x >= xSize || y >= ySize)
            throw new IllegalArgumentException("x, y out of bound: x=" + x + ", y=" + y);
        return x + xSize * y;
    }

    /**
     * @return null if eval invalid type
     */
    private static Integer calculate(Object eval, PartitionUtil partitionUtil, int keyType, int hashSliceStart,
                                     int hashSliceEnd) {
        if (eval == UNEVALUATABLE || eval == null)
            return null;
        switch (keyType) {
        case PARTITION_KEY_TYPE_LONG:
            long longVal;
            if (eval instanceof Number) {
                longVal = ((Number) eval).longValue();
            } else if (eval instanceof String) {
                longVal = Long.parseLong((String) eval);
            } else {
                throw new IllegalArgumentException("unsupported data type for partition key: " + eval.getClass());
            }
            return partitionUtil.partition(longVal);
        case PARTITION_KEY_TYPE_STRING:
            String key = String.valueOf(eval);
            int start = hashSliceStart >= 0 ? hashSliceStart : key.length() + hashSliceStart;
            int end = hashSliceEnd > 0 ? hashSliceEnd : key.length() + hashSliceEnd;
            long hash = StringUtil.hash(key, start, end);
            return partitionUtil.partition(hash);
        default:
            throw new IllegalArgumentException("unsupported partition key type: " + keyType);
        }
    }

    @Override
    public Integer[] evaluationInternal(Map<? extends Object, ? extends Object> parameters) {
        return calculate(parameters);
    }

    // modify by JJF  For sihuatech
    private Integer[] eval(Object xInput, Object yInput) {
    	//start
    	String yString = strToDate(yInput);
    	//System.out.println(yString);
    	if(null != yString) {
    		Integer[] xArray = new Integer[1];
    		xArray[0] = calculate(yString + xInput, partitionUtilX, keyTypeX, hashSliceStartX, hashSliceEndX);
        	return xArray;
    	}
    	//end
        Integer x = calculate(xInput, partitionUtilX, keyTypeX, hashSliceStartX, hashSliceEndX);
        Integer y = calculate(yInput, partitionUtilY, keyTypeY, hashSliceStartY, hashSliceEndY);
        if (x != null && y != null) {
            return new Integer[] { getByXY(x, y) };
        } else if (x == null && y != null) {
            return getByY(y);
        } else if (x != null && y == null) {
            return getByX(x);
        } else {
            return getAll();
        }
    }
    
    @SuppressWarnings("deprecation")
	private static String strToDate(Object obj) {
    	if(null == obj) {
    		return null;
    	}
    	if(obj instanceof String) {
    		String date = (String) obj;
    		if(date.startsWith("str_to_date(")) {
    			date = date.substring(12);
    		}
    		if(date.length() >= 10 && ((date.charAt(4) == '-' && date.charAt(7) == '-') 
    				|| (date.charAt(4) == '/' && date.charAt(7) == '/'))) {
    			return String.valueOf(date.charAt(5)) + String.valueOf(date.charAt(6)) 
    					+ String.valueOf(date.charAt(8)) + String.valueOf(date.charAt(9));
    		} else if(date.length() >= 8 && -1 == date.indexOf("-") && -1 == date.indexOf("/")) {
    			return date.substring(4, 8);
    		}
    	} else if(obj instanceof Date) {
    		Date date = (Date) obj;
    		return String.valueOf(date.getMonth()) + String.valueOf(date.getDay());
    	} else if(obj instanceof java.sql.Date) {
    		java.sql.Date date = (java.sql.Date) obj;
    		return String.valueOf(date.getMonth()) + String.valueOf(date.getDay()); 
    	}
    	return null;
    }

    @Override
    public void init() {
        initialize();
    }

    @Override
    public void initialize() {
        partitionUtilX = new PartitionUtil(countX, lengthX);
        partitionUtilY = new PartitionUtil(countY, lengthY);
        buildAll();
        buildByX();
        buildByY();
    }

    @Override
    public FunctionExpression constructFunction(List<Expression> arguments) {
        if (arguments == null || arguments.size() != 2)
            throw new IllegalArgumentException("function " + getFunctionName() + " must have 2 arguments but is "
                    + arguments);
        Object[] args = new Object[arguments.size()];
        int i = -1;
        for (Expression arg : arguments) {
            args[++i] = arg;
        }
        return (FunctionExpression) constructMe(args);
    }

    @Override
    public RuleAlgorithm constructMe(Object... objects) {
        List<Expression> args = new ArrayList<Expression>(objects.length);
        for (Object obj : objects) {
            args.add((Expression) obj);
        }
        PartitionByStringAndDate rst = new PartitionByStringAndDate(functionName, args);
        rst.countX = countX;
        rst.xSize = xSize;
        rst.lengthX = lengthX;
        rst.keyTypeX = keyTypeX;
        rst.hashSliceStartX = hashSliceStartX;
        rst.hashSliceEndX = hashSliceEndX;
        rst.countY = countY;
        rst.ySize = ySize;
        rst.lengthY = lengthY;
        rst.keyTypeY = keyTypeY;
        rst.hashSliceStartY = hashSliceStartY;
        rst.hashSliceEndY = hashSliceEndY;
        return rst;
    }

    @Override
    public Integer[] calculate(Map<? extends Object, ? extends Object> parameters) {
        if (arguments == null || arguments.size() < 2)
            throw new IllegalArgumentException("arguments.size < 2 for function of " + getFunctionName());
        Object xInput = arguments.get(0).evaluation(parameters);
        Object yInput = arguments.get(1).evaluation(parameters);

        return eval(xInput, yInput);
    }

    public static void main(String[] args) throws Exception {
	    PartitionByStringAndDate func = new PartitionByStringAndDate("test999", new ArrayList<Expression>(2));
	    func.setKeyTypeX("string");
	    func.setPartitionCountX("2,8");
	    func.setPartitionLengthX("64,112");
	    func.setKeyTypeY("string");
	    func.setPartitionCountY("2,8");
	    func.setPartitionLengthY("64,112");
	    func.setHashLengthY(8);
	    func.setHashLengthX(20);
	    func.init();
	    
	    int[] count = new int[10];
	    for(int i=0; i<count.length; i++) {
	    	count[i] = 0;
	    }
	    
	    for(int i=1; i<31; i++) {
	    	String ii = i+ "";
	    	if(i < 10) {
	    		ii = "0" + ii;
	    	}
	    	for(int j=1; j<31; j++) {
	    		String jj = j + "";
	    		if(j < 10) {
	    			jj = "0" + jj;
	    		}
	    		Integer[] ints=func.eval("cztv", "2015" + ii + jj);
	    		for(Integer t:ints){
	    			System.out.println(t);
	    			count[t]++;
	    		}
	    	}
	    }
	    
	    for(int i = 0; i<count.length; i++) {
	    	System.out.println(i + " : " + count[i]);
	    }
	    
	    
	    System.out.println("str_to_date(201409023".substring(12));
	    
    }
}
