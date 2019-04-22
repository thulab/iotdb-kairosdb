package cn.edu.tsinghua.iotdb.kairosdb.rollup;


public class RollUpException extends Exception
{
	public RollUpException(String message)
	{
		super(message);
	}

	public RollUpException(String message, Throwable cause)
	{
		super(message, cause);
	}

	public RollUpException(Throwable cause)
	{
		super(cause);
	}
}
