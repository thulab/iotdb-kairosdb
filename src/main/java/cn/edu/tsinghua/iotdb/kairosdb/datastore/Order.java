package cn.edu.tsinghua.iotdb.kairosdb.datastore;


public enum Order
{
	ASC("asc"),
	DESC("desc");

	private String m_text;

	Order(String text)
	{
		m_text = text;
	}

	public String getText()
	{
		return (m_text);
	}


//	public static Order fromString(String text, String context) throws BeanValidationException
//	{
//		Order ret = null;
//
//		if (text != null)
//		{
//			for (Order o : Order.values())
//			{
//				if (text.equalsIgnoreCase(o.m_text))
//				{
//					ret = o;
//					break;
//				}
//			}
//		}
//
//
//		if (ret == null)
//		{
//			throw new BeanValidationException(new QueryParser.SimpleConstraintViolation("order", "must be either 'asc' or 'desc'"), context);
//		}
//		else
//			return (ret);
//	}
}
