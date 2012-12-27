package com.volkan;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Utility {

	public static String buildLogFileName () {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddhhmm");
		return "hede" + simpleDateFormat.format(new Date());
	}
}
