package com.dataman.webservice.util;

import java.io.UnsupportedEncodingException;

import org.apache.commons.codec.binary.Base64;

public class Base64Util {
	public static String decodeBase64withUTF8(String msgbase64) {
		byte[] msgafterdecode = Base64.decodeBase64(msgbase64);
		try {
			return new String(msgafterdecode, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	public static String encodeUTF8String(String msg) {
		String msgbase64 = null;
		try {
			msgbase64 = Base64.encodeBase64String(msg.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
		return msgbase64;
	}

	public static String encode(byte[] msg){
		String msgbase64 = Base64.encodeBase64String(msg);
		return msgbase64;
	}
}
