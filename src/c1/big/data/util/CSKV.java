package c1.big.data.util;

import org.apache.hadoop.io.Text;

public class CSKV extends Text {
	public CSKV() {
		super();
	}
	public CSKV(String v) {
		super(v);
	}
	public CSKV(int v) {
		super(String.valueOf(v));
	}
	public CSKV(long v) {
		super(String.valueOf(v));
	}
	public CSKV(float v) {
		super(String.valueOf(v));
	}

	public CSKV(Integer v) {
		super(String.valueOf(v.intValue()));
	}
	public CSKV(Long v) {
		super(String.valueOf(v.longValue()));
	}
	public CSKV(Double v) {
		super(String.valueOf(v.doubleValue()));
	}

	public String toString() {
		return super.toString();
	}
	public Integer toInt() {
		return Integer.parseInt(this.toString());
	}
	public Long toLong() {
		return Long.parseLong(this.toString());
	}
	public Double toDouble() {
		return Double.parseDouble(this.toString());
	}

	public String[] split(String regex){
		return super.toString().split(regex);
	}
}
