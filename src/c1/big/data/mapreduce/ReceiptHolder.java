package c1.big.data.mapreduce;

public class ReceiptHolder {
	private String id;
	private long value;

	private boolean isFood;
	private boolean isDrink;

	public ReceiptHolder(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

	public boolean equalId(String id) {
		return this.id.equals(id);
	}

	public long getValue() {
		return value;
	}

	public void addValue(long value) {
		this.value += value;
	}

	public void addValue(String value) {
		this.value += Long.parseLong(value);
	}

	public boolean canEmit() {
		return isFood && isDrink;
	}

	public boolean isFood() {
		return isFood;
	}

	public void setIsFood(boolean isFood) {
		this.isFood = isFood;
	}

	public boolean isDrink() {
		return isDrink;
	}

	public void setIsDrink(boolean isDrink) {
		this.isDrink = isDrink;
	}

}
