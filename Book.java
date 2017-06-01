public class Book {
	private String id;
	private String bookname;
	private Properties properties;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getBookname() {
		return bookname;
	}

	public void setBookname(String bookname) {
		this.bookname = bookname;
	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	class Properties {
		private String subscription;
		private String unit;

		public String getSubscription() {
			return subscription;
		}

		public void setSubscription(String subscription) {
			this.subscription = subscription;
		}

		public String getUnit() {
			return unit;
		}

		public void setUnit(String unit) {
			this.unit = unit;
		}

	}

}
