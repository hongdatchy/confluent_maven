package org.example;

public class Message {
    private String key;
    private String value;

    public String getKey() {
        return key;
    }
    public void setKey(String key) {
        this.key = key;
    }
    public String getValue() {
        return value;
    }
    public void setValue(String value) {
        this.value = value;
    }

    public Message(String key, String value) {
        this.key = key;
        this.value = value;
    }
}
