package PartieA;

public class Message {
    public enum Type { REQUEST, REPLY, RELEASE }
    Type type;
    int senderId;
    int timestamp;

    public Message(Type type, int senderId, int timestamp) {
        this.type = type;
        this.senderId = senderId;
        this.timestamp = timestamp;
    }
}

