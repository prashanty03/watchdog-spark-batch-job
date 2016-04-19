package com.watchdog.model;

public enum DeviceTypes {
REFRIGERATOR("refrigerator"),
TELEVISION("television");


private final String deviceType;

private DeviceTypes(final String deviceType) {
    this.deviceType = deviceType;
}
@Override
public String toString() {
    return deviceType;
}
};
