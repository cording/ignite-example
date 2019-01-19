package org.cord.ignite.kafka;

import com.google.gson.JsonObject;

import java.util.Date;

/**
 * @author: cord
 * @date: 2019/1/19 0:08
 */
public class GoldenGateMsg {

    private String table;

    private String opType;

    private Date opTs;

    private String currentTs;

    private String pos;

    private JsonObject before;

    private JsonObject after;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getOpType() {
        return opType;
    }

    public void setOpType(String opType) {
        this.opType = opType;
    }

    public Date getOpTs() {
        return opTs;
    }

    public void setOpTs(Date opTs) {
        this.opTs = opTs;
    }

    public String getCurrentTs() {
        return currentTs;
    }

    public void setCurrentTs(String currentTs) {
        this.currentTs = currentTs;
    }

    public String getPos() {
        return pos;
    }

    public void setPos(String pos) {
        this.pos = pos;
    }

    public JsonObject getBefore() {
        return before;
    }

    public void setBefore(JsonObject before) {
        this.before = before;
    }

    public JsonObject getAfter() {
        return after;
    }

    public void setAfter(JsonObject after) {
        this.after = after;
    }
}
