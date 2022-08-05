package com.fnz.db2.journal.retrieve;

import java.util.HashMap;
import java.util.Map;

public enum JournalEntryType {
    
    ADD_ROW2("R.PT"),
    ADD_ROW1("R.PX"),
    AFTER_IMAGE("R.UP"),
    BEFORE_IMAGE("R.UB"),
    DELETE_ROW1("R.DR"),
    DELETE_ROW2("R.DL"),
	FILE_CREATED("D.CT"),
	FILE_CHANGE("D.CG"),
	START_COMMIT("C.SC"),
	END_COMMIT("C.CM"),
    OPEN("F.OP"),
    CLOSE("F.CL");
	
    public final String code;
	private JournalEntryType(String code) {
	    this.code = code;
	}
	
    private static final Map<String, JournalEntryType> BY_LABEL = new HashMap<>();
    
    static {
        for (JournalEntryType e: values()) {
            BY_LABEL.put(e.code, e);
        }
    }
	public static JournalEntryType toValue(String code) {
	    if (code == null) {
	        return null;
	    }
        return BY_LABEL.get(code);
	}
}
