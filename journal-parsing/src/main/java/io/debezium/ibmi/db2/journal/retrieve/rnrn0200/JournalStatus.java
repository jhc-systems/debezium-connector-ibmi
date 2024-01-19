/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve.rnrn0200;

public enum JournalStatus {
    Attached(1),
    OnlineSavedDetached(2),
    SavedDetchedNotFreed(3),
    SavedDetachedFreed(4),
    Partial(5),
    Empty(6);

    private final int type;

    JournalStatus(int type) {
        this.type = type;
    }

    public static JournalStatus valueOf(int value) {
        for (JournalStatus e : values()) {
            if (e.type == value) {
                return e;
            }
        }
        return null;
    }

    public static JournalStatus valueOfString(String svalue) {
        int value = Integer.valueOf(svalue);
        for (JournalStatus e : values()) {
            if (e.type == value) {
                return e;
            }
        }
        return null;
    }
    /*
     * see https://www.ibm.com/docs/en/i/7.3?topic=ssw_ibm_i_73/apis/QJORJRNI.htm "Journal receiver status."
     * 1 The journal receiver is currently attached to the journal.
     * 2 The journal receiver is online. The journal receiver has not been saved, and it has been detached from the journal.
     * 3 The journal receiver was saved after it was detached. The journal receiver storage was not freed when it was saved.
     * 4 The journal receiver was saved after it was detached. The journal receiver storage was freed when it was saved.
     * 5 The journal receiver status is partial for one of the following reasons:
     * It was restored from a version that was saved while it was attached to the journal. Additional journal entries may have been written that were not restored.
     * It was one of a pair of dual journal receivers, and it was found damaged while attached to the journal. The journal receiver has since been detached. This journal receiver is considered partial
     * because additional journal entries may have been written to the dual journal receiver.
     * It is associated with a remote journal and it does not contain all the journal entries that are in the corresponding journal receiver associated with the source journal.
     * 6 The journal receiver status is empty, since the receiver has never been attached to a journal.
     */
}
