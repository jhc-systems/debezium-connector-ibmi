#!/QOpenSys/pkgs/bin/bash

set -exuo pipefail

if [ $# -lt 1 -o $# -gt 2 ]; then
    echo "Usage: $0 LIBRARY [DATA_LIBRARY]"
    echo "  e.g. $0 DEBEZIUM"
    echo "  e.g. $0 DEBEZIUM MYDATABASE"
    exit 1
fi

library="${1^^}"

system "ADDEXITPGM EXITPNT(QIBM_QJO_DLT_JRNRCV) FORMAT(DRCV0100) PGMNBR(*LOW) PGM($library/DEBEXIT) THDSAFE(*YES)"