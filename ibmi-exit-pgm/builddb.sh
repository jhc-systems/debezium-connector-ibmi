#!/QOpenSys/pkgs/bin/bash

set -euo pipefail

if [ $# -ne 1 ]; then
    echo "Usage: $0 LIBRARY"
    echo "  e.g. $0 DEBEZIUM"
    exit 1
fi

target_lib="${1%% }"
lib_path="/QSYS.LIB/$1.LIB"
repo_base=`dirname $0`
tgtrls="*CURRENT"

if [[ ! -d  "$lib_path" ]]; then
    echo "Library $1 does not exist. Creating..."
    /QOpenSys/usr/bin/db2 "CREATE SCHEMA $target_lib"
    echo "Library $1 created."
fi

target_pgm="$target_lib/DEBEXIT"

echo "Creating tables in library ${target_lib}..."

build_cmd="RUNSQLSTM SRCSTMF('$repo_base/src/database.sql') COMMIT(*NONE) TGTRLS($tgtrls)"

liblist -c "$target_lib"
cl -S "$build_cmd"

echo "Database created."