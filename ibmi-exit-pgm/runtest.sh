#!/QOpenSys/pkgs/bin/bash

set -euo pipefail

test_lib=TSTDEBEXIT
lib_path="/QSYS.LIB/$test_lib.LIB"
repo_base=`dirname $0`
tgtrls="*CURRENT"

if [[ ! -d  "$lib_path" ]]; then
    echo "Library $test_lib does not exist. Setting up..."
    ${repo_base}/builddb.sh TSTDEBEXIT
fi

${repo_base}/buildpgm.sh "$test_lib"

target_pgm="$test_lib/TSTDEBEXIT"

echo "Creating program $target_pgm..."

build_cmd="CRTSQLRPGI OBJ($target_pgm) SRCSTMF('$repo_base/test/tstdebexit.sqlrpgle') TGTRLS($tgtrls) COMPILEOPT('TGTCCSID(*JOB)') RPGPPOPT(*LVL2) DBGVIEW(*SOURCE)"

cl -S "$build_cmd" > /dev/null || cl -S "$build_cmd OUTPUT(*PRINT)"  # Second attempt will give compiler listing

echo "Test program created. Executing..."

liblist -c "$test_lib"
cl "CALL TSTDEBEXIT"

echo "Test finished successfully."