#!/bin/bash

# Set Oracle environment variables
export ORACLE_HOME=~/oracle/instantclient_19_19
export PATH=$ORACLE_HOME:$PATH
export DYLD_LIBRARY_PATH=$ORACLE_HOME:$DYLD_LIBRARY_PATH
export OCI_LIB_DIR=$ORACLE_HOME
export OCI_INC_DIR=$ORACLE_HOME/sdk/include

# Create symbolic links if they don't exist
if [ ! -f $ORACLE_HOME/libclntsh.dylib ]; then
    cd $ORACLE_HOME
    ln -s libclntsh.dylib.19.1 libclntsh.dylib
fi

# Print environment variables
echo "Oracle environment variables set:"
echo "ORACLE_HOME=$ORACLE_HOME"
echo "PATH=$PATH"
echo "DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH"
echo "OCI_LIB_DIR=$OCI_LIB_DIR"
echo "OCI_INC_DIR=$OCI_INC_DIR" 