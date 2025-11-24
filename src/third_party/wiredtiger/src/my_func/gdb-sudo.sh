#!/bin/sh
# exec を使ってプロセスを置換する (推奨)
exec sudo /usr/bin/gdb "$@"