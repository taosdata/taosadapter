#!/bin/bash

# Copyright (c) 2021 TAOS Data, Inc.
#
# SPDX-License-Identifier: MIT
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

# uninstall_adapter.sh — uninstall taosAdapter from the system

set -e

adapterBinary="taosadapter"
configFile="taosadapter.toml"
serviceFile="taosadapter.service"
installBin="/usr/bin/${adapterBinary}"
installConfig="/etc/taos/${configFile}"
installConfigNew="/etc/taos/${configFile}.new"
installService="/etc/systemd/system/${serviceFile}"

if [ "$(uname)" != "Linux" ]; then
  echo "Error: this uninstaller only supports Linux."
  exit 1
fi

if [ "$(id -u)" -ne 0 ]; then
  echo "Error: please run as root."
  exit 1
fi

if command -v systemctl >/dev/null 2>&1; then
  systemctl stop taosadapter >/dev/null 2>&1 || :
  systemctl disable taosadapter >/dev/null 2>&1 || :
fi

rm -f "$installBin" "$installConfig" "$installConfigNew" "$installService"

if command -v systemctl >/dev/null 2>&1; then
  systemctl daemon-reload >/dev/null 2>&1 || :
fi

echo "taosAdapter uninstalled."
