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

# install_adapter.sh — install taosAdapter from package.tar.gz

set -e

productName="taosAdapter"
adapterBinary="taosadapter"
configFile="taosadapter.toml"
serviceFile="taosadapter.service"
packageFile="package.tar.gz"
installBinDir="/usr/bin"
installConfigDir="/etc/taos"
installServiceDir="/etc/systemd/system"
silent_mode=0

show_help() {
  cat <<USAGE
${productName} Installer.

Usage: $(basename "$0") [-s] [-h]

Options:
  -s    Silent mode installation
  -h    Show help
USAGE
}

while getopts "sh" arg; do
  case "$arg" in
    s) silent_mode=1 ;;
    h)
      show_help
      exit 0
      ;;
    ?)
      show_help
      exit 1
      ;;
  esac
done

if [ "$(uname)" != "Linux" ]; then
  echo "Error: this installer only supports Linux."
  exit 1
fi

if [ "$(id -u)" -ne 0 ]; then
  echo "Error: please run as root."
  exit 1
fi

script_dir="$(dirname "$(readlink -f "$0")")"

if [ ! -f "${script_dir}/${packageFile}" ]; then
  echo "Error: ${packageFile} not found in ${script_dir}"
  exit 1
fi

if [ "$silent_mode" -ne 1 ]; then
  printf 'Install %s to this system? [Y/n] ' "$productName"
  read -r confirm
  if [[ -n "$confirm" && ! "$confirm" =~ ^[Yy]$ ]]; then
    echo "Installation cancelled."
    exit 0
  fi
fi

work_dir="${script_dir}/package"
rm -rf "$work_dir"
mkdir -p "$work_dir"
tar -zxf "${script_dir}/${packageFile}" -C "$work_dir"

binary_src="${work_dir}/bin/${adapterBinary}"
config_src="${work_dir}/cfg/${configFile}"
service_src="${work_dir}/cfg/${serviceFile}"

for f in "$binary_src" "$config_src" "$service_src"; do
  if [ ! -f "$f" ]; then
    echo "Error: packaged file missing: $f"
    exit 1
  fi
done

mkdir -p "$installBinDir" "$installConfigDir" "$installServiceDir"
install -m 755 "$binary_src" "${installBinDir}/${adapterBinary}"

if [ -f "${installConfigDir}/${configFile}" ]; then
  install -m 644 "$config_src" "${installConfigDir}/${configFile}.new"
  echo "Existing config preserved: ${installConfigDir}/${configFile}"
  echo "New config saved as: ${installConfigDir}/${configFile}.new"
else
  install -m 644 "$config_src" "${installConfigDir}/${configFile}"
fi

install -m 644 "$service_src" "${installServiceDir}/${serviceFile}"

if command -v systemctl >/dev/null 2>&1; then
  if ! systemctl daemon-reload; then
    echo "Warning: systemctl daemon-reload failed. Please reload systemd manually."
  fi
else
  echo "Warning: systemctl not found. Please install the service manually."
fi

lib_found=0
if command -v ldconfig >/dev/null 2>&1; then
  if ldconfig -p 2>/dev/null | grep -q 'libtaosnative\.so'; then
    lib_found=1
  fi
fi
if [ "$lib_found" -eq 0 ]; then
  for path in /usr/local/taos/driver/libtaosnative.so /usr/lib/libtaosnative.so /usr/lib64/libtaosnative.so /usr/local/lib/libtaosnative.so; do
    if [ -e "$path" ]; then
      lib_found=1
      break
    fi
  done
fi
if [ "$lib_found" -eq 0 ]; then
  echo "Warning: libtaosnative.so was not found in the system library paths."
  echo "         ${productName} loads the TDengine client driver at run time, so it also"
  echo "         searches its own directory, <dir>/../lib, <dir>/../driver and"
  echo "         LD_LIBRARY_PATH (DYLD_LIBRARY_PATH on macOS)."
  echo "         Install the taos-community package first, or point TDENGINE_DRIVER_PATH"
  echo "         at the driver to use."
fi

rm -rf "$work_dir"

echo ""
echo "${productName} installed successfully."
echo "Usage tips:"
echo "  1. Edit ${installConfigDir}/${configFile} if needed."
echo "  2. Start service: systemctl start taosadapter"
echo "  3. Enable at boot: systemctl enable taosadapter"
echo "  4. Run manually: ${installBinDir}/${adapterBinary} -c ${installConfigDir}/${configFile}"
