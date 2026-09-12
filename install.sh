#!/bin/bash
#
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
#

set -euo pipefail

abort() {
  printf "%s\n" "$@"
  exit 1
}

require_cmd() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    abort "Required command not found: ${cmd}"
  fi
}

if [[ -z "${BASH_VERSION:-}" ]]; then
  abort "Bash is required to interpret this script."
fi

for cmd in uname id curl tar mkdir chmod chown rm mv tr sed head tail mktemp grep readlink; do
  require_cmd "${cmd}"
done

CURRENT_USER="${USER:-$(id -un)}"
GROUP="$(id -gn)"
CHOWN="$(command -v chown)"
CHMOD="$(command -v chmod)"
CURL="$(command -v curl)"
TAR="$(command -v tar)"
MKDIR="$(command -v mkdir)"
RM="$(command -v rm)"
MV="$(command -v mv)"

# First check OS.
OS="$(uname)"
if [[ "${OS}" == "Linux" ]]; then
  ADDAX_ON_LINUX=1
elif [[ "${OS}" != "Darwin" ]]; then
  abort "Addax is only supported on macOS and Linux."
fi

# Required installation paths. To install elsewhere (which is unsupported)
if [[ -z "${ADDAX_ON_LINUX-}" ]]; then
  UNAME_MACHINE="$(uname -m)"

  if [[ "${UNAME_MACHINE}" == "arm64" ]]; then
    # On ARM macOS, this script installs to /opt/addax only
    ADDAX_PREFIX="/opt/addax"
    ADDAX_REPOSITORY="${ADDAX_PREFIX}"
  else
    # On Intel macOS, this script installs to /usr/local only
    ADDAX_PREFIX="/usr/local"
    ADDAX_REPOSITORY="${ADDAX_PREFIX}/addax"
  fi
else
  # On Linux, it installs to /opt/addax, you SHOULD have sudo access or you're root
  ADDAX_PREFIX="/opt/addax"
  ADDAX_REPOSITORY="${ADDAX_PREFIX}"
fi

have_sudo_access() {
  if [[ ! -x "/usr/bin/sudo" ]]; then
    HAVE_SUDO_ACCESS=1
    return 1
  fi

  local -a sudo_args=()
  if [[ -n "${SUDO_ASKPASS-}" ]]; then
    sudo_args+=("-A")
  elif [[ -n "${NONINTERACTIVE-}" ]]; then
    sudo_args+=("-n")
  fi

  if [[ -z "${HAVE_SUDO_ACCESS-}" ]]; then
    if [[ -n "${NONINTERACTIVE-}" ]]; then
      # Why: On macOS system bash (3.2), expanding an empty array with set -u can raise
      # "unbound variable", so we must branch before using "${sudo_args[@]}".
      if [[ "${#sudo_args[@]}" -gt 0 ]]; then
        if /usr/bin/sudo "${sudo_args[@]}" -l mkdir >/dev/null 2>&1; then
          HAVE_SUDO_ACCESS=0
        else
          HAVE_SUDO_ACCESS=$?
        fi
      else
        if /usr/bin/sudo -l mkdir >/dev/null 2>&1; then
          HAVE_SUDO_ACCESS=0
        else
          HAVE_SUDO_ACCESS=$?
        fi
      fi
    else
      if [[ "${#sudo_args[@]}" -gt 0 ]]; then
        if /usr/bin/sudo "${sudo_args[@]}" -v >/dev/null 2>&1 && /usr/bin/sudo "${sudo_args[@]}" -l mkdir >/dev/null 2>&1; then
          HAVE_SUDO_ACCESS=0
        else
          HAVE_SUDO_ACCESS=$?
        fi
      else
        if /usr/bin/sudo -v >/dev/null 2>&1 && /usr/bin/sudo -l mkdir >/dev/null 2>&1; then
          HAVE_SUDO_ACCESS=0
        else
          HAVE_SUDO_ACCESS=$?
        fi
      fi
    fi
  fi

  if [[ -z "${ADDAX_ON_LINUX-}" ]] && [[ "${HAVE_SUDO_ACCESS}" -ne 0 ]]; then
    abort "Need sudo access on macOS (e.g. the user ${CURRENT_USER} needs to be an Administrator)!"
  fi

  return "${HAVE_SUDO_ACCESS}"
}

# get the special repo latest version
# param: $1 - repo name, e.g wgzhao/Addax
# returns: the version number: e.g 4.0.6
get_latest_release() {
  local repo="$1"
  local latest_url

  latest_url="$(${CURL} --silent --show-error --location --fail --output /dev/null --write-out '%{url_effective}' "https://github.com/${repo}/releases/latest")" || return 1

  if [[ "${latest_url}" != *"/releases/tag/"* ]]; then
    return 1
  fi

  printf "%s\n" "${latest_url##*/}"
}

shell_join() {
  local arg
  printf "%s" "$1"
  shift
  for arg in "$@"; do
    printf " "
    printf "%s" "${arg// /\ }"
  done
}

execute() {
  if ! "$@"; then
    abort "$(printf "Failed during: %s" "$(shell_join "$@")")"
  fi
}

execute_sudo() {
  local -a args=("$@")
  if have_sudo_access; then
    local -a sudo_args=()
    if [[ -n "${SUDO_ASKPASS-}" ]]; then
      sudo_args+=("-A")
    fi

    # Why: Keep compatibility with bash 3.2 + set -u where empty-array expansion
    # may fail, especially on default macOS environments.
    if [[ "${#sudo_args[@]}" -gt 0 ]]; then
      echo "/usr/bin/sudo" "${sudo_args[@]}" "${args[@]}"
      execute "/usr/bin/sudo" "${sudo_args[@]}" "${args[@]}"
    else
      echo "/usr/bin/sudo" "${args[@]}"
      execute "/usr/bin/sudo" "${args[@]}"
    fi
  else
    execute "${args[@]}"
  fi
}

safe_remove_repository() {
  local target="$1"

  if [[ "${target}" != "/opt/addax" && "${target}" != "/usr/local/addax" ]]; then
    abort "Refusing to delete unexpected path: ${target}"
  fi

  execute_sudo "${RM}" "-rf" "${target}"
}

get_java_major_version_from_bin() {
  local java_bin="$1"
  local version_line=""
  local version_string=""
  local major=""

  if [[ ! -x "${java_bin}" ]]; then
    return 1
  fi

  version_line="$("${java_bin}" -version 2>&1 | head -n 1)" || return 1
  version_string="$(printf "%s\n" "${version_line}" | sed -E 's/.*"([^"]+)".*/\1/')"

  if [[ -z "${version_string}" ]]; then
    return 1
  fi

  if [[ "${version_string}" == 1.* ]]; then
    major="${version_string#1.}"
    major="${major%%.*}"
  else
    major="${version_string%%.*}"
  fi

  printf "%s\n" "${major}"
}

find_java17_home() {
  local candidate=""
  local major=""
  local alternatives=""

  if [[ -n "${JAVA_HOME-}" ]] && [[ -x "${JAVA_HOME}/bin/java" ]]; then
    major="$(get_java_major_version_from_bin "${JAVA_HOME}/bin/java" || true)"
    if [[ "${major}" == "17" ]]; then
      printf "%s\n" "${JAVA_HOME}"
      return 0
    fi
  fi

  if [[ -z "${ADDAX_ON_LINUX-}" ]] && [[ -x "/usr/libexec/java_home" ]]; then
    candidate="$(/usr/libexec/java_home -v 17 2>/dev/null || true)"
    if [[ -n "${candidate}" ]] && [[ -x "${candidate}/bin/java" ]]; then
      printf "%s\n" "${candidate}"
      return 0
    fi
  fi

  if [[ -z "${ADDAX_ON_LINUX-}" ]] && command -v brew >/dev/null 2>&1; then
    candidate="$(brew --prefix openjdk@17 2>/dev/null || true)/libexec/openjdk.jdk/Contents/Home"
    if [[ -n "${candidate}" ]] && [[ -x "${candidate}/bin/java" ]]; then
      printf "%s\n" "${candidate}"
      return 0
    fi
  fi

  if [[ -n "${ADDAX_ON_LINUX-}" ]] && command -v update-alternatives >/dev/null 2>&1; then
    alternatives="$(update-alternatives --list java 2>/dev/null || true)"
    if [[ -n "${alternatives}" ]]; then
      while IFS= read -r candidate; do
        [[ -x "${candidate}" ]] || continue
        major="$(get_java_major_version_from_bin "${candidate}" || true)"
        if [[ "${major}" == "17" ]]; then
          printf "%s\n" "${candidate%/bin/java}"
          return 0
        fi
      done <<< "${alternatives}"
    fi
  fi

  if command -v java >/dev/null 2>&1; then
    candidate="$(command -v java)"
    major="$(get_java_major_version_from_bin "${candidate}" || true)"
    if [[ "${major}" == "17" ]]; then
      if [[ -n "${ADDAX_ON_LINUX-}" ]]; then
        candidate="$(readlink -f "${candidate}" 2>/dev/null || true)"
        if [[ -n "${candidate}" ]] && [[ "${candidate}" == */bin/java ]]; then
          printf "%s\n" "${candidate%/bin/java}"
          return 0
        fi
      fi
    fi
  fi

  if [[ -n "${ADDAX_ON_LINUX-}" ]]; then
    for candidate in /usr/lib/jvm/*17*/bin/java /usr/java/*17*/bin/java /opt/java/*17*/bin/java; do
      [[ -x "${candidate}" ]] || continue
      major="$(get_java_major_version_from_bin "${candidate}" || true)"
      if [[ "${major}" == "17" ]]; then
        printf "%s\n" "${candidate%/bin/java}"
        return 0
      fi
    done
  fi

  return 1
}

install_jdk17_runtime() {
  if [[ -z "${ADDAX_ON_LINUX-}" ]]; then
    if ! command -v brew >/dev/null 2>&1; then
      abort "Homebrew is required for automatic JDK 17 installation on macOS. Please install Homebrew first or install JDK 17 manually."
    fi

    execute "brew" "install" "openjdk@17"
    return 0
  fi

  if command -v apt-get >/dev/null 2>&1; then
    execute_sudo "apt-get" "update"
    execute_sudo "apt-get" "install" "-y" "openjdk-17-jdk"
  elif command -v dnf >/dev/null 2>&1; then
    execute_sudo "dnf" "install" "-y" "java-17-openjdk-devel"
  elif command -v yum >/dev/null 2>&1; then
    execute_sudo "yum" "install" "-y" "java-17-openjdk-devel"
  elif command -v zypper >/dev/null 2>&1; then
    execute_sudo "zypper" "--non-interactive" "install" "java-17-openjdk-devel"
  elif command -v pacman >/dev/null 2>&1; then
    execute_sudo "pacman" "-Sy" "--noconfirm" "jdk17-openjdk"
  elif command -v apk >/dev/null 2>&1; then
    execute_sudo "apk" "add" "openjdk17-jdk"
  else
    abort "Unsupported Linux distribution for automatic JDK 17 installation. Please install JDK 17 manually and rerun this script."
  fi
}

ensure_java17_runtime() {
  local detected_java_home=""
  local response=""
  local current_java_major=""

  detected_java_home="$(find_java17_home || true)"
  if [[ -n "${detected_java_home}" ]]; then
    ADDAX_JAVA_HOME="${detected_java_home}"
    return 0
  fi

  if [[ -n "${NONINTERACTIVE-}" ]]; then
    abort "JDK 17 runtime not found. In NONINTERACTIVE mode, please install JDK 17 manually before running this script."
  fi

  # Why: `curl ... | bash` feeds the script itself on stdin, so reading a prompt
  # from stdin would silently swallow the next line of the script instead.
  if [[ ! -r /dev/tty ]]; then
    abort "JDK 17 runtime not found and no terminal is available to ask for permission. Please install JDK 17 manually and rerun this script."
  fi

  echo "JDK 17 runtime was not detected on this system."
  if command -v java >/dev/null 2>&1; then
    current_java_major="$(get_java_major_version_from_bin "$(command -v java)" || true)"
    if [[ -n "${current_java_major}" ]]; then
      echo "Detected default java major version: ${current_java_major}"
    fi
  fi

  if ! read -r -p "Do you want this script to install JDK 17 now? [y/N] " response 2>/dev/null </dev/tty; then
    response="n"
  fi

  response="$(printf "%s" "${response}" | tr '[:upper:]' '[:lower:]')"
  if [[ "${response}" != "y" ]]; then
    abort "JDK 17 is required. Please install it manually and rerun this script."
  fi

  install_jdk17_runtime

  detected_java_home="$(find_java17_home || true)"
  if [[ -z "${detected_java_home}" ]]; then
    abort "JDK 17 installation finished, but no JDK 17 JAVA_HOME was detected. Please configure JDK 17 manually."
  fi

  ADDAX_JAVA_HOME="${detected_java_home}"
}

configure_addax_java_launcher() {
  local java_home="$1"
  local launcher_path="${ADDAX_REPOSITORY}/bin/addax.sh"
  local tmp_file=""
  local first_line=""
  local marker="# ADDAX_JDK17_RUNTIME"

  [[ -d "${java_home}" ]] || abort "Detected JDK 17 JAVA_HOME does not exist: ${java_home}"
  [[ -f "${launcher_path}" ]] || abort "Addax launcher not found: ${launcher_path}"

  if grep -q "${marker}" "${launcher_path}"; then
    return 0
  fi

  tmp_file="$(mktemp "${ADDAX_REPOSITORY}/.addax.sh.XXXXXX")" || abort "Failed to create temp file for launcher patching."
  first_line="$(head -n 1 "${launcher_path}")"

  if [[ "${first_line}" == "#!"* ]]; then
    {
      printf "%s\n" "${first_line}"
      printf "\n"
      printf "%s\n" "${marker}"
      # Why: Addax currently supports JDK 17 only, so we pin runtime to a known-good
      # JAVA_HOME to avoid failures when default java points to another major version.
      printf "%s\n" "if [ -d \"${java_home}\" ]; then"
      printf "%s\n" "    export JAVA_HOME=\"${java_home}\""
      printf "%s\n" "    export PATH=\"\${JAVA_HOME}/bin:\${PATH}\""
      printf "%s\n" "fi"
      tail -n +2 "${launcher_path}"
    } >"${tmp_file}"
  else
    {
      printf "%s\n" "${marker}"
      printf "%s\n" "if [ -d \"${java_home}\" ]; then"
      printf "%s\n" "    export JAVA_HOME=\"${java_home}\""
      printf "%s\n" "    export PATH=\"\${JAVA_HOME}/bin:\${PATH}\""
      printf "%s\n" "fi"
      cat "${launcher_path}"
    } >"${tmp_file}"
  fi

  execute "${MV}" "${tmp_file}" "${launcher_path}"
  execute "${CHMOD}" "755" "${launcher_path}"
}

cleanup_download_artifact() {
  if [[ -n "${pkg_file-}" ]] && [[ -f "${pkg_file}" ]]; then
    # Why: Keep install directory clean when download or extraction fails midway.
    "${RM}" "-f" "${pkg_file}" >/dev/null 2>&1 || true
  fi
}

if [[ -n "${ADDAX_ON_LINUX-}" ]] && [[ "$(id -u)" -ne 0 ]] && ! have_sudo_access; then
  abort "Need sudo access on Linux (or run as root) to install into ${ADDAX_PREFIX}."
fi

unset HAVE_SUDO_ACCESS

# has installed
if [[ -d "${ADDAX_REPOSITORY}" ]]; then
  echo "Addax is already installed to ${ADDAX_REPOSITORY}"

  if [[ -n "${NONINTERACTIVE-}" ]]; then
    abort "NONINTERACTIVE mode detected: refusing to reinstall automatically. Remove ${ADDAX_REPOSITORY} manually and retry."
  fi

  # Why: Same as the JDK prompt below — under `curl ... | bash` stdin is the
  # script itself, so the prompt must come from the terminal instead.
  if [[ ! -r /dev/tty ]]; then
    abort "No terminal is available to ask for confirmation. Remove ${ADDAX_REPOSITORY} manually and retry."
  fi

  echo "Do you want to reinstall? This script will clean up ${ADDAX_REPOSITORY}."
  response=""
  if ! read -r -p "Do you want to continue? [y/N] " response 2>/dev/null </dev/tty; then
    response="n"
  fi

  response="$(printf "%s" "${response}" | tr '[:upper:]' '[:lower:]')"
  if [[ "${response}" != "y" ]]; then
    abort "Reinstall declined. Nothing was changed."
  fi

  safe_remove_repository "${ADDAX_REPOSITORY}"
fi

# create install directory
if [[ ! -d "${ADDAX_PREFIX}" ]]; then
  execute_sudo "${MKDIR}" "-p" "${ADDAX_PREFIX}"
  if [[ -z "${ADDAX_ON_LINUX-}" ]]; then
    execute_sudo "${CHOWN}" "root:wheel" "${ADDAX_PREFIX}"
  else
    execute_sudo "${CHOWN}" "${CURRENT_USER}:${GROUP}" "${ADDAX_PREFIX}"
  fi
fi

if [[ ! -d "${ADDAX_REPOSITORY}" ]]; then
  execute_sudo "${MKDIR}" "-p" "${ADDAX_REPOSITORY}"
fi

execute_sudo "${CHOWN}" "-R" "${CURRENT_USER}:${GROUP}" "${ADDAX_REPOSITORY}"

ensure_java17_runtime
echo "Using JDK 17 runtime: ${ADDAX_JAVA_HOME}"

# try to download the latest version
version="$(get_latest_release "wgzhao/Addax")" || abort "Failed to detect the latest Addax release from GitHub."
normalized_version="${version#v}"
pkg_url="https://github.com/wgzhao/Addax/releases/download/${version}/addax-${normalized_version}.tar.gz"

echo "Downloading and installing Addax..."
cd "${ADDAX_REPOSITORY}" || abort "Failed to enter ${ADDAX_REPOSITORY}."

pkg_file="addax.tar.gz"
max_download_attempts=10
attempt=1
download_ok=0
trap cleanup_download_artifact EXIT

while [[ "${attempt}" -le "${max_download_attempts}" ]]; do
  echo "Downloading package (attempt ${attempt}/${max_download_attempts})..."
  # Why: Use resume mode to survive unstable networks and avoid re-downloading
  # from zero after interruption, which is common in constrained regions.
  if "${CURL}" --fail --location --show-error --continue-at - "${pkg_url}" -o "${pkg_file}"; then
    download_ok=1
    break
  fi

  attempt=$((attempt + 1))
  sleep 2
done

if [[ "${download_ok}" -ne 1 ]]; then
  # Why: A partially downloaded archive is invalid input and should not be kept
  # after all retries are exhausted.
  execute "${RM}" "-f" "${pkg_file}"
  abort "Failed to download package from ${pkg_url} after ${max_download_attempts} attempts."
fi

execute "${TAR}" "-xzf" "${pkg_file}" "--strip-components" "1"
execute "${RM}" "-f" "${pkg_file}"
configure_addax_java_launcher "${ADDAX_JAVA_HOME}"
execute_sudo "${CHOWN}" "-R" "${CURRENT_USER}:${GROUP}" "${ADDAX_REPOSITORY}"
trap - EXIT

cat <<EOS
  Addax has installed on ${ADDAX_REPOSITORY}
  JDK 17 runtime has been pinned to: ${ADDAX_JAVA_HOME}
  We recommend that you execute the following command to add addax execute path to PATH environment variable:
    export PATH=${ADDAX_REPOSITORY}/bin:\$PATH
EOS
