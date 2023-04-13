#!/bin/bash
set -u

abort() {
  printf "%s\n" "$@"
  exit 1
}

if [ -z "${BASH_VERSION:-}" ]; then
  abort "Bash is required to interpret this script."
fi

# First check OS.
OS="$(uname)"
if [[ "${OS}" == "Linux" ]]; then
  ADDAX_ON_LINUX=1
elif [[ "${OS}" != "Darwin" ]]; then
  abort "Addax is only supported on Windows and Linux."
fi

# Required installation paths. To install elsewhere (which is unsupported)
if [[ -z "${ADDAX_ON_LINUX-}" ]]; then
  UNAME_MACHINE="$(/usr/bin/uname -m)"

  if [[ "${UNAME_MACHINE}" == "arm64" ]]; then
    # On ARM macOS, this script installs to /opt/addax only
    ADDAX_PREFIX="/opt/addax"
    ADDAX_REPOSITORY="${ADDAX_PREFIX}"
  else
    # On Intel macOS, this script installs to /usr/local only
    ADDAX_PREFIX="/usr/local"
    ADDAX_REPOSITORY="${ADDAX_PREFIX}/addax"
  fi

  STAT_FLAG="-f"
  PERMISSION_FORMAT="%A"
  CHOWN="/usr/sbin/chown"
  CHGRP="/usr/bin/chgrp"
  GROUP="admin"
  TOUCH="/usr/bin/touch"
else
  UNAME_MACHINE="$(uname -m)"

  # On Linux, it installs to /opt/addax, you SHOULD have sudo access or you're root
  ADDAX_PREFIX="/opt/addax"
  ADDAX_REPOSITORY="${ADDAX_PREFIX}"
  STAT_FLAG="--printf"
  PERMISSION_FORMAT="%a"
  CHOWN="/bin/chown"
  CHGRP="/bin/chgrp"
  GROUP="$(id -gn)"
  TOUCH="/bin/touch"
fi

have_sudo_access() {
  if [[ ! -x "/usr/bin/sudo" ]]; then
    return 1
  fi

  local -a args
  if [[ -n "${SUDO_ASKPASS-}" ]]; then
    args=("-A")
  elif [[ -n "${NONINTERACTIVE-}" ]]; then
    args=("-n")
  fi

  if [[ -z "${HAVE_SUDO_ACCESS-}" ]]; then
    if [[ -n "${args[*]-}" ]]; then
      SUDO="/usr/bin/sudo ${args[*]}"
    else
      SUDO="/usr/bin/sudo"
    fi
    if [[ -n "${NONINTERACTIVE-}" ]]; then
      # Don't add quotes around ${SUDO} here
      ${SUDO} -l mkdir &>/dev/null
    else
      ${SUDO} -v && ${SUDO} -l mkdir &>/dev/null
    fi
    HAVE_SUDO_ACCESS="$?"
  fi

  if [[ -z "${ADDAX_ON_LINUX-}" ]] && [[ "${HAVE_SUDO_ACCESS}" -ne 0 ]]; then
    abort "Need sudo access on macOS (e.g. the user ${USER} needs to be an Administrator)!"
  fi

  return "${HAVE_SUDO_ACCESS}"
}

# get the special repo latest version
# param: $1 - repo name, e.g wgzhao/addax
# returns: the version number: e.g 4.0.6
get_latest_release() {
  curl --silent "https://api.github.com/repos/$1/releases/latest" | # Get latest release from GitHub api
    grep '"tag_name":' |                                            # Get tag line
    sed -E 's/.*"([^"]+)".*/\1/'                                    # Pluck JSON value
}

shell_join() {
  local arg
  printf "%s" "$1"
  shift
  for arg in "$@"
  do
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
    if [[ -n "${SUDO_ASKPASS-}" ]]; then
      args=("-A" "${args[@]}")
    fi
    echo "/usr/bin/sudo" "${args[@]}"
    execute "/usr/bin/sudo" "${args[@]}"
  else
    ohai "${args[@]}"
    execute "${args[@]}"
  fi
}

# has installed
if [ -d "${ADDAX_REPOSITORY}" ]; then
  echo "Addax is already installed  to ${ADDAX_REPOSITORY}"
  echo "Do you want to re-installed, if you want, the install script will cleanup the ${ADDAX_REPOSITORY}"
  read -r -p "Do you want to continue? [y/N] " response
  if response="$(echo "${response}" | tr '[:upper:]' '[:lower:]')"; then
    if [[ "${response}" != "y" ]]; then
      exit 1
    else
      execute_sudo "rm" "-rf" "${ADDAX_REPOSITORY}"
    fi
  else
    exit 0
  fi
fi

unset HAVE_SUDO_ACCESS

chmods=()
chowns=()
chgrps=()
group_chmods=()
user_chmods=()
mkdirs=()
# create install directory
if [[ -d "${ADDAX_PREFIX}" ]]; then
  if [[ "${#chmods[@]}" -gt 0 ]]; then
    execute_sudo "/bin/chmod" "u+rwx" "${chmods[@]}"
  fi
  if [[ "${#group_chmods[@]}" -gt 0 ]]; then
    execute_sudo "/bin/chmod" "g+rwx" "${group_chmods[@]}"
  fi
  if [[ "${#user_chmods[@]}" -gt 0 ]]; then
    execute_sudo "/bin/chmod" "g-w,o-w" "${user_chmods[@]}"
  fi
  if [[ "${#chowns[@]}" -gt 0 ]]; then
    execute_sudo "${CHOWN}" "${USER}" "${chowns[@]}"
  fi
  if [[ "${#chgrps[@]}" -gt 0 ]]; then
    execute_sudo "${CHGRP}" "${GROUP}" "${chgrps[@]}"
  fi
else
  execute_sudo "/bin/mkdir" "-p" "${ADDAX_PREFIX}"
  if [[ -z "${ADDAX_ON_LINUX-}" ]]; then
    execute_sudo "${CHOWN}" "root:wheel" "${ADDAX_PREFIX}"
  else
    execute_sudo "${CHOWN}" "${USER}:${GROUP}" "${ADDAX_PREFIX}"
  fi
fi

if [[ "${#mkdirs[@]}" -gt 0 ]]; then
  execute_sudo "/bin/mkdir" "-p" "${mkdirs[@]}"
  execute_sudo "/bin/chmod" "u=rwx,g=rwx" "${mkdirs[@]}"
  if [[ "${#mkdirs_user_only[@]}" -gt 0 ]]; then
    execute_sudo "/bin/chmod" "g-w,o-w" "${mkdirs_user_only[@]}"
  fi
  execute_sudo "${CHOWN}" "${USER}" "${mkdirs[@]}"
  execute_sudo "${CHGRP}" "${GROUP}" "${mkdirs[@]}"
fi

if ! [[ -d "${ADDAX_REPOSITORY}" ]]; then
  execute_sudo "/bin/mkdir" "-p" "${ADDAX_REPOSITORY}"
fi
execute_sudo "${CHOWN}" "-R" "${USER}:${GROUP}" "${ADDAX_REPOSITORY}"

# try to download the latest version
# tmpdir=$(mktemp -d -t addax-install.XXXXXX)
version=$(get_latest_release "wgzhao/addax")
pkg_url="https://github.com/wgzhao/Addax/releases/download/${version}/addax-${version}.tar.gz"

echo "Downloading and installing Addax..."
(
  cd "${ADDAX_REPOSITORY}" >/dev/null || return
  curl -L "${pkg_url}" -o addax.tar.gz
  if [ $? -ne 0 ]; then
    abort "Failed to download package from ${pkg_url}"
  fi
  tar -xzf addax.tar.gz
  mv addax-${version}/* .
  rm -r addax-${version}
  rm -f addax.tar.gz
  execute_sudo chown -R "${USER}:${GROUP}" .
)

cat <<EOS
  Addax has installed on ${ADDAX_REPOSITORY}
  We recommend that you execute the following command to add addax execute path to PATH environment variable:
    export PATH=${ADDAX_REPOSITORY}/bin:\$PATH
EOS
