#!/bin/sh

# This script contains CI tests for FreeBSD, testing
#
# - cargo build & cargo test
# - configure and start sccache-dist and scheduler
# - test distributed compile
# - test that the cache is used
#
# It creates a temporary test pool backed by a
# file (using mdconfig) and does a full configuration
# of pot.
#
# After running it copies the sccache log file into
# the repo's root directory. It also does a full
# cleanup (removal of all temporary files, test pool
# etc.) after each run. This can be prevented by
# setting FREEBSD_CI_NOCLEAN in the environment:
#
#     FREEBSD_CI_NOCLEAN=1 scripts/freebsd-ci-test.sh
#
# When running in a loop, time and bandwidth can be
# saved by placing FreeBSD distribution files in
# $HOME/.potcache
#
#     mkdir $HOME/.potcache
#     fetch -o $HOME/.potcache/14.1-RELEASE_base.txz \
#     https://ftp.freebsd.org/pub/FreeBSD/releases/amd64/14.1-RELEASE/base.txz
#
# This script can be run from a github action. When run locally, make
# sure to install the required packages:
#
#     pkg install -y ca_root_nss curl gmake gtar pot sudo rabbitmq
#

# shellcheck disable=SC3040
set -eo pipefail

init()
{
	base=$(realpath "$(dirname "$0")"/..)
	OS_VERSION="$(freebsd-version | awk -F- '{print $1}')"
	PUB_INTF="$(netstat -4rn | grep default | awk '{ print $4}')"
	TEST_TMPDIR=$(mktemp -d "/tmp/sccache_freebsd.XXXXXXX") || exit 1
	chmod g+r "$TEST_TMPDIR"
	export XDG_CONFIG_HOME="$TEST_TMPDIR/.config"
	mkdir -p "$XDG_CONFIG_HOME"
	export SCCACHE_DIR="$TEST_TMPDIR/.cache"
	export SCCACHE_DIST_JOBS_DIR="$TEST_TMPDIR/.cache/jobs"
	export SCCACHE_DIST_TOOLCHAINS_DIR="$TEST_TMPDIR/.cache/toolchains"
	killall sccache 2>/dev/null || true
	killall sccache-dist 2>/dev/null || true
	export RUST_LOG_STYLE=never
}

output_env_info()
{
	echo "## user"
	whoami
	echo "## environment"
	env | sort
	echo "## network"
	ifconfig
	echo "## tooling info"
	cargo -V
	rustc -V
	curl --version
	# See https://github.com/bsdpot/pot/pull/253
	pot version || true
	gtar --version
	echo "## installed packages"
	pkg info
}

build_and_test_sccache()
{
	echo "#### building sccache (cargo)"
	cd "$base"
	FAULT=0
	export RUSTFLAGS="-C debuginfo=0"
	cargo build \
		--features "dist-client,dist-server" \
		--target-dir "$TEST_TMPDIR"/target \
	|| FAULT=1
	echo "#### testing sccache (cargo)"
	rm -rf /tmp/sccache_local_daemon.txt
	cargo test \
		--features "dist-client,dist-server" \
		--target-dir "$TEST_TMPDIR"/target \
		-- \
		--test-threads 1 || FAULT=1
	unset RUSTFLAGS
	if [ "$FAULT" -eq 0 ]; then
		# save build time by avoiding "cargo install"
		cp -a "$TEST_TMPDIR"/target/debug/sccache "$TEST_TMPDIR"/target/debug/sccache-dist \
		  "$HOME/.cargo/bin/."
	fi
	if [ $FAULT -ne 0 ]; then return 1; fi
}

prepare_sccache_dist()
{
	echo "#### preparing sccache-dist"
	CLIENT_TOKEN="client_token"
	# create scheduler.conf
	cat >"$TEST_TMPDIR"/scheduler.conf <<-EOF
	public_addr = "127.0.0.1:10600"
	[client_auth]
	type = "token"
	token = "$CLIENT_TOKEN"
	EOF

	# Create server.conf
	cat >"$TEST_TMPDIR"/server.conf <<-EOF
	cache_dir = "$TEST_TMPDIR/toolchains"
	[builder]
	type = "pot"
	pot_fs_root = "$TEST_TMPDIR/pot"
	EOF

	# create sccache client config
	TC="$(rustup toolchain list | grep default | awk '{ print $1 }')"
	RUSTC_PATH="$HOME/.rustup/toolchains/$TC/bin/rustc"
	mkdir -p "$XDG_CONFIG_HOME/sccache"
	cat >"$XDG_CONFIG_HOME/sccache/config" <<-EOF
	[dist]
	scheduler_url = "http://127.0.0.1:10600"
	toolchain_cache_size = 5368709120
	cache_dir = "$HOME/.cache/sccache-dist-client"
	[dist.auth]
	type = "token"
	token = "$CLIENT_TOKEN"
	[[dist.toolchains]]
	type = "path_override"
	compiler_executable = "/usr/bin/cc"
	archive = "$TEST_TMPDIR/empty.tar.gz"
	archive_compiler_executable = "/usr/bin/cc"
	[[dist.toolchains]]
	type = "path_override"
	compiler_executable = "$RUSTC_PATH"
	archive = "$TEST_TMPDIR/rust-toolchain.tgz"
	archive_compiler_executable = "$RUSTC_PATH"
	EOF

	echo "Creating toolchain tarballs"
	gtar cvf - --files-from /dev/null | \
	  gzip -n >"$TEST_TMPDIR/empty.tar.gz"
	gtar cf - --sort=name --mtime='2022-06-28 17:35Z' "$HOME/.rustup"  | \
	  gzip -n >"$TEST_TMPDIR/rust-toolchain.tgz"

	echo "Starting rabbitmq"
	sudo service rabbitmq onestart
	sleep 5
	echo "Checking rabbitmq status"
	sudo service rabbitmq onestatus
}

prepare_zpool()
{
	echo "#### preparing zpool"
	sudo dd if=/dev/zero of="$TEST_TMPDIR/zfs1" bs=1 count=1 seek=3G
	MDUNIT=$(sudo mdconfig -a -n -t vnode -S 4096 -f "$TEST_TMPDIR/zfs1")
	zdev="/dev/md$MDUNIT"
	sudo zpool create -f potpool "$zdev"
}

prepare_pot()
{
	echo "#### preparing pot"
	sudo sysrc -f /usr/local/etc/pot/pot.conf POT_ZFS_ROOT=potpool/pot
	sudo sysrc -f /usr/local/etc/pot/pot.conf POT_EXTIF="$PUB_INTF"
	sudo sysrc -f /usr/local/etc/pot/pot.conf POT_TMP="$TEST_TMPDIR"
	sudo sysrc -f /usr/local/etc/pot/pot.conf \
	  POT_FS_ROOT="$TEST_TMPDIR/pot"
	sudo sysrc -f /usr/local/etc/pot/pot.conf POT_GROUP=wheel
	sudo pot init -f ""
	sudo pot version
	sudo cp "$HOME"/.potcache/*.txz /var/cache/pot 2>/dev/null || true
	sudo pot create -p sccache-template -N alias -i "lo0|127.0.0.2" \
	  -t single -b "$OS_VERSION"
	sudo pot set-cmd -p sccache-template -c /usr/bin/true
	sudo pot set-attr -p sccache-template -A no-rc-script -V YES
	sudo pot snapshot -p sccache-template
}

start_scheduler()
{
	echo "#### starting scheduler"
	# sudo so the scheduler can access the server job results
	sudo \
		AMQP_ADDR="amqp://127.0.0.1:5672//" \
		NO_COLOR=1 \
		SCCACHE_DIR="$SCCACHE_DIR" \
		SCCACHE_DIST_JOBS_DIR="$SCCACHE_DIST_JOBS_DIR" \
		SCCACHE_DIST_TOOLCHAINS_DIR="$SCCACHE_DIST_TOOLCHAINS_DIR" \
		SCCACHE_LOG="sccache=debug,tower_http=debug,axum::rejection=trace" \
		SCCACHE_NO_DAEMON=1 \
		"$HOME"/.cargo/bin/sccache-dist scheduler \
		--config "$TEST_TMPDIR"/scheduler.conf \
		1>"$TEST_TMPDIR"/sccache_scheduler_log.txt 2>&1 &
	sleep 1
}

start_build_server()
{
	echo "#### starting build-server (as root)"
	sudo \
		AMQP_ADDR="amqp://127.0.0.1:5672//" \
		NO_COLOR=1 \
		SCCACHE_DIR="$SCCACHE_DIR" \
		SCCACHE_DIST_JOBS_DIR="$SCCACHE_DIST_JOBS_DIR" \
		SCCACHE_DIST_SERVER_ID="build-server" \
		SCCACHE_DIST_TOOLCHAINS_DIR="$SCCACHE_DIST_TOOLCHAINS_DIR" \
		SCCACHE_LOG="sccache=debug" \
		SCCACHE_NO_DAEMON=1 \
		"$HOME"/.cargo/bin/sccache-dist server \
			--config "$TEST_TMPDIR"/server.conf \
			1>"$TEST_TMPDIR"/sccache_server_log.txt 2>&1 &
	sleep 1
}

create_build_test_project()
{
	echo "#### create and build test project"
	cd "$TEST_TMPDIR"
	cargo init buildtest
	cd buildtest
	echo 'chrono = "0.4"' >>Cargo.toml
}

start_sccache_server()
{
	echo "#### starting sccache-server"
	killall sccache 2>/dev/null || true
	SCCACHE_LOG="sccache=debug" \
	SCCACHE_ERROR_LOG="$TEST_TMPDIR"/sccache_client_log.txt \
		sccache --start-server \
		1>"$TEST_TMPDIR"/sccache_client_log.txt 2>&1
	sleep 5
}

test_sccache_dist_01()
{
	echo "#### running scache_dist test 01"
	cd "$TEST_TMPDIR/buildtest"
	cargo clean --target-dir /tmp/buildtest
	RUSTC_WRAPPER=sccache cargo build --target-dir /tmp/buildtest
	STATS="$(sccache -s)"
	echo "Statistics of first buildtest"
	echo "$STATS"
	CACHE_HITS="$(echo "$STATS" | \
	  grep "Cache hits" | grep -vE '(rate|Rust)' | awk '{ print $3 }')"
	FAILED_DIST="$(echo "$STATS" | \
	  grep "Failed distributed compilations" | awk '{ print $4 }')"
	SUCCEEDED_DIST="$(echo "$STATS" | \
	  (grep -F "build-server" || echo 0 0) | awk '{ print $2 }')"

	if [ "$CACHE_HITS" -ne 0 ]; then
		2>&1 echo "Unexpected cache hits"
		return 1
	fi
	# We sometimes get "connection closed before message completed"
	# on the first remote build (which will make sccache fall-back
	# to building locally). Until this has been resolved, accept
	# one failed remote build.
	if [ "$FAILED_DIST" -gt 1 ]; then
		2>&1 echo "More than one distributed compilation failed"
		cat "$TEST_TMPDIR"/sccache_client_log.txt
		cat "$TEST_TMPDIR"/sccache_scheduler_log.txt
		cat "$TEST_TMPDIR"/sccache_server_log.txt
		return 1
	fi
	if [ "$SUCCEEDED_DIST" -eq 0 ]; then
		2>&1 echo "No distributed compilations succeeded"
		cat "$TEST_TMPDIR"/sccache_client_log.txt
		cat "$TEST_TMPDIR"/sccache_scheduler_log.txt
		cat "$TEST_TMPDIR"/sccache_server_log.txt
		return 1
	fi
}

test_sccache_dist_02()
{
	echo "#### running scache_dist test 02"
	cd "$TEST_TMPDIR/buildtest"
	sccache -z
	cargo clean --target-dir /tmp/buildtest
	RUSTC_WRAPPER=sccache cargo build --target-dir /tmp/buildtest
	STATS="$(sccache -s)"
	echo "Statistics of second buildtest"
	echo "$STATS"
	CACHE_HITS="$(echo "$STATS" | \
	  grep "Cache hits" | grep -vE '(rate|Rust)' | awk '{ print $3 }')"
	FAILED_DIST="$(echo "$STATS" | \
	  grep "Failed distributed compilations" | awk '{ print $4 }')"
	SUCCEEDED_DIST="$(echo "$STATS" | \
	  (grep -F "build-server" || echo 0 0) | awk '{ print $2 }')"

	if [ "$CACHE_HITS" -eq 0 ]; then
		2>&1 echo "No cache hits when there should be some"
		return 1
	fi
	# We sometimes get "connection closed before message completed"
	# on the first remote build (which will make sccache fall-back
	# to building locally). Until this has been resolved, accept
	# one failed remote build.
	if [ "$FAILED_DIST" -gt 1 ]; then
		2>&1 echo "More than one distributed compilation failed"
		cat "$TEST_TMPDIR"/sccache_client_log.txt
		cat "$TEST_TMPDIR"/sccache_scheduler_log.txt
		cat "$TEST_TMPDIR"/sccache_server_log.txt
		return 1
	fi
	if [ "$SUCCEEDED_DIST" -ne 0 ]; then
		2>&1 echo "Unexpected distributed compilations happened"
		cat "$TEST_TMPDIR"/sccache_client_log.txt
		cat "$TEST_TMPDIR"/sccache_scheduler_log.txt
		cat "$TEST_TMPDIR"/sccache_server_log.txt
		return 1
	fi
}

cleanup()
{
	echo "#### cleaning up"
	set +e
	sccache --stop-server
	killall sccache
	killall sccache-dist && sleep 3
	sudo killall sccache-dist && sleep 3
	sudo killall -9 sccache-dist
	killall sccache
	echo "Stopping rabbitmq"
	sudo service rabbitmq onestop
	cp "$TEST_TMPDIR"/sccache_client_log.txt "$base/sccache_client_log_$(date +%s).txt"
	cp "$TEST_TMPDIR"/sccache_server_log.txt "$base/sccache_server_log_$(date +%s).txt"
	cp "$TEST_TMPDIR"/sccache_scheduler_log.txt "$base/sccache_scheduler_log_$(date +%s).txt"
	if [ -f /tmp/sccache_local_daemon.txt ]; then
		cp /tmp/sccache_local_daemon.txt "$base/sccache_local_daemon_$(date +%s).txt"
	fi
	if [ -z "${FREEBSD_CI_NOCLEAN:-}" ]; then
		for name in $(sudo pot ls -q); do
			sudo pot stop -p "$name" || true
		done
		sudo pot de-init
		sudo zpool destroy -f potpool
		if [ -n "$MDUNIT" ]; then
			sudo mdconfig -d -u "$MDUNIT"
		fi
		sudo rm -rf "$TEST_TMPDIR"
	fi
	set -e
}

install_signal_handler()
{
	trap 'remove_signal_handler; cleanup; exit' EXIT INT HUP
}

remove_signal_handler()
{
	trap - EXIT INT HUP
}

main()
{
	install_signal_handler
	init
	output_env_info
	build_and_test_sccache
	prepare_sccache_dist
	prepare_zpool
	prepare_pot
	start_scheduler
	start_build_server
	create_build_test_project
	start_sccache_server
	test_sccache_dist_01
	test_sccache_dist_02
	remove_signal_handler
	cleanup
}

# run main function
main
