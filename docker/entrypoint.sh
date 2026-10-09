#!/bin/sh
# Runtime PID 1 wrapper: materialize allowlisted *_FILE secrets, then exec workhold.
# POSIX / dash only. Never enable xtrace. Errors print names only, never secret values.
set -eu

file_env() {
	var=$1
	file_var=${var}_FILE
	eval "var_set=\${$var-}"
	eval "file_set=\${$file_var-}"
	if [ -n "$var_set" ] && [ -n "$file_set" ]; then
		printf >&2 'error: both %s and %s are set (but are exclusive)\n' "$var" "$file_var"
		exit 1
	fi
	if [ -z "$file_set" ]; then
		return 0
	fi
	case $file_set in
	/*) ;;
	*)
		printf >&2 'error: %s must be an absolute path\n' "$file_var"
		exit 1
		;;
	esac
	if [ ! -f "$file_set" ] || [ ! -r "$file_set" ]; then
		printf >&2 'error: %s does not point to a readable regular file\n' "$file_var"
		exit 1
	fi
	# Command substitution strips trailing LF; then strip one trailing CR (CRLF).
	val=$(cat "$file_set")
	case $val in
	*"$(printf '\r')") val=${val%"$(printf '\r')"} ;;
	esac
	if [ -z "$val" ]; then
		printf >&2 'error: %s is empty\n' "$file_var"
		exit 1
	fi
	export "$var=$val"
	unset "$file_var"
}

materialize_secrets() {
	file_env DATABASE_URL
	file_env QUEUE_API_BEARER_TOKEN
	file_env QUEUE_API_BEARER_TOKEN_PREVIOUS
	file_env QUEUE_API_PRINCIPALS_MANIFEST
	file_env SENTRY_DSN
}

# Executed as image entrypoint: materialize then replace PID 1 with workhold.
# Sourced: define helpers only (no exec) for optional in-image checks later.
case $0 in
*entrypoint.sh)
	materialize_secrets
	exec workhold "$@"
	;;
esac
