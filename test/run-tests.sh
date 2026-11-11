#!/usr/bin/env bash
set -eo pipefail

go_command=$1
shift
chunkstore=github.com/go-graphite/go-carbon/internal/chunkstore
whisperio=github.com/go-graphite/go-carbon/internal/whisperio
whisper=github.com/go-graphite/go-whisper

# Preserve explicitly selected tests and other go commands such as vet.
if [[ $1 != test ]]; then
    exec "$go_command" "$@" ./... "$whisper"
fi
for arg in "$@"; do
    case "$arg" in
        -run*|-bench*|-fuzz*|-list*|-coverprofile*)
            exec "$go_command" "$@" ./... "$whisper"
            ;;
    esac
done
shift

package_list=$("$go_command" list ./...)
packages=()
while IFS= read -r package; do
    if [[ $package != "$whisperio" && $package != "$chunkstore" ]]; then
        packages+=("$package")
    fi
done <<< "$package_list"
# Keep the mandatory >1M-point native-copy oracles in CI. Only their exhaustive
# 20M-point scale variants use -short; other packages keep their normal coverage.
# An explicit -short=false overrides this default, as do selected-test commands.
"$go_command" test -short "$@" "$chunkstore"
"$go_command" test "$@" -p 2 "${packages[@]}" "$whisper"

# Each invocation owns a bounded part of the matrix and its timeout. Do not
# let completed cases consume the remaining cases' package-wide time budget.
list=$(mktemp)
trap 'rm -f "$list"' EXIT
list_flags=()
for arg in "$@"; do
    case "$arg" in
        -json*) ;;
        *) list_flags+=("$arg") ;;
    esac
done
"$go_command" test "${list_flags[@]}" -list '^(Test|Example|Fuzz)' "$whisperio" > "$list"
patterns=()
while IFS= read -r name; do
    case "$name" in
        TestRandomInterleavingsKeepBlocksEncodable*)
            # Include every seed and schema while bounding the format cases.
            for buckets in false true; do
                for converted in false true; do
                    patterns+=("^${name}$/././^v7=${buckets}$/^converted=${converted}$")
                done
            done
            ;;
        TestRandomInterleavingsThroughStore)
            for buckets in false true; do
                for cache in false true; do
                    patterns+=("^${name}$/./^v7=${buckets}$/^cache=${cache}$")
                done
            done
            ;;
        TestContinuationTailReadReuseParity)
            # Bound each aggregation/x-files-factor case independently. These
            # prefixes retain every current and future numeric method and factor.
            for prefix in '[0-1]' '2' '3' '4' '5' '[^0-5]'; do
                for xff in '0$' '0[.]' '[^0]'; do
                    patterns+=("^${name}$/^${prefix}/^${xff}")
                done
            done
            ;;
        TestContinuationStoreMatchesCWhisperAfterImport)
            # Complementary prefixes include new configurations automatically.
            for prefix in '[A-Ga-g0-2_]' '[H-Nh-n3-5]' '[O-Uo-u6-8]' '[^A-Ua-u0-8_]'; do
                patterns+=("^${name}$/^${prefix}")
            done
            ;;
        Test*|Example*|Fuzz*) patterns+=("^${name}$") ;;
    esac
done < "$list"
printf 'whisperio: %d bounded test groups, 2 workers\n' "${#patterns[@]}" >&2
printf '%s\n' "${patterns[@]}" | xargs -P 2 -I '{}' \
    "$go_command" test "$@" -timeout 150s -run '{}' "$whisperio"
