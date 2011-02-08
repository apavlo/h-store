#!/usr/bin/python
# Exit with an error code if Valgrind finds "definitely lost" memory.

import subprocess
import sys
import tempfile

VALGRIND = "valgrind"


def valgrindVersion():
    # Figure out the version of valgrind since command line arguments vary
    valgrind = subprocess.Popen((VALGRIND, "--version"),
            stdout = subprocess.PIPE)
    valgrind_version = valgrind.stdout.read()
    status = valgrind.wait()
    assert status == 0
    return valgrind_version


def valleak(executable):
    """Returns (error, stdout, stderr).
    
    error == 0 if successful, an integer > 0 if there are memory leaks or errors."""

    valgrind_output = tempfile.NamedTemporaryFile()

    # Figure out the version of valgrind since command line arguments vary
    valgrind_version = valgrindVersion()
    log_argument = "--log-file"
    if valgrind_version.startswith("valgrind-3.2"):
        log_argument = "--log-file-exactly"

    valgrind_command = (
            VALGRIND,
            "--leak-check=full",
            log_argument + "=" + valgrind_output.name,
            "--error-exitcode=1",
            executable)
            
    #~ print " ".join(valgrind_command)
    process = subprocess.Popen(
            valgrind_command,
            bufsize = -1,
            stdin = subprocess.PIPE,
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE,
            close_fds = True)

    process.stdin.close()
    stdout = process.stdout.read()
    stderr = process.stderr.read()
    error = process.wait()

    valgrind_error_file = open(valgrind_output.name)
    valgrind_error = valgrind_error_file.read()
    valgrind_error_file.close()
    valgrind_output.close()

    # Find the last summary block in the valgrind report
    # This ignores forks
    summary_start = valgrind_error.rindex("== ERROR SUMMARY:")
    summary = valgrind_error[summary_start:]

    append_valgrind = False
    if error == 0:
        assert "== ERROR SUMMARY: 0 errors" in summary
        # Check for memory leaks: we care about definitely and possibly lost reports
        # Still reachable is not interesting
        if "==    definitely lost:" in summary and (
                "==    definitely lost: 0" not in summary or
                "==      possibly lost: 0" not in summary):
            error = 1
            append_valgrind = True
    elif "== ERROR SUMMARY: 0 errors" not in summary:
        # We also have valgrind errors: append the log to stderr
        append_valgrind = True

    if append_valgrind:
        stderr = stderr + "\n\n" + valgrind_error

    return (error, stdout, stderr)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.stderr.write("valleak.py [executable]\n")
        sys.exit(1)

    exe = sys.argv[1]
    error, stdin, stderr = valleak(exe)
    sys.stdout.write(stdin)
    sys.stderr.write(stderr)
    sys.exit(error)
