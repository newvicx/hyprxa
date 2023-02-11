import logging
import subprocess
from typing import Any



def log_subprocess(
    logger: logging.Logger,
    *popenargs: Any,
    input=None,
    timeout=None,
    check=False,
    **kwargs
) -> None:
    """Run a subprocess and route the `stdout` and `stderr` to the logger.
    
    Stdout is debug information and stderr is warning.

    Raises:
        CalledProcessError: If `check` is `True` and process exited with non-zero
            exit code. Stdout and Stderr will be captured in the exc info of the
            log. Logged as warning.
    """
    try:
        result = subprocess.run(
            *popenargs,
            input=input,
            capture_output=True,
            timeout=timeout,
            check=check,
            **kwargs
        )
    except subprocess.CalledProcessError:
        logger.warning("Process returned non zero exit code", exc_info=True)
        raise
    else:
        stdout, stderr = result.stdout, result.stderr
        if stdout:
            for line in stdout.splitlines(False):
                line = line.decode() if isinstance(line, bytes) else line
                logger.debug(line)
        if stderr:
            for line in stderr.splitlines(False):
                line = line.decode() if isinstance(line, bytes) else line
                logger.warning(line)