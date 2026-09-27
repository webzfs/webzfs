#!/usr/bin/env bash
set -euo pipefail

RED='\033[1;31m'
RESET='\033[0m'

printf >&2 "${RED}"
printf >&2 '%s\n' \
    '################################################################################' \
    '#                                                                              #' \
    '#                              WARNING                                         #' \
    '#                                                                              #' \
    '#                      DEVELOPMENT TESTING ONLY                                #' \
    '#                                                                              #' \
    '#  DO NOT RUN THIS SCRIPT ON A PRODUCTION SYSTEM.                              #' \
    '#                                                                              #' \
    '#  This changes WebZFS from localhost-only access to listening on every        #' \
    '#  network interface. This can expose WebZFS to other systems on the network.  #' \
    '#                                                                              #' \
    '#  Press Ctrl-C now to cancel.                                                 #' \
    '#                                                                              #' \
    '################################################################################'

for seconds in {10..1}; do
    printf >&2 '\rContinuing in %2d seconds. Press Ctrl-C to cancel. ' "${seconds}"
    sleep 1
done

printf >&2 "\rContinuing with development bind change.                    \n${RESET}"

sudo sed -i 's/^BIND_IP=127\.0\.0\.1$/BIND_IP=0.0.0.0/' /opt/webzfs/.env