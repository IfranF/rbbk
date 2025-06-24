#!/bin/bash
printf "[global]\nindex-url=https://${AKV_PAT_TOKEN}@pkgs.dev.azure.com/raboweb/51cc9317-c550-4d26-91e6-b32dc246f6e2/_packaging/edp_dbr_artifacts_feed/pypi/simple/" >> /etc/pip.conf