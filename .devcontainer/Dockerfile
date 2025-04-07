# syntax=docker/dockerfile:1.5

ARG BASE
FROM $BASE

ARG LOCAL_WORKSPACE_FOLDER
ARG LOCAL_WORKSPACE_FOLDER_BASE_NAME

RUN <<EOF
if grep ubuntu:x:1000:1000 /etc/passwd >/dev/null; then userdel -f -r ubuntu; fi

mkdir -p /opt "${LOCAL_WORKSPACE_FOLDER}"
ln -s "${LOCAL_WORKSPACE_FOLDER}" /opt/sccache
EOF
