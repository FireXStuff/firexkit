# ug=rwx,o=
# This ensures the last three bits are cleared, but, doesn't set any bits.
DEFAULT_UMASK = 0o0007
DEFAULT_CHMOD_MODE = 0o770

# ug+rw
NONEXEC_CHMOD_MODE = 0o660

#u=rwx,g=rx,o=
NO_GROUP_WRITE_UMASK = 0o0027
NO_GROUP_WRITE_CHMOD_MODE = 0o750
