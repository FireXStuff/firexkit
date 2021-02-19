# ug=rwx
# This ensures the last three bits are cleared, but, doesn't set any bits.
DEFAULT_UMASK = 0o0007

# ug+rwx
DEFAULT_CHMOD_MODE = 0o770

# ug+rw
CHMOD_MODE_NONEXEC = 0o660
