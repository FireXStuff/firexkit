from firexkit.result import RETURN_KEYS_KEY
from inspect import Signature


class BagOfGoodies(object):
    # Special Char to denote indirect parameter references
    INDIRECT_ARG_CHAR = '@'

    def __init__(self, sig: Signature, args, kwargs):
        args = tuple(args)
        kwargs = dict(kwargs)
        self.sig = sig

        params = sig.parameters
        # Check if the method signature contains
        # any VAR_KEYWORD (i.e., **kwargs)
        self.varkeyword = False
        for param in params.values():
            if param.kind == param.VAR_KEYWORD:
                self.varkeyword = True

        add_later = {}
        try:
            # If the first positional argument is a
            # dict (i.e., result of a previous task), we need to process it.
            if isinstance(args[0], dict):
                original_args = args[0]
                # Remove the RETURN_KEYS_KEY entry
                if RETURN_KEYS_KEY in original_args:
                    del original_args[RETURN_KEYS_KEY]
                # Partially bind the remaining arguments
                ba = sig.bind_partial(*args[1:]).arguments
                for k, v in original_args.items():
                    # Add k,v pairs from the dictionary that are not in the
                    # partially bound args
                    if k not in ba:
                        # But only if the keys exist in the arguments of the
                        # method signature, or if a varkeyword(e.g. **kwargs)
                        # appeared in the signature
                        if (k in params) or self.varkeyword:
                            if k not in kwargs:
                                kwargs[k] = v
                        else:
                            # Otherwise, we still need to add this parameter
                            # later, adter the method is called
                            add_later[k] = v
                # Remove the dict from the positional arguments
                args = args[1:]
        except IndexError:
            pass

        remove_from_kwargs = {}
        if not self.varkeyword:
            remove_from_kwargs = {k: v for k, v in kwargs.items() if k not in params.keys()}

        # Pass in the kwargs that don't appear in the original app signature
        # to be later used possibly by other apps
        add_later.update(remove_from_kwargs)
        # and remove them from the kwargs
        for k in remove_from_kwargs.keys():
            del kwargs[k]

        self.args = args
        self.kwargs = kwargs

        self.return_args = dict(kwargs)
        self.return_args.update(sig.bind_partial(*args).arguments)
        self.return_args.update(add_later)

        self._apply_indirect()

    def get_bag(self) -> {}:
        return self.return_args

    def update(self, updates: {}):
        self._update(updates)
        self._apply_indirect()

    def _update(self, updates: {}):
        self.return_args.update(updates)

        arguments = self.sig.bind_partial(*self.args).arguments
        for k, v in updates.items():
            if k in arguments:
                arguments[k] = v
            elif k in self.sig.parameters or self.varkeyword:
                self.kwargs[k] = v
        self.args = list(arguments.values())

    def split_for_signature(self) -> ([], {}):
        return self.args, self.kwargs

    @classmethod
    def _is_indirect(cls, value):
        return hasattr(value, 'startswith') and value.startswith(cls.INDIRECT_ARG_CHAR)

    @classmethod
    def _get_indirect_key(cls, value):
        return value.lstrip(cls.INDIRECT_ARG_CHAR)

    def _apply_indirect(self):
        arguments = self.sig.bind_partial(*self.args).arguments
        ind_args = {k: self._get_indirect_key(v) for k, v in arguments.items() if self._is_indirect(v)}

        defaults = [v for v in self.sig.parameters.values() if v.default]
        defaults = [v for v in defaults if v.name not in self.kwargs]
        defaults = [v for v in defaults if v.name not in arguments]
        ind_defaults = {v.name: self._get_indirect_key(v.default) for v in defaults if self._is_indirect(v.default)}

        ind_kargs = {k: self._get_indirect_key(v) for k, v in self.kwargs.items() if self._is_indirect(v)}
        bag = self.get_bag()

        all_to_update = {**ind_defaults, **ind_kargs, **ind_args}
        updates = {k: bag[i_key] for k, i_key in all_to_update.items() if i_key in bag}

        # default keys needs to be in kwargs to allow the update
        for k in ind_defaults:
            if k not in self.kwargs:
                self.kwargs[k] = ""
        self._update(updates)

    def pop(self, k, *default):
        for l in [self.return_args, self.kwargs]:
            try:
                v = l.pop(k)
            except KeyError:
                pass
        try:
            return v
        except NameError:
            try:
                return default[0]
            except IndexError:
                raise KeyError('%s not in bog.kwargs or bog.return_keys')