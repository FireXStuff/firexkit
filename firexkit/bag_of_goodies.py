from typing import Type, Any
import dataclasses
from inspect import Signature, Parameter
import typing

from celery.utils.log import get_task_logger

from firexkit.result import RETURN_KEYS_KEY

logger = get_task_logger(__name__)

class BagOfGoodies:
    # Special Char to denote indirect parameter references
    INDIRECT_ARG_CHAR = '@'

    def __init__(
        self,
        sig: Signature,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        has_returns_from_previous_task=True,
    ):
        self.sig = sig
        args = self.resolve_circular_indirect_references(
            sig, tuple(args), has_returns_from_previous_task)
        mutated_kwargs = dict(kwargs)

        # Check if the method signature contains any VAR_KEYWORD (i.e., **kwargs)
        self.varkeyword = any(p.kind == p.VAR_KEYWORD for p in sig.parameters.values())

        non_input_returns : dict[str, Any] = {}
        try:
            # If the first positional argument is a
            # dict (i.e., result of a previous task), we need to process it.
            if isinstance(args[0], dict) and has_returns_from_previous_task:
                prev_task_result: dict[str, Any] = args[0]
                # Remove the RETURN_KEYS_KEY entry since results are in prev_task_result
                prev_task_result.pop(RETURN_KEYS_KEY, None)
                # get remaining argument names
                bound_arg_names = set(sig.bind_partial(*args[1:]).arguments.keys())
                for pt_result_name, pr_result_val in prev_task_result.items():
                    is_prev_task_auto_reg = pt_result_name == AutoInjectRegistry.AUTO_IN_REG_ABOG_KEY
                    # Add previous task results to cur args only if they're not already bound
                    if pt_result_name not in bound_arg_names:
                        # But only if the keys exist in the arguments of the
                        # method signature, or if a varkeyword(e.g. **kwargs)
                        # appeared in the signature
                        prev_result_accepted_by_sig = bool(
                            pt_result_name in sig.parameters
                            or ( self.varkeyword and not is_prev_task_auto_reg)
                        )
                        if prev_result_accepted_by_sig:
                            # if x='@x', and x was present in the original args, we must use it
                            indirect_to_self = (
                                pt_result_name in mutated_kwargs
                                and self._is_indirect(mutated_kwargs[pt_result_name])
                                and self._get_indirect_key(mutated_kwargs[pt_result_name]) == pt_result_name
                            )
                            if (
                                pt_result_name not in mutated_kwargs
                                or indirect_to_self
                            ):
                                mutated_kwargs[pt_result_name] = pr_result_val
                        else:
                            # Otherwise add to result
                            non_input_returns[pt_result_name] = pr_result_val
                    elif is_prev_task_auto_reg and pr_result_val:
                        non_input_returns[AutoInjectRegistry.AUTO_IN_REG_ABOG_KEY] = pr_result_val
                # Remove the dict from the positional arguments
                args = args[1:]
        except IndexError:
            pass

        bound_pos_args : dict[str, Any] = sig.bind_partial(*args).arguments

        if ( auto_in_reg := AutoInjectRegistry.get_auto_inject_registry(non_input_returns, kwargs) ):
            # make future auto-injected args use explicit values if present.
            auto_in_reg.update_auto_inject_args( mutated_kwargs | bound_pos_args )
            # see if this task needs any args auto-injected.
            auto_in_kwargs = auto_in_reg.get_auto_inject_values(
                sig.parameters,
                bound_param_names=set(bound_pos_args) | set(mutated_kwargs),
            )
            mutated_kwargs.update(auto_in_kwargs)
            mutated_kwargs.pop(AutoInjectRegistry.AUTO_IN_REG_ABOG_KEY, None)
            if AutoInjectRegistry.AUTO_IN_REG_ABOG_KEY not in non_input_returns:
                non_input_returns[AutoInjectRegistry.AUTO_IN_REG_ABOG_KEY] = auto_in_reg
            else:
                pass # already there from prev crazy result handling above :/

        # remove keys from kwargs that are bound by the positional args
        remove_from_kwargs = {}
        if not self.varkeyword:
            remove_from_kwargs = {
                k: v for k, v in mutated_kwargs.items()
                if k not in sig.parameters.keys()
            }

        # Pass in the kwargs that don't appear in the original app signature
        # to be later
        non_input_returns.update(remove_from_kwargs)

        # and remove them from the kwargs
        for k in remove_from_kwargs.keys():
            del mutated_kwargs[k]

        for k in bound_pos_args.keys():
            mutated_kwargs.pop(k, None)

        self.args = tuple(args)
        self.kwargs = mutated_kwargs

        self.return_args = self.kwargs | bound_pos_args | non_input_returns
        self._apply_indirect()

    def resolve_circular_indirect_references(self, sig: Signature, args: tuple,
                                             has_returns_from_previous_task: bool) -> tuple:
        # Handle cases when '@x' is passed positionally to the argument x
        if len(args) and isinstance(args[0], dict) and has_returns_from_previous_task:
            bound_args = sig.bind_partial(*args[1:]).arguments
            for k, v in bound_args.items():
                if self._is_indirect(v) and self._get_indirect_key(v) == k and k in args[0]:
                    bound_args[k] = args[0][k]
            return (args[0],) + tuple(bound_args.values())
        else:
            return args

    def get_bag(self) -> dict[str, Any]:
        return self.return_args

    def update(self, updates: dict[str, Any]):
        self._update(updates)
        self._apply_indirect()

    def _update(self, updates: dict[str, Any]):

        self.return_args.update(updates)

        arguments = self.sig.bind_partial(*self.args).arguments
        for k, v in updates.items():
            if k in arguments:
                arguments[k] = v
            elif k in self.sig.parameters or self.varkeyword:
                self.kwargs[k] = v

        new_args = []
        for arg, val in arguments.items():
            param = self.sig.parameters[arg]

            if param.kind != param.VAR_POSITIONAL:
                new_args.append(val)
            else:
                try:
                    new_args.extend(val)
                except TypeError as e:
                    #  Did we update() a VAR_POSITIONAL arg with a non-iterable arg? Don't do that!
                    raise ValueError(f'VAR_POSITIONAL argument {arg} should always be an iterable') from e

        self.args = new_args

    def public_kwargs(self) -> typing.Mapping[str, Any]:
        return {
            k: v for k, v in self.kwargs.items()
            if k != AutoInjectRegistry.AUTO_IN_REG_ABOG_KEY
        }

    @classmethod
    def _is_indirect(cls, value):
        return hasattr(value, 'startswith') and value.startswith(cls.INDIRECT_ARG_CHAR)

    @classmethod
    def _get_indirect_key(cls, value):
        return value.lstrip(cls.INDIRECT_ARG_CHAR)

    def _apply_indirect(self):
        arguments = self.sig.bind_partial(*self.args).arguments
        ind_args = {k: self._get_indirect_key(v) for k, v in arguments.items() if self._is_indirect(v)}

        indirect_defaults = {
            param.name: self._get_indirect_key(param.default)
            for param in self.sig.parameters.values()
            if (
                param.default is not self.sig.empty
                and param.name not in self.kwargs
                and param.name not in arguments
                and self._is_indirect(param.default)
            )
        }

        ind_kargs = {k: self._get_indirect_key(v) for k, v in self.kwargs.items() if self._is_indirect(v)}

        all_to_update = indirect_defaults | ind_kargs | ind_args
        updates = {
            k: self.return_args[i_key]
            for k, i_key in all_to_update.items()
            if i_key in self.return_args}

        # default keys needs to be in kwargs to allow the update
        for k in indirect_defaults:
            if k not in self.kwargs:
                self.kwargs[k] = ""
        self._update(updates)

    def pop(self, k, *default):
        vals = []
        for l in [self.return_args, self.kwargs]:
            if k in l:
                vals.append(l.pop(k))
        if vals:
            return vals[-1]
        if default:
            return default[0]
        raise KeyError(f'{k} not in bog.kwargs or bog.return_keys')



A = typing.TypeVar('A')
AutoInject = typing.Annotated[A, 'FireXAutoInject']

def _get_auto_inject_type(annotation) -> typing.Optional[typing.Type]:
    if (
        typing.get_origin(annotation) is typing.Annotated
        and annotation.__metadata__[0] == 'FireXAutoInject'
    ):
        return annotation.__origin__
    return None


T = typing.TypeVar('T')

@dataclasses.dataclass
class AutoInjectSpec(typing.Generic[T]):
    arg_type: typing.Type[T]
    arg_name: str
    default_value: T
    value: typing.Optional[T] = None


@dataclasses.dataclass(frozen=True)
class AutoInjectRegistry:

    # dynamic auto inject keys/values
    _specs_by_name_and_type: dict[str, dict[Type, AutoInjectSpec]]

    AUTO_IN_REG_ABOG_KEY : typing.ClassVar[str] = '__auto_inject_registry'
    EMPTY : typing.ClassVar[typing.Optional['AutoInjectRegistry']] = None

    @classmethod
    def get_auto_inject_registry(
        cls,
        primary_args: dict[str, Any],
        secondary_args: dict[str, Any],
    ) -> typing.Optional['AutoInjectRegistry']:
        return (
            primary_args.get(cls.AUTO_IN_REG_ABOG_KEY)
            or secondary_args.get(cls.AUTO_IN_REG_ABOG_KEY)
        )

    @classmethod
    def empty_auto_inject_reg(cls) -> 'AutoInjectRegistry':
        if cls.EMPTY is None:
            cls.EMPTY = AutoInjectRegistry({})
        return cls.EMPTY

    @staticmethod
    def create_auto_in_reg(specs: list[AutoInjectSpec]) -> 'AutoInjectRegistry':
        specs_by_name_and_type: dict[str, dict[Type, AutoInjectSpec]] = {}

        for s in specs:
            if s.value is not None:
                raise ValueError(
                    f'AutoInjectRegistry should only be initialised with default values, but {s.arg_name} has a real value.')
            if s.arg_name not in specs_by_name_and_type:
                specs_by_name_and_type[s.arg_name] = {}
            if s.arg_type not in specs_by_name_and_type[s.arg_name]:
                specs_by_name_and_type[s.arg_name][s.arg_type] = s
            else:
                raise ValueError(f'Duplicate specs for {s.arg_name}[{s.arg_type}]')

        return AutoInjectRegistry(specs_by_name_and_type)

    def get_auto_injectable_arg_names(self) -> set[str]:
        return set(self._specs_by_name_and_type)

    def get(self, name: str) -> Any:
        if name in self._specs_by_name_and_type:
            specs = list(self._specs_by_name_and_type[name].values())
            assert len(specs) == 1, f'Expected exactly one auto-inject value for {name}, found {len(specs)}'
            return specs[0].value or specs[0].default_value
        return None

    def update_auto_inject_args(self, pos_and_kw_args: dict[str, Any]):
        """
            Update the value in the auto-inject registry so that the nearest ancestor's
            value of an auto-injected arg is used instead of the default or a farther ancestor's
            value.
        """
        for arg_name, arg_val in pos_and_kw_args.items():
            if (
                arg_name in self._specs_by_name_and_type
                and (auto_in_arg := self._get_spec_by_name_and_instance(arg_name, arg_val) )
            ):
                if auto_in_arg.value != arg_val:
                    logger.info(f'Overwriting auto-inject arg {arg_name} with abog value: {arg_val}')
                    auto_in_arg.value = arg_val

    def _get_spec_by_name_and_instance(self, arg_name: str, val: typing.Any) -> typing.Optional[AutoInjectSpec]:
        for t, spec in self._specs_by_name_and_type[arg_name].items():
            if isinstance(val, t):
                return spec
        return None

    def _get_spec_by_name_and_type(self, arg_name: str, _type: Type) -> typing.Optional[AutoInjectSpec]:
        if arg_name not in self._specs_by_name_and_type:
            logger.error(f'AutoInjectRegistry not statically initialized for {arg_name}')
        elif _type not in self._specs_by_name_and_type[arg_name]:
            logger.error(f'AutoInjectRegistry not statically initialized for {arg_name}/[{_type}]')
        else:
            return self._specs_by_name_and_type[arg_name][_type]
        return None

    def get_auto_inject_values(
        self,
        parameters: typing.Mapping[str, Parameter],
        # TODO: could validate bound AutoInject values adhere to AutoInject's type.
        bound_param_names: set[str],
    ) -> dict[str, typing.Any]:
        auto_inject_kwargs = {}
        possible_auto_injectable_params = {
            k: p
            for k, p in parameters.items()
            if (
                # does the receiving task declare any unbound AutoInject
                # args that should be auto-injected?
                _get_auto_inject_type(p.annotation)
                and k not in bound_param_names
            )
        }
        for auto_inject_name, param in possible_auto_injectable_params.items():
            if param.default != param.empty:
                # for now don't support service-level defaults since all use-cases require run-level defaults
                # and none additionally require service-level defaults. The point of AutoInject is that
                # service definitions can be written assuming AutoInject is populated with a valide type,
                # to adding a default at the service level confuses that and encourages "always have a default"
                # needless defensive coding.
                raise Exception(f'AutoInject arg {auto_inject_name} has a default value.')

            auto_inject_type = _get_auto_inject_type(param.annotation)
            if auto_inject_type:
                spec = self._get_spec_by_name_and_type(auto_inject_name, auto_inject_type)
                if spec:
                    if spec.value is not None:
                        logger.debug(f'Setting non-default AutoInject {auto_inject_name}')
                        auto_in_v = spec.value
                    else:
                        logger.debug(f'Setting default AutoInject {auto_inject_name}')
                        auto_in_v = spec.default_value
                    auto_inject_kwargs[auto_inject_name] = auto_in_v
            else:
                raise Exception(
                    f'AutoInject arg {auto_inject_name} has no inner type. The "Foo" in AutoInject[Foo] is required.')
        if auto_inject_kwargs:
            logger.debug(f'Auto-Injecting args: {", ".join(auto_inject_kwargs)}')
        return auto_inject_kwargs

    def get_auto_inject_values_and_reg(
        self,
        parameters: typing.Mapping[str, Parameter],
        bound_param_names: set[str],
    ) -> dict[str, typing.Any]:
        auto_inject_kwargs = self.get_auto_inject_values(parameters, bound_param_names)
        return auto_inject_kwargs | {
            self.AUTO_IN_REG_ABOG_KEY: self, # propagate the registry
        }