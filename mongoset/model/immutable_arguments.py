class _Unset:
    pass


class Immutable:
    """
    Set a field of an ObjectModel to this class to make it immutable.

    Examples:
        class ExampleObject(ObjectModel):
            x: int = Immutable(5)
            y: str = Immutable()
    """

    def __init__(self, default_val=_Unset):
        self.default_val = default_val


def _add_immutables_to_namespace(bases: list, namespace: dict) -> dict:
    immutables = {}

    # Add all immutables in this class's bases to this one
    for base in bases:
        if "__immutables__" in base.__dict__:
            immutables.update(base.__immutables__)

    # Check each annotation to see if it's immutable
    if "__annotations__" in namespace:
        for var_name, var_type in namespace["__annotations__"].items():
            if var_name in namespace:
                # The variable actually has a value (only way it could be immutable)
                var_value = namespace[var_name]
                if isinstance(var_value, Immutable):
                    # The variable's value is the Immutable class
                    if var_value.default_val == _Unset:
                        # If there is no default value, delete the variable from the namespace
                        del namespace[var_name]
                    else:
                        # Otherwise set the value in the namespace with the correct value so pydantic gets it
                        namespace[var_name] = var_value.default_val

                    # Add this new immutable to the dict
                    immutables.update({var_name: var_value})

    namespace.update({"__immutables__": immutables})

    return namespace
