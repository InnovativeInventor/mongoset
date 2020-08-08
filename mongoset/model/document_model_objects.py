from __future__ import annotations

from typing import Optional, Dict, Any

from pydantic.main import BaseModel, ModelMetaclass

from mongoset.model.immutable_arguments import _add_immutables_to_namespace, Immutable


class _ObjectModelMetaclass(ModelMetaclass):
    def __new__(mcs, name, bases, namespace, **kwargs):
        namespace = _add_immutables_to_namespace(bases, namespace)
        return super().__new__(mcs, name, bases, namespace, **kwargs)


class ObjectModel(BaseModel, metaclass=_ObjectModelMetaclass):
    __immutables__: Dict[str, Immutable]

    def __init__(self, **data: Any) -> None:
        self.__config__.validate_assignment = True
        super().__init__(**data)

    def serialize(self) -> dict:
        return {k: v for k, v in super().dict().items() if v is not None}

    def force_set(self, name, value):
        super().__setattr__(name, value)

    def __setattr__(self, name, value):
        if name in self.__immutables__:
            if name in self.__dict__:
                raise ValueError(f"{self.__class__.__name__}.{name} is immutable")

        return super().__setattr__(name, value)


class DocumentModel(ObjectModel):
    id: Optional[str] = Immutable()
