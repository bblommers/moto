

from typing import TYPE_CHECKING, Dict
if TYPE_CHECKING:
    from moto.extensions import AbstractExtension

from .manager import ExtensionManager

extension_instances: Dict[str, "AbstractExtension"] = {}
extension_manager = ExtensionManager()
