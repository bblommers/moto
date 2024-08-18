from moto.extensions import AbstractExtension

from moto.moto_api._internal.extensions.manager import ExtensionManager


class FirstExtension(AbstractExtension):
    def extension_id(self) -> str:
        return "e1"


class SecondExtension(AbstractExtension):
    def extension_id(self) -> str:
        return "e2"


class ThirdExtension(AbstractExtension):
    def extension_id(self) -> str:
        return "e3"


def test_extensions_are_sorted():
    my_manager = ExtensionManager()
    first = FirstExtension()
    second = SecondExtension()
    third = ThirdExtension()

    my_manager.add_extension(index=10, extension=second)
    my_manager.add_extension(index=5, extension=first)
    my_manager.add_extension(index=20, extension=third)

    assert my_manager.extensions == [(5, first), (10, second), (20, third)]


def test_extensions_can_have_duplicate_index():
    my_manager = ExtensionManager()
    first = FirstExtension()
    second = SecondExtension()

    my_manager.add_extension(index=10, extension=first)
    my_manager.add_extension(index=10, extension=second)

    assert my_manager.extensions == [(10, first), (10, second)]
